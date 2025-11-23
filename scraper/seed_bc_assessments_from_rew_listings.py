# scraper/seed_bc_assessments_from_rew_listings.py
import asyncio
import logging
import pathlib
import random

from datetime import datetime

from logging_config import setup_logging

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from sqlalchemy import select, exists, and_

from models import (
    AsyncSessionLocal,
    init_db,
    RewListing,
    Assessment,
    PropertyCharacteristics,
)
from property_utils import get_or_create_property
from parsers import (
    _extract_bc_main_address,
    parse_bc_assessment_property_characteristics,
    parse_bc_assessment_assessments,
    parse_bc_assessment_neighbor_urls,
)
from bc_assessment_url_queue import enqueue_property_urls
from bc_assessment_worker import upsert_property_characteristics, upsert_assessments
from sqlalchemy.dialects.postgresql import insert as pg_insert


logger = logging.getLogger(f"scraper.{pathlib.Path(__file__).stem}")
setup_logging()

BCA_BASE_URL = "https://www.bcassessment.ca"

# tune these as needed
BATCH_SIZE = 5
EMPTY_SLEEP_SECONDS = 60
PER_LISTING_SLEEP_SECONDS = 1
PAGE_TIMEOUT_SECONDS = 45


SEARCH_JS_TEMPLATE = r"""
(async () => {
  const wait = (ms) => new Promise(r => setTimeout(r, ms));

  const agreeBtn = document.getElementById('btnAgree');
  if (agreeBtn) { agreeBtn.click(); await wait(1000); }

  const input = document.querySelector('#rsbSearch');
  if (!input) return;
  input.focus();
  input.value = '';

  const address = "%(address)s";
  for (let char of address) {
      input.value += char;
      input.dispatchEvent(new Event('input', { bubbles: true }));
      await wait(30);
  }
  input.dispatchEvent(new Event('change', { bubbles: true }));

  let attempts = 0;
  while (!document.querySelector('ul.ui-autocomplete') && attempts < 50) {
      await wait(100);
      attempts++;
  }
  await wait(500);

  const activeLink = document.querySelector('li.ui-menu-item.ui-state-active a');
  if (activeLink) {
      activeLink.click();
  } else {
      input.dispatchEvent(new KeyboardEvent('keydown', {
          bubbles: true, cancelable: true, key: 'Enter', code: 'Enter', keyCode: 13
      }));
  }
})();
"""

WAIT_FOR_INFO_OR_ERROR = """js:() => {
  const hasInfo = document.querySelector('#lblTotalAssessedValue');
  const error = document.body.innerText.includes('No results found');
  return hasInfo || error;
}"""


def build_search_string(listing: RewListing) -> str:
    """
    Construct the search string used on bcassessment.ca
    from a REW listing. Simple & overridable.
    """
    parts = []
    if listing.street_address:
        parts.append(listing.street_address.strip())
    if listing.city:
        parts.append(listing.city.strip())
    # BC Assessment usually doesn't need province in the query,
    # but you can add it if you like:
    # if listing.province:
    #     parts.append(listing.province.strip())
    return " ".join(parts)


async def fetch_next_rew_batch(session, offset: int, batch_size: int = BATCH_SIZE):
    """
    Fetch a batch of REW listings to seed BC assessments for.

    Strategy:
    - province == 'BC'
    - have a street_address & city
    - property_id is known
    - no existing Assessment from 'bc_assessment' (to avoid rework)
    """
    assess_exists = exists().where(
        and_(
            Assessment.property_id == RewListing.property_id,
            Assessment.source == "bc_assessment",
        )
    )

    stmt = (
        select(RewListing)
        .where(
            RewListing.province == "BC",
            RewListing.street_address.isnot(None),
            RewListing.city.isnot(None),
            RewListing.property_id.isnot(None),
            ~assess_exists,
        )
        .order_by(RewListing.id)
        .offset(offset)
        .limit(batch_size)
    )

    result = await session.execute(stmt)
    return result.scalars().all()


async def process_single_listing(
    crawler: AsyncWebCrawler, session, listing: RewListing
):
    """
    For a given REW listing:
      - Search on bcassessment.ca by address
      - If we land on a property page, parse:
          * canonical address -> Property
          * characteristics -> PropertyCharacteristics(source='bc_assessment')
          * assessments -> Assessment(source='bc_assessment')
          * neighbour URLs -> enqueue into BCAssessmentUrl
    """
    search_str = build_search_string(listing)
    if not search_str:
        logger.info(f"Skipping listing {listing.id}: no usable address")
        return

    logger.info(
        f"[Listing {listing.id}] Search BC Assessment for: {search_str!r}"
    )

    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        js_code=SEARCH_JS_TEMPLATE % {"address": search_str.replace('"', '\\"')},
        wait_for=WAIT_FOR_INFO_OR_ERROR,
        page_timeout=PAGE_TIMEOUT_SECONDS * 1000,
        delay_before_return_html=1.5,
    )

    result = await crawler.arun(url=BCA_BASE_URL, config=run_conf)

    if not result.success:
        logger.warning(
            f"[Listing {listing.id}] BC Assessment search failed: {result.error_message}"
        )
        return

    html = getattr(result, "html", None) or getattr(result, "content", None) or ""
    if not html:
        logger.info(f"[Listing {listing.id}] Empty HTML from BC Assessment")
        return

    # Check for "No results found" guard (same phrase used in wait_for)
    if "No results found" in html:
        logger.info(
            f"[Listing {listing.id}] No BC Assessment match for address {search_str!r}"
        )
        return

    # Map BC page heading -> Property
    street, city, postal = _extract_bc_main_address(html)
    if not street or not city:
        logger.warning(
            f"[Listing {listing.id}] Could not parse BC main address for search {search_str!r}"
        )
        return

    province = "BC"
    prop = await get_or_create_property(
        session=session,
        street=street,
        city=city,
        province=province,
        postal_code=postal,
        lat=None,
        lng=None,
    )
    property_id = prop.id

    # Characteristics
    char_data = parse_bc_assessment_property_characteristics(html)
    await upsert_property_characteristics(session, property_id, char_data)

    # Assessments
    assessments = parse_bc_assessment_assessments(html)
    await upsert_assessments(session, property_id, assessments)

    # Neighbours -> PropertyCrawl queue
    neighbour_urls = parse_bc_assessment_neighbor_urls(html)
    if neighbour_urls:
        inserted = await enqueue_property_urls(neighbour_urls, session)
        logger.info(
            f"[Listing {listing.id}] Enqueued {inserted} neighbouring BC properties"
        )

    await session.commit()
    logger.info(
        f"[Listing {listing.id}] Seeded BC Assessment data for property_id={property_id} "
        f"({street}, {city}, {province} {postal or ''})"
    )


async def main():
    await init_db()
    logger.info("DB init complete for BC seeding")

    browser_conf = BrowserConfig(
        headless=True,
        enable_stealth=True,
        verbose=False,
        extra_args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    )

    async with AsyncWebCrawler(config=browser_conf) as crawler:
        offset = 0

        while True:
            async with AsyncSessionLocal() as session:
                logger.info(
                    f"Fetching REW listings batch: offset={offset}, size={BATCH_SIZE}"
                )
                listings = await fetch_next_rew_batch(
                    session, offset=offset, batch_size=BATCH_SIZE
                )

                if not listings:
                    logger.info(
                        f"No more REW listings needing BC Assessment. "
                        f"Sleeping {EMPTY_SLEEP_SECONDS} seconds..."
                    )
                    await asyncio.sleep(EMPTY_SLEEP_SECONDS)
                    # after sleeping, you can choose to break instead of looping:
                    # break
                    continue

                for listing in listings:
                    try:
                        await process_single_listing(crawler, session, listing)
                    except Exception as e:
                        logger.exception(
                            f"Error processing listing {listing.id}: {e}"
                        )

                    sleep_for = PER_LISTING_SLEEP_SECONDS + random.uniform(0, 1)
                    logger.info(
                        f"Sleeping {sleep_for:.2f}s before next listing..."
                    )
                    await asyncio.sleep(sleep_for)

                offset += len(listings)


if __name__ == "__main__":
    asyncio.run(main())
