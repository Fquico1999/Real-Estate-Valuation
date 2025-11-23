# scraper/bc_assessment_worker.py
import asyncio
import random
import logging
import pathlib
from datetime import datetime

from logging_config import setup_logging
from bs4 import BeautifulSoup

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import (
    AsyncSessionLocal,
    init_db,
    PropertyCharacteristics,
    Assessment,
    RawScrape,
)
from property_utils import get_or_create_property
from parsers import (
    _extract_bc_main_address,
    parse_bc_assessment_property_characteristics,
    parse_bc_assessment_assessments,
    parse_bc_assessment_neighbor_urls,
)
from bc_assessment_url_queue import (
    dequeue_next_batch,
    mark_done,
    mark_failed,
    enqueue_property_urls,
)

logger = logging.getLogger(f"scraper.{pathlib.Path(__file__).stem}")
setup_logging()

EMPTY_QUEUE_SLEEP_SECONDS = 60
PER_URL_SLEEP_SECONDS = 1
PAGE_LOAD_TIMEOUT_SECONDS = 60  # seconds (we pass ms to crawl4ai)


async def upsert_property_characteristics(session, prop_id: int, char_data: dict):
    """
    Insert a PropertyCharacteristics snapshot from BC Assessment.
    """
    as_of_date = char_data.get("as_of_date") or datetime.utcnow().date()

    row = {
        "property_id": prop_id,
        "as_of_date": as_of_date,
        "source": "bc_assessment",
        "beds": char_data.get("beds"),
        "baths": char_data.get("baths"),
        "sqft_finished": char_data.get("sqft_finished"),
        "sqft_unfinished": char_data.get("sqft_unfinished"),
        "lot_sqft": char_data.get("lot_sqft"),
        "year_built": char_data.get("year_built"),
        "raw_blob": char_data,
    }

    stmt = pg_insert(PropertyCharacteristics).values(row)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "as_of_date", "source"]
    )
    await session.execute(stmt)


async def upsert_assessments(session, prop_id: int, assessments: list[dict]):
    if not assessments:
        return

    rows = []
    for a in assessments:
        rows.append(
            {
                "property_id": prop_id,
                "assessment_year": a["assessment_year"],
                "total_assessed_cad": a["total_assessed_cad"],
                "land_value": a.get("land_value"),
                "building_value": a.get("building_value"),
                "source": "bc_assessment",
                "raw_blob": a,
            }
        )

    stmt = pg_insert(Assessment).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "assessment_year", "source"]
    )
    await session.execute(stmt)


async def scrape_bc_property(
    crawler: AsyncWebCrawler, session, url: str
) -> None:
    """
    Visit a BC Assessment Property/Info page, parse address, characteristics,
    assessments, and neighbour URLs.
    """
    logger.info(f"Scraping BC Assessment: {url}")

    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        # click cookie/terms banner if present; we are already on Property/Info/<id>/
        js_code="""
            (async () => {
                const wait = (ms) => new Promise(r => setTimeout(r, ms));
                const btn = document.getElementById('btnAgree');
                if (btn) { btn.click(); await wait(500); }
            })();
        """,
        wait_for="css:#lblTotalAssessedValue",  # wait until main assessed value is present
        page_timeout=PAGE_LOAD_TIMEOUT_SECONDS * 1000,
        delay_before_return_html=1.5,
    )

    result = await crawler.arun(url=url, config=run_conf)

    if not result.success:
        raise RuntimeError(f"Crawl failed for {url}: {result.error_message}")

    html = getattr(result, "html", None) or getattr(result, "content", None)
    if not html:
        logger.warning(f"No HTML returned for {url}")
        return

    # Store raw HTML snapshot
    rs = RawScrape(
        source="bc_assessment",
        url=url,
        http_status=200,
        payload_type="html",
        payload=html,
    )
    session.add(rs)

    # Map page's address to our Property table
    street, city, postal = _extract_bc_main_address(html)
    if not street or not city:
        logger.warning(f"Could not parse main address from {url}")
        # still commit raw scrape, but bail on structured data
        await session.commit()
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
    prop_id = prop.id

    # Structural characteristics
    char_data = parse_bc_assessment_property_characteristics(html)
    await upsert_property_characteristics(session, prop_id, char_data)

    # Assessment history (current + previous year)
    assessments = parse_bc_assessment_assessments(html)
    await upsert_assessments(session, prop_id, assessments)

    # Neighbouring properties: enqueue their URLs
    neighbour_urls = parse_bc_assessment_neighbor_urls(html)
    if neighbour_urls:
        logger.info(f"Found {len(neighbour_urls)} neighbouring property URLs")
        await enqueue_property_urls(neighbour_urls, session)

    await session.commit()
    logger.info(
        f"Upserted BC Assessment data for property_id={prop_id} at {street}, {city}"
    )


async def main():
    await init_db()
    logger.info("BC Assessment worker DB init complete")

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
        logger.info("BC Assessment crawler ready, entering main loop")

        while True:
            async with AsyncSessionLocal() as session:
                logger.info("Fetching next batch from property_crawl queue...")
                batch = await dequeue_next_batch(session, batch_size=3)

                if not batch:
                    logger.info(
                        f"No pending BC Assessment URLs. Sleeping {EMPTY_QUEUE_SLEEP_SECONDS} seconds..."
                    )
                    await asyncio.sleep(EMPTY_QUEUE_SLEEP_SECONDS)
                    continue

                for row_id, url in batch:
                    try:
                        await scrape_bc_property(crawler, session, url)
                        await mark_done(session, row_id)
                    except Exception as e:
                        await mark_failed(session, row_id, str(e))
                        logger.exception(f"Failed BC Assessment scrape: {url} ({e})")

                    sleep_for = PER_URL_SLEEP_SECONDS + random.uniform(0, 1)
                    logger.info(f"Sleeping {sleep_for:.2f}s before next BC Assessment URL...")
                    await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(main())
