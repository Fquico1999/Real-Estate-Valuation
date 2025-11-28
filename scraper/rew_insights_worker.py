# scraper/rew_insights_worker.py
import asyncio
import random
import logging
import pathlib
from datetime import datetime, date

from logging_config import setup_logging

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    UndetectedAdapter,
)
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import (
    AsyncSessionLocal,
    init_db,
    PropertyCharacteristics,
    Assessment,
    Sale,
    RawScrape,
)
from geocoding import BBox
from property_utils import get_or_create_property
from parsers import parse_rew_insights
from rew_insights_url_queue import (
    dequeue_next_batch,
    mark_done,
    mark_failed,
    enqueue_insights_urls,
)
from url_queue import enqueue_urls  # existing REW listing URL queue

logger = logging.getLogger(f"scraper.{pathlib.Path(__file__).stem}")
setup_logging()

EMPTY_QUEUE_SLEEP_SECONDS = 60
PER_URL_SLEEP_SECONDS = 1
PAGE_LOAD_TIMEOUT_SECONDS = 30  # seconds (we pass ms to crawl4ai)
BATCH_SIZE = 5
RESTART_BROWSER_EVERY_N_BATCHES = 5

# Timeout / Backoff Tuning for 'flaky' timeouts
MAX_RETRIES_ON_TIMEOUT = 3
TIMEOUT_BACKOFF_SECONDS = [5 * 60, 15 * 60, 45 * 60]


def _json_safe(value):
    """
    Recursively convert any date/datetime objects to ISO strings so they can
    be stored in a JSON/JSONB column.
    """
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


async def upsert_property_characteristics_rew_insights(session, prop_id: int, basic: dict):
    """
    Insert a PropertyCharacteristics snapshot from REW Insights.
    """
    as_of_date = basic.get("as_of_date") or datetime.utcnow().date()

    row = {
        "property_id": prop_id,
        "as_of_date": as_of_date,
        "source": "rew_insights",
        "beds": basic.get("beds"),
        "baths": basic.get("baths"),
        "sqft_finished": basic.get("sqft"),
        "sqft_unfinished": None,
        "lot_sqft": None,
        "year_built": basic.get("year_built"),
        "raw_blob": _json_safe(basic),
        "scraped_at": datetime.utcnow(),
    }

    stmt = pg_insert(PropertyCharacteristics).values(row)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "as_of_date", "source"]
    )
    await session.execute(stmt)


async def upsert_assessments_rew_insights(session, prop_id: int, assessments: list[dict]):
    if not assessments:
        return

    rows = []
    for a in assessments:
        year = a.get("assessment_year")
        total = a.get("total_assessed_cad")
        if not year or total is None:
            continue

        rows.append(
            {
                "property_id": prop_id,
                "assessment_year": year,
                "total_assessed_cad": total,
                "land_value": a.get("land_value"),
                "building_value": a.get("building_value"),
                "source": "rew_insights",
                "raw_blob": _json_safe(a),
            }
        )

    if not rows:
        return

    stmt = pg_insert(Assessment).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "assessment_year", "source"]
    )
    await session.execute(stmt)


def _parse_rew_insights_sale_date(raw: str | None) -> date | None:
    if not raw:
        return None

    raw = raw.strip()
    # Try a few likely formats
    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    # As a last resort, just ignore if we can't parse confidently
    logger.debug(f"Could not parse REW Insights sale date: {raw!r}")
    return None


async def upsert_sales_rew_insights(session, prop_id: int, sales: list[dict]):
    if not sales:
        return

    rows = []
    for s in sales:
        price = s.get("sale_price_cad")
        if price is None:
            continue
        d = _parse_rew_insights_sale_date(s.get("sale_date"))
        if not d:
            continue

        rows.append(
            {
                "property_id": prop_id,
                "sale_date": d,
                "sale_price_cad": price,
                "list_price_cad": None,
                "mls_number": None,
                "source": "rew_insights",
                "beds": None,
                "baths": None,
                "sqft": None,
                "lot_sqft": None,
                "raw_blob": _json_safe(s.get("raw", {})),
            }
        )

    if not rows:
        return

    stmt = pg_insert(Sale).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "sale_date", "sale_price_cad", "source"]
    )
    await session.execute(stmt)


async def scrape_rew_insights_page(crawler: AsyncWebCrawler, session, url: str) -> None:
    """
    Visit a REW Insights page, parse:
      * canonical address -> Property
      * characteristics -> PropertyCharacteristics(source='rew_insights')
      * assessments -> Assessment(source='rew_insights')
      * sales -> Sale(source='rew_insights')
      * neighbour URLs -> enqueue into RewInsightsUrl + RewListingUrl
    """
    logger.info(f"Scraping REW Insights: {url}")

    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_for="css:.insightsheader-details",
        wait_until="domcontentloaded",
        page_timeout=PAGE_LOAD_TIMEOUT_SECONDS * 1000,
        delay_before_return_html=1.5,
        magic=True,
    )

    result = await crawler.arun(url=url, config=run_conf)

    if not getattr(result, "success", True):
        raise RuntimeError(f"Crawl failed for {url}: {getattr(result, 'error_message', 'unknown')}")

    html = getattr(result, "html", None) or getattr(result, "content", None)
    if not html:
        logger.warning(f"No HTML returned for {url}")
        return

    # Store raw HTML snapshot
    rs = RawScrape(
        source="rew_insights",
        url=url,
        http_status=200,
        payload_type="html",
        payload=html,
    )
    session.add(rs)

    parsed = parse_rew_insights(html, url)
    basic = parsed.get("basic") or {}

    street = basic.get("street_address")
    city = basic.get("city")
    province = basic.get("province") or "BC"
    postal = basic.get("postal_code")

    if not street or not city:
        logger.warning(f"Could not parse main address from Insights page {url}")
        await session.commit()  # at least keep RawScrape
        return

    bbox_dict = basic.get("bbox")
    bbox_obj = BBox(
        south=bbox_dict["south"],
        north=bbox_dict["north"],
        west=bbox_dict["west"],
        east=bbox_dict["east"],
    ) if bbox_dict else None

    # Map Insights address -> Property
    prop = await get_or_create_property(
        session=session,
        street=street,
        city=city,
        province=province,
        postal_code=postal,
        lat=basic.get("lat"),
        lng=basic.get("lng"),
        bbox=bbox_obj,
    )
    prop_id = prop.id

    # Characteristics snapshot
    await upsert_property_characteristics_rew_insights(session, prop_id, basic)

    # Assessments
    assessments = parsed.get("assessments") or []
    await upsert_assessments_rew_insights(session, prop_id, assessments)

    # Sales history
    sales = parsed.get("sales") or []
    await upsert_sales_rew_insights(session, prop_id, sales)

    # Neighbourhood links
    links = parsed.get("neighbourhood_links") or {}
    insights_urls = links.get("insights_urls") or []
    listing_urls = links.get("listing_urls") or []

    if insights_urls:
        inserted_insights = await enqueue_insights_urls(insights_urls, session)
        logger.info(f"  -> enqueued {inserted_insights} neighbouring REW Insights URLs")

    if listing_urls:
        inserted_listings = await enqueue_urls(listing_urls, session)
        logger.info(f"  -> enqueued {inserted_listings} neighbouring REW listing URLs")

    await session.commit()
    logger.info(
        f"Finished REW Insights scrape for property_id={prop_id} "
        f"({street}, {city}, {province} {postal or ''})"
    )


async def main():
    await init_db()
    logger.info("DB init complete for REW Insights worker")

    # Configure browser
    browser_conf = BrowserConfig(
        headless=True,
        enable_stealth=True,
        verbose=False,
        extra_args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
        adapter=UndetectedAdapter(),
        crawler_strategy=AsyncPlaywrightCrawlerStrategy,
    )

    batch_counter = 0

    while True:
        try:
            logger.info("Creating AsyncWebCrawler for REW Insights...")
            async with AsyncWebCrawler(config=browser_conf) as crawler:
                logger.info("Crawler ready, entering main loop")
                consecutive_timeout_batches = 0

                while True:
                    batch_counter += 1

                    async with AsyncSessionLocal() as session:
                        logger.info(f"Starting new Insights batch (#{batch_counter})...")
                        batch = await dequeue_next_batch(session, batch_size=BATCH_SIZE)

                        if not batch:
                            logger.info(
                                f"No pending REW Insights URLs. "
                                f"Sleeping for {EMPTY_QUEUE_SLEEP_SECONDS} seconds..."
                            )
                            await asyncio.sleep(EMPTY_QUEUE_SLEEP_SECONDS)
                            continue

                        for url_id, url in batch:
                            try:
                                await scrape_rew_insights_page(crawler, session, url)
                                await mark_done(session, url_id)
                            except Exception as e:
                                await mark_failed(session, url_id, str(e))
                                logger.exception(f"Failed Insights scrape: {url} ({e})")

                            sleep_for = PER_URL_SLEEP_SECONDS + random.uniform(0, 1)
                            logger.info(f"Sleeping {sleep_for:.2f}s before next Insights URL...")
                            await asyncio.sleep(sleep_for)

                    # Periodically recycle the browser to avoid Playwright weirdness
                    if batch_counter % RESTART_BROWSER_EVERY_N_BATCHES == 0:
                        logger.info("Recycling browser instance for REW Insights...")
                        break

        except Exception as e:
            logger.critical(
                f"REW Insights crawler crashed completely: {e}. "
                "Restarting loop in 10s..."
            )
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
