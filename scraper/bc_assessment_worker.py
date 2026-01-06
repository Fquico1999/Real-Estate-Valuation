# scraper/bc_assessment_worker.py
import asyncio
import random
import logging
import pathlib
from datetime import datetime, date

from logging_config import setup_logging
from bs4 import BeautifulSoup

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, UndetectedAdapter
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import (
    AsyncSessionLocal,
    init_db,
    PropertyCharacteristics,
    Assessment,
    RawScrape,
)
from property_utils import get_or_create_property, format_full_address
from geocoding import geocode
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
PAGE_LOAD_TIMEOUT_SECONDS = 30  # seconds (we pass ms to crawl4ai)
BATCH_SIZE = 3
RESTART_BROWSER_EVERY_N_BATCHES = 5

# Timeout / Backoff Tuning
MAX_RETRIES_ON_TIMEOUT = 3
TIMEOUT_BACKOFF_SECONDS = [
    5 * 60,
    15 * 60,
    45 * 60
]


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
        "raw_blob": _json_safe(char_data),
        "scraped_at": datetime.utcnow(),
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
                "raw_blob": _json_safe(a),
            }
        )

    stmt = pg_insert(Assessment).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "assessment_year", "source"]
    )
    await session.execute(stmt)


async def scrape_bc_property(crawler: AsyncWebCrawler, session, url: str) -> None:
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
        wait_until="domcontentloaded",
        page_timeout=PAGE_LOAD_TIMEOUT_SECONDS * 1000,
        delay_before_return_html=1.5,
        magic=True,
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

    # Geocode address
    # full_addr = format_full_address(street, city, province, postal)
    # geo = await geocode(full_addr)

    prop = await get_or_create_property(
        session=session,
        street=street,
        city=city,
        province=province,
        postal_code=postal,
        # lat=geo.lat,
        # lng=geo.lng,
        # bbox = geo.bbox.to_dict() if geo.bbox else None,
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

async def process_single_url(crawler, row_id, url):
    # Create fresh session for this task - thread safe
    async with AsyncSessionLocal() as session:
        try:
            # Small random stagger to avoid hitting server simultaneously
            await asyncio.sleep(random.uniform(0.1, 1.0))
            await scrape_bc_property(crawler, session, url)
            await mark_done(session, row_id)
            # Shoul happen already, but final commit here
            await session.commit()
            logger.info(f"Done: {url}")
            return {"url": url, "timed_out": False}
        except Exception as e:
            await session.rollback() # Clean up if something exploded
            await mark_failed(session, row_id, str(e))
            await session.commit() # Commit the failure state
            
            msg = str(e).lower()
            # Heuristic timeout detection: library may wrap TimeoutError
            is_timeout = isinstance(e, asyncio.TimeoutError) or "timeout" in msg or "timed out" in msg

            if is_timeout:
                logger.warning(f"Timeout scraping {url}: {e}")
            else:
                logger.error(f"Failed: {url} | Error: {e}")

            return {"url": url, "timed_out": is_timeout}


async def main():
    await init_db()
    logger.info("BC Assessment worker DB init complete")

    browser_conf = BrowserConfig(
        headless=True,
        enable_stealth=True,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        verbose=False,
        extra_args=[
            "--disable-features=IsolateOrigins,site-per-process"
        ],
    )
    # We define the strategy outside, but we will re-init the crawler inside
    adapter = UndetectedAdapter()
    strategy = AsyncPlaywrightCrawlerStrategy(
        browser_config=browser_conf,
        browser_adapter=adapter
    )

    consecutive_timeout_batches = 0 

    while True:
        # Re-enter context manager every N batches to flush memory/cookies
        try:
            async with AsyncWebCrawler(crawler_strategy=strategy, config=browser_conf) as crawler:
                logger.info("Browser session started/restarted.")
                
                # Inner loop for N batches
                for _ in range(RESTART_BROWSER_EVERY_N_BATCHES):
                    batch = []
                    async with AsyncSessionLocal() as queue_session:
                        batch = await dequeue_next_batch(queue_session, batch_size=BATCH_SIZE)
                        # Ensure the "dequeue" status update is saved before we move on
                        await queue_session.commit() 
                    
                    if not batch:
                        logger.info(f"Queue empty. Sleeping {EMPTY_QUEUE_SLEEP_SECONDS}s...")
                        await asyncio.sleep(EMPTY_QUEUE_SLEEP_SECONDS)
                        consecutive_timeout_batches = 0
                        continue 

                    logger.info(f"Processing batch of {len(batch)} URLs...")
                    tasks = []
                    for row_id, url in batch:
                        tasks.append(process_single_url(crawler, row_id, url))
                    
                    # Run at the same time
                    results = await asyncio.gather(*tasks)

                    batch_had_timeout = any(
                        r and isinstance(r, dict) and r.get("timed_out")
                        for r in results
                    )

                    if batch_had_timeout:
                        consecutive_timeout_batches += 1
                        logger.warning(
                            f"Batch had timeouts. consecutive_timeout_batches="
                            f"{consecutive_timeout_batches}"
                        )
                    else:
                        # Any fully-successful batch resets the counter
                        if consecutive_timeout_batches:
                            logger.info(
                                "Timeouts seem to have cleared; "
                                "resetting consecutive_timeout_batches to 0."
                            )
                        consecutive_timeout_batches = 0

                    # Exponential-style backoff after repeated timeouts
                    if consecutive_timeout_batches >= MAX_RETRIES_ON_TIMEOUT:
                        # How far into the backoff schedule we are
                        idx = min(
                            consecutive_timeout_batches - MAX_RETRIES_ON_TIMEOUT,
                            len(TIMEOUT_BACKOFF_SECONDS) - 1,
                        )
                        backoff_seconds = TIMEOUT_BACKOFF_SECONDS[idx]

                        logger.warning(
                            "Hit %s consecutive timeout-heavy batches; "
                            "backing off for %.0f seconds (%.1f minutes).",
                            consecutive_timeout_batches,
                            backoff_seconds,
                            backoff_seconds / 60.0,
                        )
                        await asyncio.sleep(backoff_seconds)

                logger.info("Recycling browser instance...")
        except Exception as e:
            logger.critical(f"Crawler crashed completely: {e}. Restarting loop in 10s...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
