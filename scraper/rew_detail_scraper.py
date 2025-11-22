# scraper/rew_detail_scraper.py
import asyncio
import os
import random
import json
from datetime import datetime
import pathlib, logging
from logging_config import setup_logging

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from property_utils import get_or_create_property
from models import PropertyCharacteristics, RewListing, AsyncSessionLocal, init_db, Assessment, Sale
from parsers import parse_rew_listing, parse_rew_assessment_history, parse_rew_sales_history
from url_queue import dequeue_next_batch, mark_done, mark_failed


EMPTY_QUEUE_SLEEP_SECONDS = 60  # how long to sleep when no URLs are pending
PER_URL_SLEEP_SECONDS = 1 # How long to pause for after processing a URL
PAGE_LOAD_TIMEOUT_SECONDS = 60  # Safety timeout per page

logger = logging.getLogger(f"scraper.{pathlib.Path(__file__).stem}")
setup_logging()

async def upsert_listing(session, data: dict):
    rew_url = data.get("rew_url")
    if not rew_url:
        return

    result = await session.execute(
        select(RewListing).where(RewListing.rew_url == rew_url)
    )
    existing = result.scalar_one_or_none()

    if existing:
        for k, v in data.items():
            setattr(existing, k, v)
    else:
        session.add(RewListing(**data))

    await session.commit()

def validate_listing_data(data: dict) -> None:
    """
    Raises ValueError if the scraped listing doesn't meet the minimum requirements.
    """
    missing = []

    rew_url = (data.get("rew_url") or "").strip()
    if not rew_url:
        missing.append("rew_url")

    street_address = (data.get("street_address") or "").strip()
    if not street_address:
        missing.append("street_address")

    city = (data.get("city") or "").strip()
    if not city:
        missing.append("city")

    price = data.get("price_cad")
    try:
        # allow numeric-like strings, but enforce > 0
        if price is None:
            raise ValueError
        price_val = int(price)
        if price_val <= 0:
            raise ValueError
        data["price_cad"] = price_val  # normalize
    except Exception:
        missing.append("price_cad")

    if missing:
        raise ValueError(f"Missing or invalid required fields: {', '.join(missing)}")

async def scrape_listing_detail(crawler: AsyncWebCrawler, session, url: str):
    run_conf = CrawlerRunConfig( 
        cache_mode=CacheMode.BYPASS, 
        capture_network_requests=True, 
        wait_until="domcontentloaded",
        page_timeout=PAGE_LOAD_TIMEOUT_SECONDS * 1000
        )

    logger.info(f"Scraping {url}")
    result = await crawler.arun(url=url, config=run_conf)

    if not result.success:
        logger.error(f"Crawl failed for {url}: {result.error_message}")
        raise RuntimeError(f"Crawl failed: {result.error_message}")

    html = getattr(result, "html", None) or getattr(result, "content", None)
    if not html:
        logger.info(f"No HTML for {url}")
        return
    
    # Check network payload
    gql_data: dict = {}

    for evt in (result.network_requests or []):
        if evt.get("event_type") != "response":
            continue
        if "/graphql/rew-portal" not in evt.get("url", ""):
            continue
        if evt.get("status") != 200:
            continue

        body = evt.get("body")
        if not isinstance(body, dict):
            continue

        text = body.get("text")
        if not text:
            continue

        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue

        data_part = payload.get("data") or {}
        # Merge keys from this response into the global gql_data
        for key, value in data_part.items():
            # if the same key appears twice, keep the first one (usually fine here)
            if key not in gql_data:
                gql_data[key] = value


    data = parse_rew_listing(html, url)
    # Ensure rew_url is set even if parser forgets
    data.setdefault("rew_url", url)

    # Validate listing
    validate_listing_data(data)

    # Extract address fields from parsed data
    street = (data.get("street_address") or "").strip()
    city = (data.get("city") or "").strip()
    province = (data.get("province") or "BC").strip()
    postal = (data.get("postal_code") or "").strip() or None

    # Resolve or create canonical property
    prop = await get_or_create_property(
        session=session,
        street=street,
        city=city,
        province=province,
        postal_code=postal,
        lat=data.get("lat"),
        lng=data.get("lng"),
    )

    # Attach property_id to listing payload
    prop_id = prop.id
    data["property_id"] = prop_id

    await upsert_listing(session, data)
    logger.info(f"Upserted listing {data.get('mls_number')} from {url}")

    # Create Structural Snapshot from listing data
    pc = PropertyCharacteristics(
        property_id=prop_id,
        as_of_date=datetime.utcnow().date(),
        source="rew",
        beds=data.get("beds"),
        baths=data.get("baths"),
        sqft_finished=data.get("sqft"),
        lot_sqft=data.get("lot_sqft"),
        raw_blob=data,
    )
    logger.info("Creating Structural Snapshot...")
    session.add(pc)
    try:
        await session.commit()
    except Exception:
        await session.rollback()  # ignore duplicate snapshot errors
    
    # If available, add Assessment history and/or Sales history
    if gql_data:

        # === Parse JSON ===
        assessments = parse_rew_assessment_history(gql_data)
        sales = parse_rew_sales_history(gql_data)

        # === Insert assessments ===
        if assessments:
            logger.info(f"Found {len(assessments)} historical assessments...")
            rows = [{
                "property_id": prop_id,
                "assessment_year": a["assessment_year"],
                "total_assessed_cad": a["total_assessed_cad"],
                "land_value": a["land_value"],
                "building_value": a["building_value"],
                "source": "rew_graphql",
                "raw_blob": a["raw"],
            } for a in assessments]

            stmt = pg_insert(Assessment).values(rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["property_id", "assessment_year", "source"]
            )
            await session.execute(stmt)

        # === Insert sales ===
        if sales:
            logger.info(f"Found {len(sales)} historical sales...")
            rows = [{
                "property_id": prop_id,
                "sale_date": s["sale_date"],
                "sale_price_cad": s["sale_price_cad"],
                "list_price_cad": None,
                "mls_number": None,
                "source": "rew_graphql",
                "beds": None,
                "baths": None,
                "sqft": None,
                "lot_sqft": None,
                "raw_blob": s["raw"],
            } for s in sales]

            stmt = pg_insert(Sale).values(rows)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["property_id", "sale_date", "sale_price_cad", "source"]
            )
            await session.execute(stmt)

        await session.commit()


async def main():
    await init_db()
    logger.info("DB init complete")

    # Configure browser ONCE
    browser_conf = BrowserConfig( 
        headless=True,
        enable_stealth=True, 
        verbose=False, 
        extra_args=[
            "--no-sandbox", 
            "--disable-dev-shm-usage", 
            "--disable-gpu"
        ])

    # Long-running worker loop
    logger.info("Creating AsyncWebCrawler...")
    async with AsyncWebCrawler(config=browser_conf) as crawler:
        logger.info("Crawler ready, entering main loop")
        while True:
            # Create new session every batch to prevent stale connections
            async with AsyncSessionLocal() as session:
                logger.info(f"Starting new batch...")
                batch = await dequeue_next_batch(session, batch_size=5)

                if not batch:
                    logger.info(f"No pending URLs. Sleeping for {EMPTY_QUEUE_SLEEP_SECONDS} seconds...")
                    await asyncio.sleep(EMPTY_QUEUE_SLEEP_SECONDS)
                    continue

                for url_id, url in batch:
                    try:
                        await scrape_listing_detail(crawler, session, url)
                        await mark_done(session, url_id)
                    except Exception as e:
                        # Also includes validation failures
                        await mark_failed(session, url_id, str(e))
                        logger.exception(f"Failed: {url} ({e})")
                    logger.info(f"Finished scraping... Sleeping for {PER_URL_SLEEP_SECONDS} seconds.")
                    await asyncio.sleep(PER_URL_SLEEP_SECONDS + random.uniform(0, 1))


if __name__ == "__main__":
    asyncio.run(main())
