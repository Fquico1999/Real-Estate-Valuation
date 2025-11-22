# scraper/rew_detail_scraper.py
import asyncio
import os
import random
import pathlib, logging
from logging_config import setup_logging

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from sqlalchemy import select

from models import RewListing, AsyncSessionLocal, init_db
from parsers import parse_rew_listing
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
    run_conf = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

    logger.info(f"Scraping {url}")
    result = await asyncio.wait_for(
            crawler.arun(url=url, config=run_conf),
            timeout=PAGE_LOAD_TIMEOUT_SECONDS
        )

    # depending on crawl4ai version, this attribute might differ;
    # adjust to result.html / result.content if needed.
    html = getattr(result, "html", None) or getattr(result, "content", None)
    if not html:
        logger.info(f"No HTML for {url}")
        return

    data = parse_rew_listing(html, url)
    # Ensure rew_url is set even if parser forgets
    data.setdefault("rew_url", url)

    # Validate listing
    validate_listing_data(data)

    await upsert_listing(session, data)
    logger.info(f"Upserted listing {data.get('mls_number')} from {url}")


async def main():
    await init_db()
    logger.info("DB init complete")

    # Configure browser ONCE
    browser_conf = BrowserConfig( 
        headless=True,
        enable_stealth=True, 
        verbose=True, 
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
                        logger.warning(f"Failed: {url} ({e})")
                    logger.info(f"Finished scraping... Sleeping for {PER_URL_SLEEP_SECONDS} seconds.")
                    await asyncio.sleep(PER_URL_SLEEP_SECONDS + random.uniform(0, 1))


if __name__ == "__main__":
    asyncio.run(main())
