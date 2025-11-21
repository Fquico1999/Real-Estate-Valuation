# scraper/rew_detail_scraper.py
import asyncio
import os

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from sqlalchemy import select

from models import RewListing, AsyncSessionLocal, init_db
from parsers import parse_rew_listing
from url_queue import dequeue_next_batch, mark_done, mark_failed

EMPTY_QUEUE_SLEEP_SECONDS = 60  # how long to sleep when no URLs are pending
PER_URL_SLEEP_SECONDS = 1 # How long to pause for after processing a URL
PAGE_LOAD_TIMEOUT_SECONDS = 60  # Safety timeout per page

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


async def scrape_listing_detail(crawler: AsyncWebCrawler, session, url: str):
    run_conf = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

    print(f"Scraping {url}")
    result = await asyncio.wait_for(
            crawler.arun(url=url, config=run_conf),
            timeout=PAGE_LOAD_TIMEOUT_SECONDS
        )

    # depending on crawl4ai version, this attribute might differ;
    # adjust to result.html / result.content if needed.
    html = getattr(result, "html", None) or getattr(result, "content", None)
    if not html:
        print(f"No HTML for {url}")
        return

    data = parse_rew_listing(html, url)
    await upsert_listing(session, data)
    print(f"Upserted listing {data.get('mls_number')} from {url}")


async def main():
    await init_db()
    print("[SCRAPER] DB init complete")

    # Configure browser ONCE
    browser_conf = BrowserConfig( 
        headless=True, 
        verbose=True, 
        extra_args=[
            "--no-sandbox", 
            "--disable-dev-shm-usage", 
            "--disable-gpu"
        ])

    # Long-running worker loop
    print("[SCRAPER] Creating AsyncWebCrawler...")
    async with AsyncWebCrawler(config=browser_conf) as crawler:
        print("[SCRAPER] Crawler ready, entering main loop")
        while True:
            # Create new session every batch to prevent stale connections
            async with AsyncSessionLocal() as session:
                print(f"Starting new batch...")
                batch = await dequeue_next_batch(session, batch_size=5)

                if not batch:
                    print(f"No pending URLs. Sleeping for {EMPTY_QUEUE_SLEEP_SECONDS} seconds...")
                    await asyncio.sleep(EMPTY_QUEUE_SLEEP_SECONDS)
                    continue

                for url_id, url in batch:
                    print(f"Scraping {url}")

                    try:
                        await scrape_listing_detail(crawler, session, url)
                        await mark_done(session, url_id)
                        print(f"Finished scraping... Sleeping for {PER_URL_SLEEP_SECONDS} seconds.")
                        await asyncio.sleep(PER_URL_SLEEP_SECONDS)
                    except Exception as e:
                        await mark_failed(session, url_id, str(e))
                        print(f"Failed: {url} ({e})")


if __name__ == "__main__":
    asyncio.run(main())
