# scraper/rew_discover_worker.py
import asyncio
from datetime import datetime

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from models import AsyncSessionLocal, init_db
from url_queue import enqueue_urls  # helper that inserts into rew_listing_urls

BASE_URL = "https://www.rew.ca"
DISCOVERY_INTERVAL_SECONDS = 60 * 60  # 1 hour
PAGE_LOAD_TIMEOUT_SECONDS = 60  # Safety timeout per page

async def discover_once() -> int:
    """
    Run a single discovery pass: crawl a few 'latest' pages, extract listing URLs,
    enqueue them into the rew_listing_urls table.
    Returns the number of new URLs inserted.
    """
    print(f"[{datetime.utcnow().isoformat()}] Discovering Vancouver latest listings...")

    # Config: Run headless, disable cache to ensure we get fresh listings
    browser_config = BrowserConfig(
        headless=True, 
        verbose=True, # Set to False in production to reduce noise
        extra_args=[
            "--no-sandbox", 
            "--disable-dev-shm-usage", 
            "--disable-gpu"
        ]
    )

    run_config = CrawlerRunConfig(
        wait_for="css:.displaycard",
        cache_mode=CacheMode.BYPASS,
    )

    listing_urls = set()
    max_pages = 5

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for page in range(1, max_pages + 1):
            url = f"{BASE_URL}/properties/areas/vancouver-bc/sort/latest"
            if page > 1:
                url += f"/page/{page}"

            print(f"[DISCOVER] Fetching {url}")

            try:
                result = await crawler.arun(url=url, config=run_config)
            except asyncio.TimeoutError:
                print(f"[ERROR] Timeout while fetching {url} - skipping page.")
                continue
            except Exception as e:
                print(f"[ERROR] Failed to fetch {url}: {e}")
                continue

            # Check success status if available on result object
            if not result.success:
                print(f"[WARN] Crawl failed for {url}: {result.error_message}")
                continue

            html = result.html or ""
            soup = BeautifulSoup(html, "lxml")
            articles = soup.select("article.displaycard, article.marqueepanel")

            page_urls = set()
            for article in articles:
                a = article.select_one("a.displaycard-link, a.marqueepanel-link")
                if not a or not a.get("href"):
                    continue
                full_url = urljoin(BASE_URL, a["href"])
                if "/properties/" in full_url:
                    page_urls.add(full_url)

            print(f"  -> found {len(page_urls)} listing URLs on this page")
            listing_urls.update(page_urls)

    # enqueue into DB with dedicated session block
    inserted = 0
    if listing_urls:
        async with AsyncSessionLocal() as session:
            inserted = await enqueue_urls(list(listing_urls), session)

    print(f"[{datetime.utcnow().isoformat()}] Discovery complete. Inserted {inserted} new URLs.")
    return inserted


async def main():
    await init_db()

    while True:
        try:
            await discover_once()
        except Exception as e:
            print(f"[ERROR] discovery run failed: {e}")

        print(f"Sleeping {DISCOVERY_INTERVAL_SECONDS} seconds before next discovery...")
        await asyncio.sleep(DISCOVERY_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped by user.")
