# rew_discover_all.py
import asyncio
import random
from datetime import datetime
from urllib.parse import urljoin
import logging, pathlib
from logging_config import setup_logging

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup

from models import AsyncSessionLocal, init_db
from url_queue import enqueue_urls  # inserts into rew_listing_urls

BASE_URL = "https://www.rew.ca"
PAGE_LOAD_TIMEOUT_SECONDS = 90  # Safety timeout per page
PER_PAGE_SLEEP_SECONDS = 1      # To prevent rate limiting between successful pages

# Retry / backoff settings
MAX_RETRIES_PER_PAGE = 3
BACKOFF_BASE_SECONDS = 2        # starting backoff
BACKOFF_MAX_SECONDS = 30        # clamp max backoff
MAX_CONSECUTIVE_FAILED_PAGES = 3

logger = logging.getLogger(f"discoverer.{pathlib.Path(__file__).stem}")
setup_logging()


async def fetch_page_with_retries(crawler, url: str, run_config: CrawlerRunConfig) -> str | None:
    """
    Try to fetch a page with retries and exponential backoff.
    Returns HTML string on success, or None if all retries fail.
    """
    for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
        try:
            logger.info(f"({attempt}/{MAX_RETRIES_PER_PAGE}) {url}")
            result = await asyncio.wait_for(
                crawler.arun(url=url, config=run_config),
                timeout=PAGE_LOAD_TIMEOUT_SECONDS,
            )

            # Respect crawl4ai's success flag if present
            if hasattr(result, "success") and not result.success:
                err_msg = getattr(result, "error_message", "unknown error")
                raise RuntimeError(f"crawler reported failure: {err_msg}")

            html = getattr(result, "html", None) or getattr(result, "content", "") or ""
            if not html:
                raise RuntimeError("empty HTML content")

            # Success
            return html

        except (asyncio.TimeoutError, Exception) as e:
            # Last attempt -> give up
            if attempt == MAX_RETRIES_PER_PAGE:
                logger.error(f"Failed to fetch {url} after {attempt} attempts: {e}")
                return None

            # Compute backoff with jitter: base * 2^(attempt-1) + random(0,1)
            backoff = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1)
            backoff = min(backoff, BACKOFF_MAX_SECONDS)
            logger.warning(
                f"Error fetching {url} (attempt {attempt}/{MAX_RETRIES_PER_PAGE}): {e}. "
                f"Retrying in {backoff:.2f} seconds..."
            )
            await asyncio.sleep(backoff)


async def discover_all() -> int:
    """
    Crawl REW 'latest' listings pages starting from page 1 and continue
    until we hit a page with no listing URLs OR too many consecutive failures.
    Insert all unique URLs into rew_listing_urls via enqueue_urls().

    Returns the number of new URLs inserted.
    """
    logger.info(f"Starting full discovery of BC latest listings...")

    browser_config = BrowserConfig(
        headless=True,
        enable_stealth=True,
        verbose=True,  # Set to False in production to reduce noise
        extra_args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    )

    run_config = CrawlerRunConfig(
        wait_for="css:.displaycard",
        cache_mode=CacheMode.BYPASS,
    )

    listing_urls = set()
    page = 1
    consecutive_failed_pages = 0

    async with AsyncWebCrawler(config=browser_config) as crawler:
        while True:
            url = f"{BASE_URL}/properties/areas/bc/sort/latest"
            if page > 1:
                url += f"/page/{page}"

            logger.info(f"Processing page {page}: {url}")

            html = await fetch_page_with_retries(crawler, url, run_config)

            # If we still have no HTML after retries, count a failed page and decide whether to stop
            if not html:
                consecutive_failed_pages += 1
                logger.warning(f"Giving up on page {page}. Consecutive failed pages: {consecutive_failed_pages}")

                if consecutive_failed_pages >= MAX_CONSECUTIVE_FAILED_PAGES:
                    logger.error(
                        f"Reached {MAX_CONSECUTIVE_FAILED_PAGES} consecutive failed pages. "
                        f"Stopping discovery."
                    )
                    break

                # Skip this page, move to the next
                page += 1
                continue

            # Reset failure counter on success
            consecutive_failed_pages = 0

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

            logger.info(f"  -> page {page}: found {len(page_urls)} listing URLs")

            # Stop when we reach a page with no listings
            if not page_urls:
                logger.info(f"No listing URLs found on page {page}. Assuming end of pagination.")
                break

            listing_urls.update(page_urls)

            # Sleep a bit before next page to avoid rate limiting
            sleep_for = PER_PAGE_SLEEP_SECONDS + random.uniform(0, 1)
            logger.info(f"Sleeping {sleep_for:.2f} seconds before next page...")
            await asyncio.sleep(sleep_for)

            page += 1

    # Enqueue into DB
    inserted = 0
    if listing_urls:
        async with AsyncSessionLocal() as session:
            inserted = await enqueue_urls(list(listing_urls), session)

    logger.info(f"Discovery complete.")
    logger.info(f"  Total pages visited (including failed/empty): {page - 1}")
    logger.info(f"  Unique listing URLs found: {len(listing_urls)}")
    logger.info(f"  Inserted {inserted} new URLs into rew_listing_urls.")
    return inserted


async def main():
    await init_db()
    await discover_all()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Discovery stopped by user.")
