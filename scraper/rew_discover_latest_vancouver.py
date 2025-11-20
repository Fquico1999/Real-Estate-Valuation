import asyncio
import json
from urllib.parse import urljoin
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

from url_queue import enqueue_urls

BASE_URL = "https://www.rew.ca"


async def scrape_listings():
    print("Discovering Vancouver latest listings...")

    # 1. Define the extraction schema based on your HTML structure
    # We look for both standard (.displaycard) and featured (.marqueepanel) listings
    schema = {
        "name": "Property Listings",
        "baseSelector": "article.displaycard, article.marqueepanel",
        "fields": [
            {
                "name": "path",
                "selector": "a.displaycard-link, a.marqueepanel-link",
                "type": "attribute",
                "attribute": "href"
            },
            {
                "name": "price",
                "selector": ".displaycard-title, .marqueepanel-title",
                "type": "text"
            }
        ]
    }

    # 2. Configure the browser (Headless=False helps debug if you get blocked)
    browser_config = BrowserConfig(
        headless=True, 
        verbose=True
    )

    # 3. Configure the run to wait for the specific REACT component to load
    run_config = CrawlerRunConfig(
        # Critical: Wait until the listing cards actually exist in the DOM
        wait_for="css:.displaycard", 
        
        # Use the structured extractor defined above
        extraction_strategy=JsonCssExtractionStrategy(schema),
        
        # Bypass cache to get fresh data
        cache_mode=CacheMode.BYPASS
    )

    listing_urls = set()
    max_pages = 3

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for page in range(1, max_pages + 1):
            url = f"{BASE_URL}/properties/areas/vancouver-bc/sort/latest"
            if page > 1:
                url += f"/page/{page}"

            print(f"[FETCH] {url}")

            # Run the crawler
            result = await crawler.arun(
                url=url,
                config=run_config
            )

            if not result.success:
                print(f"Error: {result.error_message}")
                continue

            # Parse the structured JSON output
            try:
                data = json.loads(result.extracted_content)
                
                # Normalize URLs (handle relative paths like /properties/...)
                for item in data:
                    if item.get('path'):
                        full_url = urljoin(BASE_URL, item['path'])
                        listing_urls.add(full_url)
                
                print(f"  -> found {len(data)} listings on this page")
                
            except json.JSONDecodeError:
                print("  -> Failed to parse JSON content")

            # Politeness delay
            await asyncio.sleep(2)

    print("\nFinal listings found:")
    for u in listing_urls:
        print(u)

    async with AsyncSessionLocal() as session:
        count = await enqueue_urls(list(listing_urls), session)
        print(f"Inserted {count} new URLS.")

if __name__ == "__main__":
    asyncio.run(scrape_listings())