
# Real Estate Valuation Pipeline

An automated data ingestion pipeline designed to discover, scrape, and store real estate listings (currently targeting `rew.ca`) for historical analysis and valuation modeling.

## Project Goal

To build a robust, self-updating dataset of real estate properties by:

1.  Continuously discovering new property URLs.
2.  Scraping detailed property attributes (price, sqft, address, etc.).
3.  Providing a dashboard to monitor ingestion health and view data.

Additionally, to leverage the dataset and experiment with modelling property valuation.

## To Do
- [ ] Increase information gathered per listing - days on market, amenities, etc.
- [ ] Figure out how to represent past sales effectively - list of tuples?
- [ ] Mechanism for updating days on market automatically - run a script daily to increment days on market?
- [ ] Mechanism for revisting listings after some time has passed - Add a last-updated/last-checked param to RewListingURLs to revisit/scrape listings
- [ ] Consider adding other sources than REW.

## Architecture

The system runs entirely on **Docker** and consists of four orchestrated services:

  * **`db` (PostgreSQL 16):** Serves as both the data storage for listings and the job queue for URLs.
  * **`discoverer` (Python/Crawl4AI):** Runs hourly to crawl search result pages (e.g., "Latest Vancouver Listings") and pushes new URLs into the queue.
  * **`scraper` (Python/Crawl4AI):** Continuously polls the database for "pending" URLs, visits the listing page using a headless browser, scrapes details, and upserts the data.
  * **`webapp` (FastAPI/Jinja2):** A lightweight web dashboard to view extraction statistics, queue status, and raw listing data.

## Getting Started

### Prerequisites

  * Docker Desktop (or Docker Engine + Compose)

### Installation & Running

1.  **Clone the repository:**

    ```bash
    git clone <repository-url>
    cd Real-Estate-Valuation
    ```

2.  **Launch the stack:**

    ```bash
    # Builds images and starts all services
    docker-compose up --build -d
    ```

3.  **Access the Dashboard:**
    Open your browser to [http://localhost:8000](http://localhost:8000).

### Configuration

The project uses `docker-compose.yml` for configuration.

  * **Database Credentials:** Default is `rewuser` / `rewpass` (defined in compose).
  * **Concurrency:** Browser resource limits (`shm_size`) and arguments are configured in the compose file and Python scripts to ensure stability in Docker.

## Project Structure

```text
├── db/             # Database initialization SQL
├── scraper/        # Scraper logic (Discoverer & Detail Scraper)
│   ├── rew_discover_worker.py  # Finds URLs
│   ├── rew_detail_scraper.py   # Extracts Data
│   └── models.py               # DB Schemas
├── webapp/         # FastAPI Dashboard
│   ├── app.py                  # Routes & Views
│   └── templates/              # HTML UI
└── docker-compose.yml
```
