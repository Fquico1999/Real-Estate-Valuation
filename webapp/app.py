# webapp/app.py
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, func

from models import Base, RewListing, RewListingUrl

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://rewuser:rewpass@db:5432/real_estate",
)

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
templates = Jinja2Templates(directory="templates")

app = FastAPI(title="REW Listings Viewer")


@app.on_event("startup")
async def on_startup():
    # Ensure tables exist (idempotent)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    async with AsyncSessionLocal() as session:
        total_stmt = select(func.count(RewListing.id))
        total = (await session.execute(total_stmt)).scalar() or 0

        latest_stmt = (
            select(RewListing)
            .order_by(RewListing.scraped_at.desc())
            .limit(10)
        )
        latest = (await session.execute(latest_stmt)).scalars().all()
        
        #URL queue stats
        total_urls_stmt = select(func.count(RewListingUrl.id))
        total_urls = (await session.execute(total_urls_stmt)).scalar() or 0

        done_urls_stmt = select(func.count(RewListingUrl.id)).where(
            RewListingUrl.status == "done"
        )
        done_urls = (await session.execute(done_urls_stmt)).scalar() or 0

        error_urls_stmt = select(func.count(RewListingUrl.id)).where(
            RewListingUrl.status == "error"
        )
        error_urls = (await session.execute(error_urls_stmt)).scalar() or 0

        pending_urls = total_urls - done_urls - error_urls
        scrape_ratio = (done_urls / total_urls) if total_urls > 0 else 0.0

        # Latest discovered URL (any status)
        latest_discovered_stmt = (
            select(RewListingUrl)
            .order_by(RewListingUrl.discovered_at.desc())
            .limit(1)
        )
        latest_discovered = (
            await session.execute(latest_discovered_stmt)
        ).scalars().first()

        # Latest successfully scraped URL
        latest_done_stmt = (
            select(RewListingUrl)
            .where(RewListingUrl.status == "done")
            .order_by(RewListingUrl.last_attempt_at.desc().nullslast())
            .limit(1)
        )
        latest_done = (await session.execute(latest_done_stmt)).scalars().first()

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "total": total,
            "latest": latest,
            # URL stats
            "total_urls": total_urls,
            "done_urls": done_urls,
            "pending_urls": pending_urls,
            "error_urls": error_urls,
            "scrape_ratio": scrape_ratio,
            "latest_discovered": latest_discovered,
            "latest_done": latest_done,
        },
    )


@app.get("/listings", response_class=HTMLResponse)
async def listings(request: Request, page: int = 1, page_size: int = 20):
    offset = (page - 1) * page_size

    async with AsyncSessionLocal() as session:
        stmt = (
            select(RewListing)
            .order_by(RewListing.price_cad.desc().nullslast())
            .offset(offset)
            .limit(page_size)
        )
        rows = (await session.execute(stmt)).scalars().all()

        count_stmt = select(func.count(RewListing.id))
        total = (await session.execute(count_stmt)).scalar() or 0

    total_pages = max((total + page_size - 1) // page_size, 1)

    return templates.TemplateResponse(
        "listings.html",
        {
            "request": request,
            "listings": rows,
            "page": page,
            "total_pages": total_pages,
        },
    )
