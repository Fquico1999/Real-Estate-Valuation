# webapp/app.py
import os
import json
from collections import defaultdict

from typing import Optional, Dict, List, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi import Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, func

from models import Base, RewListing, RewListingUrl, Sale, Assessment

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://rewuser:rewpass@db:5432/real_estate",
)

PREFERRED_ASSESSMENT_SOURCES: List[str] = ["bc_assessment", "rew_graphql"]
PREFERRED_SALE_SOURCES: List[str] = ["land_title", "mls", "rew_graphql"]

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
templates = Jinja2Templates(directory="templates")

app = FastAPI(title="REW Listings Viewer")

def parse_int(value: Optional[str]) -> Optional[int]:
    """Convert a query string to int, or None if empty/invalid."""
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None

def _pick_primary_source(source: str, preferred: List[str]) -> int:
    """Helper to sort sources by preference."""
    try:
        return preferred.index(source)
    except ValueError:
        return len(preferred)


def merge_assessments(assessments: List[Assessment]) -> List[Dict[str, Any]]:
    """
    Merge assessments by (property_id, assessment_year), preferring
    official sources per year and computing % change vs previous year.
    """
    by_year: Dict[int, List[Assessment]] = defaultdict(list)
    for a in assessments:
        by_year[a.assessment_year].append(a)

    merged_rows: List[Dict[str, Any]] = []
    for year, rows in by_year.items():
        rows_sorted = sorted(
            rows,
            key=lambda r: _pick_primary_source(r.source, PREFERRED_ASSESSMENT_SOURCES),
        )
        primary = rows_sorted[0]
        merged_rows.append(
            {
                "assessment_year": year,
                "total_assessed_cad": primary.total_assessed_cad,
                "land_value": primary.land_value,
                "building_value": primary.building_value,
                "primary_source": primary.source,
                "all_sources": [r.source for r in rows],
                "change_pct": None,  # computed below
            }
        )

    # sort by year descending and compute change vs previous year
    merged_rows.sort(key=lambda r: r["assessment_year"], reverse=True)
    previous = None
    for row in merged_rows:
        if previous is not None and previous["total_assessed_cad"]:
            delta = row["total_assessed_cad"] - previous["total_assessed_cad"]
            row["change_pct"] = (delta / previous["total_assessed_cad"]) * 100.0
        previous = row

    return merged_rows


def merge_sales(sales: List[Sale]) -> List[Dict[str, Any]]:
    """
    Merge sales by (sale_date, sale_price_cad), preferring more trusted sources
    for each transaction.
    """
    if not sales:
        return []

    # cluster by exact (date, price) for now
    clusters: Dict[tuple, List[Sale]] = defaultdict(list)
    for s in sales:
        key = (s.sale_date, s.sale_price_cad)
        clusters[key].append(s)

    merged_rows: List[Dict[str, Any]] = []
    for (sale_date, sale_price), rows in clusters.items():
        rows_sorted = sorted(
            rows,
            key=lambda r: _pick_primary_source(r.source, PREFERRED_SALE_SOURCES),
        )
        primary = rows_sorted[0]

        price_per_sqft = None
        if primary.sale_price_cad and primary.sqft and primary.sqft > 0:
            price_per_sqft = primary.sale_price_cad / primary.sqft

        merged_rows.append(
            {
                "sale_date": sale_date,
                "sale_price_cad": sale_price,
                "price_per_sqft": price_per_sqft,
                "beds": primary.beds,
                "baths": primary.baths,
                "sqft": primary.sqft,
                "primary_source": primary.source,
                "all_sources": [r.source for r in rows],
            }
        )

    merged_rows.sort(key=lambda r: r["sale_date"], reverse=True)
    return merged_rows


def group_assessments_by_source(assessments: List[Assessment]) -> Dict[str, List[Assessment]]:
    """
    Return {source: [Assessment, ...]} sorted by year desc.
    """
    by_source: Dict[str, List[Assessment]] = defaultdict(list)
    for a in assessments:
        by_source[a.source].append(a)

    for rows in by_source.values():
        rows.sort(key=lambda a: a.assessment_year, reverse=True)

    return dict(by_source)


def group_sales_by_source(sales: List[Sale]) -> Dict[str, List[Sale]]:
    """
    Return {source: [Sale, ...]} sorted by date desc.
    """
    by_source: Dict[str, List[Sale]] = defaultdict(list)
    for s in sales:
        by_source[s.source].append(s)

    for rows in by_source.values():
        rows.sort(key=lambda s: s.sale_date, reverse=True)

    return dict(by_source)


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

@app.get("/listings/{listing_id}", response_class=HTMLResponse)
async def listing_detail(request: Request, listing_id: int):
    async with AsyncSessionLocal() as session:
        stmt = select(RewListing).where(RewListing.id == listing_id)
        listing = (await session.execute(stmt)).scalar_one_or_none()

        if listing is None:
            raise HTTPException(status_code=404, detail="Listing not found")
        
        merged_assessments: List[Dict[str, Any]] = []
        merged_sales: List[Dict[str, Any]] = []
        raw_assessments_by_source: Dict[str, List[Assessment]] = {}
        raw_sales_by_source: Dict[str, List[Sale]] = {}

        # Only attempt to lookup history if listing is linked to cannonical property
        if listing.property_id is not None:
            assessment_result = await session.execute(
                select(Assessment).where(
                    Assessment.property_id == listing.property_id
                )
            )
            assessments: List[Assessment] = assessment_result.scalars().all()
            
            sales_result = await session.execute(
                select(Sale).where(
                    Sale.property_id == listing.property_id
                )
            )
            sales: List[Sale] = sales_result.scalars().all()

            merged_assessments = merge_assessments(assessments)
            merged_sales = merge_sales(sales)
            raw_assessments_by_source = group_assessments_by_source(assessments)
            raw_sales_by_source = group_sales_by_source(sales)

    return templates.TemplateResponse(
        "listing_detail.html",
        {
            "request": request,
            "listing": listing,
            "merged_assessments": merged_assessments,
            "merged_sales": merged_sales,
            "raw_assessments_by_source": raw_assessments_by_source, 
            "raw_sales_by_source": raw_sales_by_source
        },
    )

@app.get("/map", response_class=HTMLResponse)
async def map_view( 
    request: Request, 
    min_price: Optional[str] = Query(default=None),
    max_price: Optional[str] = Query(default=None),
    min_beds: Optional[str] = Query(default=None),
    min_baths: Optional[str] = Query(default=None),
    focus_id: Optional[int] = Query(default=None),
    ):

    # Safely parse query params to ints
    min_price_int = parse_int(min_price)
    max_price_int = parse_int(max_price)
    min_beds_int = parse_int(min_beds)
    min_baths_int = parse_int(min_baths)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(RewListing)
            .where(RewListing.lat.isnot(None), RewListing.lng.isnot(None))
            )
        # Apply filters if provided
        if min_price_int is not None:
            stmt = stmt.where(RewListing.price_cad >= min_price_int)
        if max_price_int is not None:
            stmt = stmt.where(RewListing.price_cad <= max_price_int)
        if min_beds_int is not None:
            stmt = stmt.where(RewListing.beds >= min_beds_int)
        if min_baths_int is not None:
            stmt = stmt.where(RewListing.baths >= min_baths_int)

        stmt = (
            stmt
            .order_by(RewListing.scraped_at.desc())
            .limit(2000)  # hardcoded cap. To be replaced by dynamic loading later.
        )

        rows = (await session.execute(stmt)).scalars().all()

    # Convert to a simple list of dicts for JSON use in the template
    listing_points = [
        {
            "id": l.id,
            "lat": l.lat,
            "lng": l.lng,
            "price": l.price_cad,
            "address": l.street_address,
            "city": l.city,
            "neighbourhood": l.neighbourhood,
            "url": l.rew_url,
            "detail_url": str(request.url_for("listing_detail", listing_id=l.id)),
            "beds": l.beds,
            "baths": l.baths,
            "sqft": l.sqft,
        }
        for l in rows
        if l.lat is not None and l.lng is not None
    ]

    return templates.TemplateResponse(
        "map.html",
        {
            "request": request,
            "listing_points": listing_points,
            "min_price": min_price,
            "max_price": max_price,
            "min_beds": min_beds,
            "min_baths": min_baths,
            "focus_id": focus_id,
        },
    )

