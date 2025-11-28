# webapp/app.py
import os
import json
from collections import defaultdict

from typing import Optional, Dict, List, Any
from datetime import datetime, date

from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi import Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, func, and_, or_

from models import (
    Base,
    RewListing,
    RewListingUrl,
    Sale,
    Assessment,
    Property,
    PropertyCharacteristics,
    RawScrape,
    BCAssessmentUrl,
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://rewuser:rewpass@db:5432/real_estate",
)

PREFERRED_ASSESSMENT_SOURCES: List[str] = ["bc_assessment", "rew_graphql"]
PREFERRED_SALE_SOURCES: List[str] = ["land_title", "mls", "rew_graphql", "bc_assessment"]
PREFERRED_CHARACTERISTICS_SOURCES: List[str] = ["rew_graphql", "bc_assessment", "mls"]

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
templates = Jinja2Templates(directory="templates")

app = FastAPI(title="REW Listings Viewer")
app.mount("/static", StaticFiles(directory="static"), name="static")


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


def merge_property_characteristics(chars: List[PropertyCharacteristics]) -> List[Dict[str, Any]]:
    """
    Merge property characteristics by (property_id, as_of_date), preferring
    more trusted sources for each snapshot, just like merge_assessments does
    per assessment_year.

    Output is ordered by as_of_date desc.
    """
    if not chars:
        return []

    # group all rows by as_of_date (similar idea to year in assessments)
    by_date: Dict[date, List[PropertyCharacteristics]] = defaultdict(list)
    for c in chars:
        by_date[c.as_of_date].append(c)

    merged_rows: List[Dict[str, Any]] = []

    for as_of, rows in by_date.items():
        # pick primary source for this snapshot
        rows_sorted = sorted(
            rows,
            key=lambda r: _pick_primary_source(
                r.source, PREFERRED_CHARACTERISTICS_SOURCES
            ),
        )
        primary = rows_sorted[0]

        # you can choose which fields to surface; these mirror the model
        merged_rows.append(
            {
                "as_of_date": as_of,
                "beds": primary.beds,
                "baths": primary.baths,
                "sqft_finished": primary.sqft_finished,
                "sqft_unfinished": primary.sqft_unfinished,
                "lot_sqft": primary.lot_sqft,
                "year_built": primary.year_built,
                "primary_source": primary.source,
                "all_sources": [r.source for r in rows],
                # optional: capture “freshness” if you have scraped_at
                "latest_scraped_at": max(
                    (getattr(r, "scraped_at", None) for r in rows),
                    default=None,
                ),
            }
        )

    # sort by as_of_date desc (same pattern as merge_assessments sorts by year)
    merged_rows.sort(key=lambda r: r["as_of_date"], reverse=True)
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


def group_characteristics_by_source(chars: List[PropertyCharacteristics]) -> Dict[str, List[PropertyCharacteristics]]:
    """
    Return {source: [PropertyCharacteristics, ...]} sorted by as_of_date desc.

    This mirrors group_assessments_by_source (sorted by year) and
    group_sales_by_source (sorted by sale_date).
    """
    by_source: Dict[str, List[PropertyCharacteristics]] = defaultdict(list)
    for c in chars:
        by_source[c.source].append(c)

    for rows in by_source.values():
        rows.sort(key=lambda c: c.as_of_date, reverse=True)

    return dict(by_source)


@app.on_event("startup")
async def on_startup():
    # Ensure tables exist (idempotent)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    async with AsyncSessionLocal() as session:
        # --- Global summary -------------------------------------------------
        total_listings_stmt = select(func.count(RewListing.id))
        total_listings = (await session.execute(total_listings_stmt)).scalar() or 0

        total_properties = (
            await session.execute(select(func.count(Property.id)))
        ).scalar() or 0

        total_sales = (
            await session.execute(select(func.count(Sale.id)))
        ).scalar() or 0

        total_assessments = (
            await session.execute(select(func.count(Assessment.id)))
        ).scalar() or 0

        total_property_chars = (
            await session.execute(select(func.count(PropertyCharacteristics.id)))
        ).scalar() or 0

        total_raw_scrapes = (
            await session.execute(select(func.count(RawScrape.id)))
        ).scalar() or 0

        # --- REW scraper panel ----------------------------------------------
        latest_stmt = (
            select(RewListing)
            .order_by(RewListing.scraped_at.desc())
            .limit(10)
        )
        latest = (await session.execute(latest_stmt)).scalars().all()

        # URL queue stats for REW
        total_rew_urls_stmt = select(func.count(RewListingUrl.id))
        total_rew_urls = (
            await session.execute(total_rew_urls_stmt)
        ).scalar() or 0

        rew_done_urls_stmt = select(func.count(RewListingUrl.id)).where(
            RewListingUrl.status == "done"
        )
        rew_done_urls = (
            await session.execute(rew_done_urls_stmt)
        ).scalar() or 0

        rew_error_urls_stmt = select(func.count(RewListingUrl.id)).where(
            RewListingUrl.status == "error"
        )
        rew_error_urls = (
            await session.execute(rew_error_urls_stmt)
        ).scalar() or 0

        # Dead (hit MAX_SCRAPE_ATTEMPTS inside mark_failed)
        rew_dead_urls_stmt = select(func.count(RewListingUrl.id)).where(
            RewListingUrl.status == "dead"
        )
        rew_dead_urls = (await session.execute(rew_dead_urls_stmt)).scalar() or 0

        rew_pending_urls = total_rew_urls - rew_done_urls - rew_error_urls - rew_dead_urls
        rew_scrape_ratio = (
            rew_done_urls / total_rew_urls if total_rew_urls > 0 else 0.0
        )

        # Latest discovered REW URL (any status)
        latest_rew_discovered_stmt = (
            select(RewListingUrl)
            .order_by(RewListingUrl.discovered_at.desc())
            .limit(1)
        )
        latest_rew_discovered = (
            await session.execute(latest_rew_discovered_stmt)
        ).scalars().first()

        # Latest successfully scraped REW URL
        latest_rew_done_stmt = (
            select(RewListingUrl)
            .where(RewListingUrl.status == "done")
            .order_by(RewListingUrl.last_attempt_at.desc().nullslast())
            .limit(1)
        )
        latest_rew_done = (
            await session.execute(latest_rew_done_stmt)
        ).scalars().first()

        # --- BC Assessment scraper panel ------------------------------------
        total_bca_urls_stmt = select(func.count(BCAssessmentUrl.id))
        total_bca_urls = (
            await session.execute(total_bca_urls_stmt)
        ).scalar() or 0

        bca_done_urls_stmt = select(func.count(BCAssessmentUrl.id)).where(
            BCAssessmentUrl.status == "done"
        )
        bca_done_urls = (
            await session.execute(bca_done_urls_stmt)
        ).scalar() or 0

        bca_error_urls_stmt = select(func.count(BCAssessmentUrl.id)).where(
            BCAssessmentUrl.status == "error"
        )
        bca_error_urls = (
            await session.execute(bca_error_urls_stmt)
        ).scalar() or 0

        bca_dead_urls_stmt = select(func.count(BCAssessmentUrl.id)).where(
            BCAssessmentUrl.status == "dead"
        )
        bca_dead_urls = (await session.execute(bca_dead_urls_stmt)).scalar() or 0


        bca_pending_urls = total_bca_urls - bca_done_urls - bca_error_urls - bca_dead_urls
        bca_scrape_ratio = (
            bca_done_urls / total_bca_urls if total_bca_urls > 0 else 0.0
        )

        latest_bca_discovered_stmt = (
            select(BCAssessmentUrl)
            .order_by(BCAssessmentUrl.discovered_at.desc())
            .limit(1)
        )
        latest_bca_discovered = (
            await session.execute(latest_bca_discovered_stmt)
        ).scalars().first()

        latest_bca_done_stmt = (
            select(BCAssessmentUrl)
            .where(BCAssessmentUrl.status == "done")
            .order_by(BCAssessmentUrl.last_attempt_at.desc().nullslast())
            .limit(1)
        )
        latest_bca_done = (
            await session.execute(latest_bca_done_stmt)
        ).scalars().first()

        # Latest BC Assessment properties (by characteristics)
        latest_bca_props_stmt = (
            select(PropertyCharacteristics, Property)
            .join(Property, Property.id == PropertyCharacteristics.property_id)
            .where(PropertyCharacteristics.source == "bc_assessment")
            .order_by(
                PropertyCharacteristics.scraped_at.desc().nullslast(),
                PropertyCharacteristics.id.desc(),
            )
            .limit(10)
        )

        latest_bca_props = (await session.execute(latest_bca_props_stmt)).all()

        # Geocoding coverage
        geocoded_properties_stmt = select(func.count(Property.id)).where(
            Property.lat.isnot(None),
            Property.lng.isnot(None),
        )
        geocoded_properties = (
            await session.execute(geocoded_properties_stmt)
        ).scalar() or 0

        ungeocoded_properties = max(
            total_properties - geocoded_properties, 0
        )

        geocoded_ratio = (
            geocoded_properties / total_properties
            if total_properties > 0
            else 0.0
        )

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            # Global summary
            "total_listings": total_listings,
            "total_properties": total_properties,
            "total_sales": total_sales,
            "total_assessments": total_assessments,
            "total_property_chars": total_property_chars,
            "total_raw_scrapes": total_raw_scrapes,
            "total_rew_urls": total_rew_urls,
            "total_bca_urls": total_bca_urls,
            # Geocoding stats
            "geocoded_properties": geocoded_properties,
            "ungeocoded_properties": ungeocoded_properties,
            "geocoded_ratio": geocoded_ratio,
            # REW scraper stats
            "latest": latest,
            "rew_done_urls": rew_done_urls,
            "rew_pending_urls": rew_pending_urls,
            "rew_error_urls": rew_error_urls,
            "rew_dead_urls": rew_dead_urls,
            "rew_scrape_ratio": rew_scrape_ratio,
            "latest_rew_discovered": latest_rew_discovered,
            "latest_rew_done": latest_rew_done,
            # Backwards-compatible names (if anything else uses them)
            "total": total_listings,
            "total_urls": total_rew_urls,
            "done_urls": rew_done_urls,
            "pending_urls": rew_pending_urls,
            "error_urls": rew_error_urls,
            "scrape_ratio": rew_scrape_ratio,
            "latest_discovered": latest_rew_discovered,
            "latest_done": latest_rew_done,
            # BC Assessment scraper stats
            "bca_done_urls": bca_done_urls,
            "bca_pending_urls": bca_pending_urls,
            "bca_error_urls": bca_error_urls,
            "bca_dead_urls": bca_dead_urls,
            "bca_scrape_ratio": bca_scrape_ratio,
            "latest_bca_discovered": latest_bca_discovered,
            "latest_bca_done": latest_bca_done,
            "latest_bca_props": latest_bca_props,
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


@app.get("/properties", name="properties_search", response_class=HTMLResponse)
async def properties_search(
    request: Request,
    q: Optional[str] = Query(default=None, description="Search by address"),
    min_beds: Optional[str] = Query(default=None),
    max_beds: Optional[str] = Query(default=None),
    min_baths: Optional[str] = Query(default=None),
    max_baths: Optional[str] = Query(default=None),
    min_price: Optional[str] = Query(default=None),
    max_price: Optional[str] = Query(default=None),
    min_sqft: Optional[str] = Query(default=None),
    max_sqft: Optional[str] = Query(default=None),
    property_type: Optional[str] = Query(default=None),
    neighbourhood: Optional[str] = Query(default=None),
):
    """
    Property-centric search view.

    - Always returns Property objects.
    - Default (no filters): last ~100 properties by most recent scraped data.
    - Filters:
        * Address substring (q)
        * beds / baths / sqft via latest PropertyCharacteristics
        * price via latest Assessment.total_assessed_cad
        * neighbourhood + property type via latest REW listing
    """

    # Parse numeric filters safely
    min_beds_int = parse_int(min_beds)
    max_beds_int = parse_int(max_beds)
    min_baths_int = parse_int(min_baths)
    max_baths_int = parse_int(max_baths)
    min_price_int = parse_int(min_price)
    max_price_int = parse_int(max_price)
    min_sqft_int = parse_int(min_sqft)
    max_sqft_int = parse_int(max_sqft)

    async with AsyncSessionLocal() as session:
         # --- Subqueries to get "latest" related rows per property -------------

        # Latest structural snapshot per property (for beds/baths/sqft and recency)
        char_latest_sub = (
            select(
                PropertyCharacteristics.property_id.label("pc_property_id"),
                func.max(PropertyCharacteristics.scraped_at).label("pc_max_scraped_at"),
            )
            .group_by(PropertyCharacteristics.property_id)
            .subquery()
        )

        # Latest assessment per property (for price filter)
        assess_latest_sub = (
            select(
                Assessment.property_id.label("a_property_id"),
                func.max(Assessment.assessment_year).label("a_max_year"),
            )
            .group_by(Assessment.property_id)
            .subquery()
        )

        # Latest REW listing per property (for type / neighbourhood)
        listing_latest_sub = (
            select(
                RewListing.property_id.label("l_property_id"),
                func.max(RewListing.scraped_at).label("l_max_scraped_at"),
            )
            .where(RewListing.property_id.isnot(None))
            .group_by(RewListing.property_id)
            .subquery()
        )

        # --- Base query: Property + "current" joined data ---------------------

        stmt = (
            select(
                Property,
                PropertyCharacteristics,
                Assessment,
                RewListing,
                char_latest_sub.c.pc_max_scraped_at.label("char_last_scraped_at"),
            )
            # latest characteristics (optional)
            .join(
                char_latest_sub,
                char_latest_sub.c.pc_property_id == Property.id,
                isouter=True,
            )
            .join(
                PropertyCharacteristics,
                and_(
                    PropertyCharacteristics.property_id == Property.id,
                    PropertyCharacteristics.scraped_at
                    == char_latest_sub.c.pc_max_scraped_at,
                ),
                isouter=True,
            )
            # latest assessment (optional)
            .join(
                assess_latest_sub,
                assess_latest_sub.c.a_property_id == Property.id,
                isouter=True,
            )
            .join(
                Assessment,
                and_(
                    Assessment.property_id == Property.id,
                    Assessment.assessment_year == assess_latest_sub.c.a_max_year,
                ),
                isouter=True,
            )
            # latest REW listing (optional)
            .join(
                listing_latest_sub,
                listing_latest_sub.c.l_property_id == Property.id,
                isouter=True,
            )
            .join(
                RewListing,
                and_(
                    RewListing.property_id == Property.id,
                    RewListing.scraped_at == listing_latest_sub.c.l_max_scraped_at,
                ),
                isouter=True,
            )
        )

        # --- Address search over Properties (not listings) --------------------

        if q:
            pattern = f"%{q.strip()}%"
            stmt = stmt.where(
                or_(
                    Property.street_address.ilike(pattern),
                    Property.city.ilike(pattern),
                    Property.canonical_address.ilike(pattern),
                )
            )

        # --- Feature filters (using joined tables) ----------------------------

        # beds/baths/sqft from latest characteristics
        if min_beds_int is not None:
            stmt = stmt.where(PropertyCharacteristics.beds >= min_beds_int)
        if max_beds_int is not None:
            stmt = stmt.where(PropertyCharacteristics.beds <= max_beds_int)

        if min_baths_int is not None:
            stmt = stmt.where(PropertyCharacteristics.baths >= min_baths_int)
        if max_baths_int is not None:
            stmt = stmt.where(PropertyCharacteristics.baths <= max_baths_int)

        if min_sqft_int is not None:
            stmt = stmt.where(PropertyCharacteristics.sqft_finished >= min_sqft_int)
        if max_sqft_int is not None:
            stmt = stmt.where(PropertyCharacteristics.sqft_finished <= max_sqft_int)

        # price from latest assessment (total_assessed_cad)
        if min_price_int is not None:
            stmt = stmt.where(Assessment.total_assessed_cad >= min_price_int)
        if max_price_int is not None:
            stmt = stmt.where(Assessment.total_assessed_cad <= max_price_int)

        # Property type & neighbourhood from latest REW listing
        if property_type:
            stmt = stmt.where(RewListing.property_type_human == property_type)

        if neighbourhood:
            stmt = stmt.where(RewListing.neighbourhood == neighbourhood)

        # --- Default ordering: "most recently scraped" properties -------------

        # Coalesce in order of preference:
        #   1) latest listing scrape
        #   2) latest characteristics scrape
        #   3) property.created_at
        recency_order_col = func.coalesce(
            listing_latest_sub.c.l_max_scraped_at,
            char_latest_sub.c.pc_max_scraped_at,
            Property.created_at,
        )

        stmt = stmt.order_by(recency_order_col.desc()).limit(100)

        result = await session.execute(stmt)
        rows = result.all()

        # Normalize into a list of dicts for the template
        properties = []
        for prop, char, assessment, listing, char_last_scraped_at in rows:
            # pick a display "last scraped" timestamp, prefer listing then char
            last_scraped_at = None
            if listing is not None and getattr(listing, "scraped_at", None):
                last_scraped_at = listing.scraped_at
            elif char_last_scraped_at is not None:
                last_scraped_at = char_last_scraped_at

            properties.append(
                {
                    "property": prop,
                    "char": char,
                    "assessment": assessment,
                    "listing": listing,
                    "last_scraped_at": last_scraped_at,
                }
            )

        # Dropdown values for filters (from REW listings)
        neighbourhoods_stmt = (
            select(func.distinct(RewListing.neighbourhood))
            .where(RewListing.neighbourhood.isnot(None))
            .order_by(RewListing.neighbourhood)
        )
        property_types_stmt = (
            select(func.distinct(RewListing.property_type_human))
            .where(RewListing.property_type_human.isnot(None))
            .order_by(RewListing.property_type_human)
        )

        neighbourhoods = [
            r[0] for r in (await session.execute(neighbourhoods_stmt)).all() if r[0]
        ]
        property_types = [
            r[0] for r in (await session.execute(property_types_stmt)).all() if r[0]
        ]

    return templates.TemplateResponse(
        "properties.html",
        {
            "request": request,
            "query": q or "",
            "min_beds": min_beds,
            "max_beds": max_beds,
            "min_baths": min_baths,
            "max_baths": max_baths,
            "min_price": min_price,
            "max_price": max_price,
            "min_sqft": min_sqft,
            "max_sqft": max_sqft,
            "property_type": property_type or "",
            "neighbourhood": neighbourhood or "",
            "properties": properties,
            "neighbourhoods": neighbourhoods,
            "property_types": property_types,
        },
    )


@app.get("/properties/{property_id}", response_class=HTMLResponse)
async def property_detail(request: Request, property_id: int):
    async with AsyncSessionLocal() as session:
        # 1) Load Property
        prop_result = await session.execute(
            select(Property).where(Property.id == property_id)
        )
        prop = prop_result.scalar_one_or_none()
        if prop is None:
            raise HTTPException(status_code=404, detail="Property not found")

        # 2) All REW listings for this property (active + history), newest scrape first
        listings_result = await session.execute(
            select(RewListing)
            .where(RewListing.property_id == property_id)
            .order_by(RewListing.scraped_at.desc().nullslast())
        )
        listings: List[RewListing] = listings_result.scalars().all()

        active_listing = listings[0] if listings else None

        # 3) Assessments & sales (same as listing_detail, but property-centric)
        assessments_result = await session.execute(
            select(Assessment).where(Assessment.property_id == property_id)
        )
        assessments = assessments_result.scalars().all()

        sales_result = await session.execute(
            select(Sale).where(Sale.property_id == property_id)
        )
        sales = sales_result.scalars().all()

        merged_assessments = merge_assessments(assessments)
        merged_sales = merge_sales(sales)
        raw_assessments_by_source = group_assessments_by_source(assessments)
        raw_sales_by_source = group_sales_by_source(sales)

        # 4) PropertyCharacteristics
        chars_result = await session.execute(
            select(PropertyCharacteristics)
            .where(PropertyCharacteristics.property_id == property_id)
            .order_by(PropertyCharacteristics.scraped_at.desc().nullslast())
        )
        chars = chars_result.scalars().all()

        merged_chars = merge_property_characteristics(chars)
        raw_chars_by_source = group_characteristics_by_source(chars)

        # “Current” snapshot for the hero – newest as_of_date (merged list is already sorted desc)
        current_char = merged_chars[0] if merged_chars else None

    return templates.TemplateResponse(
        "property_detail.html",
        {
            "request": request,
            "property": prop,
            "active_listing": active_listing,
            "listings": listings,
            "merged_assessments": merged_assessments,
            "merged_sales": merged_sales,
            "raw_assessments_by_source": raw_assessments_by_source,
            "raw_sales_by_source": raw_sales_by_source,
            "merged_chars": merged_chars,
            "raw_chars_by_source": raw_chars_by_source,
            "current_char": current_char,
        }
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

