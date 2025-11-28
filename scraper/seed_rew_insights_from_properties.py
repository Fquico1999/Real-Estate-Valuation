# scraper/seed_rew_insights_from_properties.py
#
# Seed REW Insights data starting from the Properties table.
#
# For each Property:
#   - build a formatted address string
#   - call https://www.rew.ca/insights/autocomplete?term=<address>
#   - pick the first suggestion that looks like an Insights URL
#   - fetch that Insights page (plain requests)
#   - parse via parsers.parse_rew_insights(html, url)
#   - upsert:
#       * PropertyCharacteristics(source='rew_insights')
#       * Assessment(source='rew_insights')
#       * Sale(source='rew_insights')
#   - optionally enqueue neighbour Insights/listing URLs
#
# This is analogous in spirit to seed_bc_assessments_from_rew_listings.py,
# but uses REW's autocomplete HTTP endpoint instead of simulating a browser.

import asyncio
import logging
import pathlib
import random

from datetime import datetime, date
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from sqlalchemy import select, exists, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from logging_config import setup_logging

from models import (
    AsyncSessionLocal,
    init_db,
    Property,
    PropertyCharacteristics,
    Assessment,
    Sale,
)
from property_utils import format_full_address
from parsers import parse_rew_insights

from rew_insights_url_queue import enqueue_insights_urls
from url_queue import enqueue_urls


logger = logging.getLogger(f"scraper.{pathlib.Path(__file__).stem}")
setup_logging()

BASE_URL = "https://www.rew.ca"
INSIGHTS_AUTOCOMPLETE_PATH = "/insights/autocomplete"

# Tunables
BATCH_SIZE = 5
EMPTY_SLEEP_SECONDS = 60
PER_PROPERTY_SLEEP_SECONDS = 1


# ---------------------------------------------------------------------------
# Helpers for JSON / date handling
# ---------------------------------------------------------------------------

def _json_safe(value):
    """
    Recursively convert any date/datetime objects to ISO strings so they can
    be safely stored in a JSON/JSONB column.
    """
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _parse_rew_insights_sale_date(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None

    raw = raw.strip()
    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue

    logger.debug(f"Could not parse REW Insights sale date: {raw!r}")
    return None


# ---------------------------------------------------------------------------
# HTTP autocomplete -> Insights URL
# ---------------------------------------------------------------------------

def lookup_insights_url_via_autocomplete(address: str) -> Optional[str]:
    """
    Call the same autocomplete endpoint the REW Insights search uses and
    return the first Insights URL, or None if nothing matches.

    This is synchronous and uses requests; we call it from async code in a
    sequential fashion, which is acceptable for a seeder.
    """
    url = urljoin(BASE_URL, INSIGHTS_AUTOCOMPLETE_PATH)

    headers = {
        "User-Agent": "Mozilla/5.0 (rew-insights-seeder)",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": urljoin(BASE_URL, "/insights"),
    }

    params = {"term": address}

    resp = requests.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()

    try:
        data = resp.json()
    except Exception as e:
        logger.warning(f"Autocomplete JSON decode failed for address {address!r}: {e}")
        return None

    candidates: List[Dict[str, Any]] = []

    if isinstance(data, list):
        candidates = data
    elif isinstance(data, dict):
        # common patterns: {'results': [...]}, {'suggestions': [...]}, etc.
        for key in ("results", "suggestions", "items"):
            if isinstance(data.get(key), list):
                candidates = data[key]
                break

    for item in candidates:
        if not isinstance(item, dict):
            continue

        # Try common fields
        for field in ("url", "href", "link", "value"):
            val = item.get(field)
            if isinstance(val, str) and "/insights/" in val:
                return urljoin(BASE_URL, val)

        # Fallback: scan values for any string containing '/insights/'
        for v in item.values():
            if isinstance(v, str) and "/insights/" in v:
                return urljoin(BASE_URL, v)

    return None


# ---------------------------------------------------------------------------
# DB upsert helpers for REW Insights
# ---------------------------------------------------------------------------

async def upsert_property_characteristics_rew_insights(
    session,
    property_id: int,
    basic: Dict[str, Any],
):
    as_of_raw = basic.get("as_of_date")
    if isinstance(as_of_raw, str):
        # leave as string; schema probably stores date, but we'll let the DB
        # cast or fallback to today if empty
        try:
            as_of_date = datetime.fromisoformat(as_of_raw).date()
        except Exception:
            as_of_date = datetime.utcnow().date()
    elif isinstance(as_of_raw, date):
        as_of_date = as_of_raw
    else:
        as_of_date = datetime.utcnow().date()

    row = {
        "property_id": property_id,
        "as_of_date": as_of_date,
        "source": "rew_insights",
        "beds": basic.get("beds"),
        "baths": basic.get("baths"),
        "sqft_finished": basic.get("sqft"),
        "sqft_unfinished": None,
        "lot_sqft": None,
        "year_built": basic.get("year_built"),
        "raw_blob": _json_safe(basic),
        "scraped_at": datetime.utcnow(),
    }

    stmt = pg_insert(PropertyCharacteristics).values(row)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "as_of_date", "source"]
    )
    await session.execute(stmt)


async def upsert_assessments_rew_insights(
    session,
    property_id: int,
    assessments: List[Dict[str, Any]],
):
    if not assessments:
        return

    rows = []
    for a in assessments:
        year = a.get("assessment_year")
        total = a.get("total_assessed_cad")
        if not year or total is None:
            continue

        rows.append(
            {
                "property_id": property_id,
                "assessment_year": year,
                "total_assessed_cad": total,
                "land_value": a.get("land_value"),
                "building_value": a.get("building_value"),
                "source": "rew_insights",
                "raw_blob": _json_safe(a),
            }
        )

    if not rows:
        return

    stmt = pg_insert(Assessment).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "assessment_year", "source"]
    )
    await session.execute(stmt)


async def upsert_sales_rew_insights(
    session,
    property_id: int,
    sales: List[Dict[str, Any]],
):
    if not sales:
        return

    rows = []
    for s in sales:
        price = s.get("sale_price_cad")
        if price is None:
            continue
        d = _parse_rew_insights_sale_date(s.get("sale_date"))
        if not d:
            continue

        rows.append(
            {
                "property_id": property_id,
                "sale_date": d,
                "sale_price_cad": price,
                "list_price_cad": None,
                "mls_number": None,
                "source": "rew_insights",
                "beds": None,
                "baths": None,
                "sqft": None,
                "lot_sqft": None,
                "raw_blob": _json_safe(s.get("raw", {})),
            }
        )

    if not rows:
        return

    stmt = pg_insert(Sale).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["property_id", "sale_date", "sale_price_cad", "source"]
    )
    await session.execute(stmt)


# ---------------------------------------------------------------------------
# Batch fetching from Properties
# ---------------------------------------------------------------------------

async def fetch_next_property_batch(session, offset: int, batch_size: int = BATCH_SIZE):
    """
    Fetch a batch of Properties to seed REW Insights for.

    Strategy:
      - province == 'BC'
      - have street_address & city
      - no existing PropertyCharacteristics from 'rew_insights'
        (to avoid rework on properties we've already seeded)
    """
    pc_exists = exists().where(
        and_(
            PropertyCharacteristics.property_id == Property.id,
            PropertyCharacteristics.source == "rew_insights",
        )
    )

    stmt = (
        select(Property)
        .where(
            Property.province == "BC",
            Property.street_address.isnot(None),
            Property.city.isnot(None),
            ~pc_exists,
        )
        .order_by(Property.id)
        .offset(offset)
        .limit(batch_size)
    )

    result = await session.execute(stmt)
    return result.scalars().all()


# ---------------------------------------------------------------------------
# Per-property processing
# ---------------------------------------------------------------------------

async def process_single_property(session, prop: Property):
    """
    For a given Property:
      - Build a formatted address string
      - Use REW Insights autocomplete HTTP endpoint to find the Insights URL
      - Fetch that Insights page (requests)
      - Parse it and upsert into PropertyCharacteristics, Assessment, Sale
      - Optionally enqueue neighbour URLs
    """

    full_addr = format_full_address(
        prop.street_address,
        prop.city,
        prop.province,
        prop.postal_code,
    )

    logger.info(f"[Property {prop.id}] Lookup REW Insights for address: {full_addr!r}")

    try:
        insights_url = lookup_insights_url_via_autocomplete(full_addr)
    except Exception as e:
        logger.exception(
            f"[Property {prop.id}] Autocomplete lookup failed: {e}"
        )
        return

    if not insights_url:
        logger.info(
            f"[Property {prop.id}] No Insights URL found via autocomplete for {full_addr!r}"
        )
        return

    logger.info(f"[Property {prop.id}] Insights URL: {insights_url}")

    try:
        resp = requests.get(
            insights_url,
            headers={"User-Agent": "Mozilla/5.0 (rew-insights-seeder)"},
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.exception(
            f"[Property {prop.id}] Failed to fetch Insights page {insights_url}: {e}"
        )
        return

    html = resp.text

    parsed = parse_rew_insights(html, insights_url)
    basic = parsed.get("basic") or {}

    # If the page gives a better canonical address, we COULD reconcile here.
    # For seeding, we just attach data to the existing Property row.
    property_id = prop.id

    # Characteristics snapshot
    await upsert_property_characteristics_rew_insights(session, property_id, basic)

    # Assessments
    assessments = parsed.get("assessments") or []
    await upsert_assessments_rew_insights(session, property_id, assessments)

    # Sales history
    sales = parsed.get("sales") or []
    await upsert_sales_rew_insights(session, property_id, sales)

    # Neighbourhood links -> optional fan-out
    links = parsed.get("neighbourhood_links") or {}
    insights_urls = links.get("insights_urls") or []
    listing_urls = links.get("listing_urls") or []

    if insights_urls:
        inserted_insights = await enqueue_insights_urls(insights_urls, session)
        logger.info(
            f"[Property {prop.id}] Enqueued {inserted_insights} neighbouring REW Insights URLs"
        )
    
    if listing_urls:
        inserted_listings = await enqueue_urls(listing_urls, session)
        logger.info(
            f"[Property {prop.id}] Enqueued {inserted_listings} neighbouring REW listing URLs"
        )

    await session.commit()
    logger.info(
        f"[Property {prop.id}] Seeded REW Insights data "
        f"for property_id={property_id} ({full_addr})"
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def main():
    await init_db()
    logger.info("DB init complete for REW Insights seeder")

    offset = 0

    while True:
        async with AsyncSessionLocal() as session:
            logger.info(
                f"Fetching Properties batch: offset={offset}, size={BATCH_SIZE}"
            )
            props = await fetch_next_property_batch(
                session, offset=offset, batch_size=BATCH_SIZE
            )

            if not props:
                logger.info(
                    f"No more Properties needing REW Insights. "
                    f"Sleeping {EMPTY_SLEEP_SECONDS} seconds..."
                )
                await asyncio.sleep(EMPTY_SLEEP_SECONDS)
                # For a pure "one-off" seeder, you might prefer to break instead:
                # break
                continue

            for prop in props:
                try:
                    await process_single_property(session, prop)
                except Exception as e:
                    logger.exception(
                        f"Error processing Property {prop.id}: {e}"
                    )

                sleep_for = PER_PROPERTY_SLEEP_SECONDS + random.uniform(0, 1)
                logger.info(
                    f"Sleeping {sleep_for:.2f}s before next property..."
                )
                await asyncio.sleep(sleep_for)

            offset += len(props)


if __name__ == "__main__":
    asyncio.run(main())
