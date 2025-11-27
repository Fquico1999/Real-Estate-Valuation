# scraper/backfill_geocodes.py
import asyncio
import logging
import pathlib
from typing import List, Optional

from sqlalchemy import select

from logging_config import setup_logging
from models import AsyncSessionLocal, Property
from geocoding import geocode
from property_utils import format_full_address

logger = logging.getLogger(f"scraper.{pathlib.Path(__file__).stem}")
setup_logging()

BATCH_SIZE = 500


async def fetch_batch(session, last_id: Optional[int]) -> List[Property]:
    """
    Fetch a batch of BC properties with no lat/lng, in ID order, starting after last_id.
    Each property in this run will be seen at most once.
    """
    stmt = (
        select(Property)
        .where(
            Property.province == "BC",
            Property.lat.is_(None),
            Property.lng.is_(None),
        )
        .order_by(Property.id)
        .limit(BATCH_SIZE)
    )
    if last_id is not None:
        stmt = stmt.where(Property.id > last_id)

    result = await session.execute(stmt)
    return result.scalars().all()


async def backfill_geocodes_batch(last_id: Optional[int]) -> tuple[int, Optional[int]]:
    """
    Process one batch of properties and return (updated_count, new_last_id).

    new_last_id will be None if there are no more rows.
    """
    async with AsyncSessionLocal() as session:
        props = await fetch_batch(session, last_id)
        if not props:
            logger.info("No more properties with missing coordinates in this run.")
            return 0, None

        logger.info(
            f"Backfilling up to {len(props)} properties "
            f"(id {props[0].id}..{props[-1].id})..."
        )

        updated = 0
        for p in props:
            address = format_full_address(
                p.street_address,
                p.city,
                p.province,
                p.postal_code,
            )
            logger.info(f"Geocoding: {address!r} (id={p.id})")
            geo = await geocode(address)

            # If geocoder couldn't find anything, we just leave lat/lng as NULL
            if geo.lat is None or geo.lng is None:
                continue

            p.lat = geo.lat
            p.lng = geo.lng

            # If bbox present and we don't already have one, store it
            if geo.bbox and p.bbox is None:
                p.bbox = geo.bbox.to_dict()

            updated += 1

        await session.commit()
        logger.info(
            f"Updated {updated} properties with lat/lng "
            f"(and bbox where available) in this batch."
        )
        # Return last processed id so the next batch starts after it
        return updated, props[-1].id


async def main():
    total_updated = 0
    last_id: Optional[int] = None

    while True:
        updated, last_id = await backfill_geocodes_batch(last_id)
        total_updated += updated

        # No more rows with lat/lng missing for this run
        if last_id is None:
            break

    logger.info(f"Backfill complete. Total updated across all batches: {total_updated}")


if __name__ == "__main__":
    asyncio.run(main())
