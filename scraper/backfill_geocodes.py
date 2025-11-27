# scraper/backfill_geocodes.py
import asyncio
import logging
import pathlib
from typing import List

from sqlalchemy import select

from logging_config import setup_logging
from models import AsyncSessionLocal, Property
from geocoding import geocode

logger = logging.getLogger(f"scraper.{pathlib.Path(__file__).stem}")
setup_logging()

BATCH_SIZE = 500  # tune as needed


async def fetch_batch(session) -> List[Property]:
    result = await session.execute(
        select(Property)
        .where(
            Property.province == "BC",
            Property.lat.is_(None),
            Property.lng.is_(None),
        )
        .limit(BATCH_SIZE)
    )
    return result.scalars().all()


async def backfill_geocodes_once() -> int:
    async with AsyncSessionLocal() as session:
        props = await fetch_batch(session)
        if not props:
            logger.info("No properties left without coordinates.")
            return 0

        logger.info(f"Backfilling up to {len(props)} properties...")

        updated = 0
        for p in props:
            address = f"{p.street_address}, {p.city} {p.province} {p.postal_code or ''}"
            logger.info(f"Geocoding: {address!r} (id={p.id})")

            geo = await geocode(address)
            if geo.lat is None or geo.lng is None:
                continue

            p.lat = geo.lat
            p.lng = geo.lng

            # If bbox present and we don't already have one, store it
            if geo.bbox and p.bbox is None:
                p.bbox = geo.bbox.to_dict()

            updated += 1

        await session.commit()
        logger.info(f"Updated {updated} properties with lat/lng (and bbox where available).")
        return updated


async def main():
    total_updated = 0
    while True:
        updated = await backfill_geocodes_once()
        total_updated += updated
        if updated == 0:
            break
    logger.info(f"Backfill complete. Total updated: {total_updated}")


if __name__ == "__main__":
    asyncio.run(main())
