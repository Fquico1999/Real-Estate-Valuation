from sqlalchemy import insert
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models import RewListingUrl

MAX_SCRAPE_ATTEMPTS = 5

async def enqueue_urls(urls: list[str], session: AsyncSession) -> int:
    """Insert new URLs into rew_listing_urls, ignoring duplicates, in bulk."""
    if not urls:
        return 0

    # Build a bulk INSERT ... ON CONFLICT DO NOTHING
    stmt = pg_insert(RewListingUrl).values([{"url": u} for u in urls])

    # URL has a unique constraint, so we target that
    stmt = stmt.on_conflict_do_nothing(index_elements=[RewListingUrl.url])

    result = await session.execute(stmt)
    await session.commit()

    # result.rowcount is the number of actually inserted rows
    return result.rowcount or 0

async def dequeue_next_batch(session: AsyncSession, batch_size=10):
    """
    Fetch and lock the next batch of URLs to scrape.
    Uses SKIP LOCKED to allow multiple workers.
    """

    stmt = text("""
        WITH picked AS (
            SELECT id, url
            FROM rew_listing_urls
            WHERE 
                (
                    status = 'pending'
                    OR (status = 'error' AND attempts < :max_attempts)
                )
            ORDER BY discovered_at
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE rew_listing_urls
        SET status='scraping',
            attempts = attempts + 1,
            last_attempt_at = NOW()
        WHERE id IN (SELECT id FROM picked)
        RETURNING id, url;
    """)

    rows = (await session.execute(stmt, {"batch_size": batch_size, "max_attempts": MAX_SCRAPE_ATTEMPTS})).fetchall()
    await session.commit()
    return rows

async def mark_done(session: AsyncSession, url_id: int):
    await session.execute(
        text("""
            UPDATE rew_listing_urls
            SET status='done'
            WHERE id=:id
        """),
        {"id": url_id}
    )
    await session.commit()


async def mark_failed(session: AsyncSession, url_id: int, error_msg: str):
    await session.execute(
        text("""
            UPDATE rew_listing_urls
            SET status='error',
                last_error=:err
            WHERE id=:id
        """),
        {"id": url_id, "err": error_msg}
    )
    await session.commit()
