# scraper/bc_assessment_url_queue.py
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import BCAssessmentUrl

MAX_SCRAPE_ATTEMPTS = 5


async def enqueue_property_urls(urls: list[str], session: AsyncSession) -> int:
    """
    Insert new BC Assessment property URLs into bc_assessment_urls, ignoring duplicates.
    """
    if not urls:
        return 0

    rows = []
    for u in urls:
        # Try to extract the BC property id from /Property/Info/<id>/
        bc_id = None
        parts = [p for p in u.split("/") if p]
        if "Info" in parts:
            idx = parts.index("Info")
            if idx + 1 < len(parts):
                bc_id = parts[idx + 1]

        rows.append({"url": u, "bc_property_id": bc_id})

    stmt = pg_insert(BCAssessmentUrl).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=[BCAssessmentUrl.url])

    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or 0


async def dequeue_next_batch(session: AsyncSession, batch_size: int = 5):
    """
    Fetch and lock the next batch of property URLs to scrape.
    Similar pattern to rew_listing_urls in url_queue.py.
    """
    stmt = text(
        """
        WITH picked AS (
            SELECT id, url
            FROM bc_assessment_urls
            WHERE
                (
                    status = 'pending'
                    OR (status = 'error' AND attempts < :max_attempts)
                )
            ORDER BY discovered_at
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE bc_assessment_urls
        SET status='scraping',
            attempts = attempts + 1,
            last_attempt_at = NOW()
        WHERE id IN (SELECT id FROM picked)
        RETURNING id, url;
        """
    )

    rows = (
        await session.execute(
            stmt,
            {"batch_size": batch_size, "max_attempts": MAX_SCRAPE_ATTEMPTS},
        )
    ).fetchall()
    await session.commit()
    return rows


async def mark_done(session: AsyncSession, row_id: int):
    await session.execute(
        text(
            """
            UPDATE bc_assessment_urls
            SET status='done'
            WHERE id=:id
            """
        ),
        {"id": row_id},
    )
    await session.commit()


async def mark_failed(session: AsyncSession, row_id: int, error_msg: str):
    await session.execute(
        text("""
            UPDATE bc_assessment_urls
            SET 
                status = CASE
                    WHEN attempts >= :max_attempts THEN 'dead'
                    ELSE 'error'
                END,
                last_error = :err
            WHERE id = :id
        """),
        {
            "id": row_id,
            "err": error_msg,
            "max_attempts": MAX_SCRAPE_ATTEMPTS,
        }
    )
    await session.commit()

