"""Index caching using SQLite to avoid re-downloading completed month indexes."""

import aiosqlite
import datetime
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Cache refresh threshold: only refresh indexes for months that ended less than this many seconds ago
# Default: 7 days = 7 * 24 * 60 * 60 seconds
CACHE_REFRESH_THRESHOLD_SECONDS = 7 * 24 * 60 * 60


def get_cache_db_path() -> Path:
    """Get the path to the SQLite cache database.

    Returns:
        Path to ~/.cache/mrt-downloader/state.sqlite3
    """
    cache_dir = Path.home() / ".cache" / "mrt-downloader"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "state.sqlite3"


async def init_cache_db(db_path: Optional[Path] = None) -> None:
    """Initialize the cache database and create tables if they don't exist.

    Args:
        db_path: Path to the database file. If None, uses default cache path.
    """
    if db_path is None:
        db_path = get_cache_db_path()

    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS index_cache (
                url TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                downloaded_at INTEGER NOT NULL,
                month_end_date TEXT NOT NULL
            )
        """)
        await db.commit()

    logger.debug(f"Initialized cache database at {db_path}")


def should_refresh_index(month_end_date: datetime.datetime) -> bool:
    """Check if an index should be refreshed based on how recently the month ended.

    An index should be refreshed if the last day of the month is less than
    CACHE_REFRESH_THRESHOLD_SECONDS ago, as files might still be added.

    Args:
        month_end_date: The last day of the month (at 23:59:59)

    Returns:
        True if the index should be refreshed, False if cached version can be used
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    # Make month_end_date timezone-aware if it isn't already
    if month_end_date.tzinfo is None:
        month_end_date = month_end_date.replace(tzinfo=datetime.timezone.utc)

    time_since_month_end = (now - month_end_date).total_seconds()
    should_refresh = time_since_month_end < CACHE_REFRESH_THRESHOLD_SECONDS

    logger.debug(
        f"Month ended {time_since_month_end:.0f}s ago, "
        f"threshold is {CACHE_REFRESH_THRESHOLD_SECONDS}s, "
        f"should_refresh={should_refresh}"
    )

    return should_refresh


async def get_cached_index(
    url: str,
    month_end_date: datetime.datetime,
    db_path: Optional[Path] = None
) -> Optional[str]:
    """Get a cached index if it exists and is still valid.

    Args:
        url: The index URL to look up
        month_end_date: The last day of the month this index represents
        db_path: Path to the database file. If None, uses default cache path.

    Returns:
        The cached HTML content if valid, None otherwise
    """
    if db_path is None:
        db_path = get_cache_db_path()

    # If the month recently ended, we should refresh the index
    if should_refresh_index(month_end_date):
        logger.debug(f"Month is recent, skipping cache for {url}")
        return None

    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                "SELECT content, downloaded_at FROM index_cache WHERE url = ?",
                (url,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    content, downloaded_at = row
                    logger.info(f"Using cached index for {url} (downloaded at {downloaded_at})")
                    return content
    except Exception as e:
        # If the database doesn't exist or there's an error, just return None
        logger.debug(f"Cache lookup failed for {url}: {e}")
        return None

    logger.debug(f"No cache entry found for {url}")
    return None


async def store_index(
    url: str,
    content: str,
    month_end_date: datetime.datetime,
    db_path: Optional[Path] = None
) -> None:
    """Store an index in the cache.

    Args:
        url: The index URL
        content: The HTML content to cache
        month_end_date: The last day of the month this index represents
        db_path: Path to the database file. If None, uses default cache path.
    """
    if db_path is None:
        db_path = get_cache_db_path()

    # Ensure database is initialized
    await init_cache_db(db_path)

    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    month_end_str = month_end_date.isoformat()

    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO index_cache (url, content, downloaded_at, month_end_date)
                VALUES (?, ?, ?, ?)
                """,
                (url, content, now, month_end_str)
            )
            await db.commit()

        logger.debug(f"Stored index cache for {url}")
    except Exception as e:
        # Log but don't fail if caching fails
        logger.warning(f"Failed to store index cache for {url}: {e}")


def get_month_end_date(year: int, month: int) -> datetime.datetime:
    """Get the last moment of a given month (last day at 23:59:59).

    Args:
        year: The year
        month: The month (1-12)

    Returns:
        Datetime representing the last second of the month (UTC)
    """
    # Get first day of next month, then subtract one second
    if month == 12:
        next_month = datetime.datetime(year + 1, 1, 1, tzinfo=datetime.timezone.utc)
    else:
        next_month = datetime.datetime(year, month + 1, 1, tzinfo=datetime.timezone.utc)

    last_moment = next_month - datetime.timedelta(seconds=1)
    return last_moment
