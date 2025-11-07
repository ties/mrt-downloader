"""Index caching using SQLite to avoid re-downloading completed month indexes."""

import aiosqlite
import datetime
import logging
from pathlib import Path
from typing import Optional

from mrt_downloader.models import CollectorFileEntry, CollectorInfo

logger = logging.getLogger(__name__)

# Cache refresh threshold: only refresh indexes for months that ended less than this many seconds ago
# Default: 7 days = 7 * 24 * 60 * 60 seconds
CACHE_REFRESH_THRESHOLD_SECONDS = 7 * 24 * 60 * 60

# Collector cache refresh threshold: refresh collector list if cached for longer than this
# Default: 24 hours = 24 * 60 * 60 seconds
COLLECTOR_CACHE_REFRESH_THRESHOLD_SECONDS = 24 * 60 * 60


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

    Creates three tables:
    - collector_cache: Stores collector information (cached for 24h)
    - index_cache: Tracks which indexes have been downloaded and when
    - file_cache: Stores the parsed CollectorFileEntry objects for each index

    Args:
        db_path: Path to the database file. If None, uses default cache path.
    """
    if db_path is None:
        db_path = get_cache_db_path()

    async with aiosqlite.connect(db_path) as db:
        # Table for storing collectors (cached for 24h)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS collector_cache (
                project TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                installed TEXT NOT NULL,
                removed TEXT,
                cached_at INTEGER NOT NULL,
                PRIMARY KEY (project, name)
            )
        """)

        # Table for tracking processed indexes
        await db.execute("""
            CREATE TABLE IF NOT EXISTS index_cache (
                url TEXT PRIMARY KEY,
                downloaded_at INTEGER NOT NULL,
                month_end_date TEXT NOT NULL
            )
        """)

        # Table for storing parsed file entries
        await db.execute("""
            CREATE TABLE IF NOT EXISTS file_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_url TEXT NOT NULL,
                collector_name TEXT NOT NULL,
                collector_project TEXT NOT NULL,
                collector_base_url TEXT NOT NULL,
                collector_installed TEXT NOT NULL,
                collector_removed TEXT,
                filename TEXT NOT NULL,
                file_url TEXT NOT NULL,
                file_type TEXT,
                FOREIGN KEY (index_url) REFERENCES index_cache(url) ON DELETE CASCADE
            )
        """)

        # Index for faster lookups
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_cache_index_url
            ON file_cache(index_url)
        """)

        await db.commit()

    logger.debug(f"Initialized cache database at {db_path}")


def should_refresh_index(month_end_date: datetime.datetime) -> bool:
    """Check if an index should be refreshed based on the month it represents.

    An index should be refreshed (not cached) if:
    1. It's for the current month (files are still being added), OR
    2. The month ended less than CACHE_REFRESH_THRESHOLD_SECONDS ago (7 days by default)

    This ensures we get fresh data for:
    - The current month (always refreshed)
    - Recent months where files might still be uploaded late

    Args:
        month_end_date: The last day of the month (at 23:59:59)

    Returns:
        True if the index should be refreshed, False if cached version can be used
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    # Make month_end_date timezone-aware if it isn't already
    if month_end_date.tzinfo is None:
        month_end_date = month_end_date.replace(tzinfo=datetime.timezone.utc)

    # Check if this is the current month
    current_month = (now.year, now.month)
    index_month = (month_end_date.year, month_end_date.month)

    if index_month == current_month:
        logger.debug(f"Index is for current month ({month_end_date.year}-{month_end_date.month:02d}), will refresh")
        return True

    # Check if the month ended recently (within threshold)
    time_since_month_end = (now - month_end_date).total_seconds()

    # If month_end_date is in the future (shouldn't happen with proper usage),
    # treat it as needing refresh
    if time_since_month_end < 0:
        logger.warning(f"Month end date {month_end_date} is in the future, will refresh")
        return True

    should_refresh = time_since_month_end < CACHE_REFRESH_THRESHOLD_SECONDS

    logger.debug(
        f"Month {month_end_date.year}-{month_end_date.month:02d} ended {time_since_month_end:.0f}s ago, "
        f"threshold is {CACHE_REFRESH_THRESHOLD_SECONDS}s, "
        f"should_refresh={should_refresh}"
    )

    return should_refresh


async def get_cached_index(
    url: str,
    month_end_date: datetime.datetime,
    force_refresh: bool = False,
    db_path: Optional[Path] = None
) -> Optional[list[CollectorFileEntry]]:
    """Get cached file entries for an index if they exist and are still valid.

    Args:
        url: The index URL to look up
        month_end_date: The last day of the month this index represents
        force_refresh: If True, ignore cache and return None
        db_path: Path to the database file. If None, uses default cache path.

    Returns:
        List of CollectorFileEntry objects if valid cache exists, None otherwise
    """
    if force_refresh:
        logger.debug(f"Force refresh enabled, skipping cache for {url}")
        return None

    if db_path is None:
        db_path = get_cache_db_path()

    # If the month recently ended, we should refresh the index
    if should_refresh_index(month_end_date):
        logger.debug(f"Month is recent, skipping cache for {url}")
        return None

    try:
        async with aiosqlite.connect(db_path) as db:
            # Check if the index is in cache
            async with db.execute(
                "SELECT downloaded_at FROM index_cache WHERE url = ?",
                (url,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    logger.debug(f"No cache entry found for {url}")
                    return None

                downloaded_at = row[0]
                logger.info(f"Using cached index for {url} (downloaded at {downloaded_at})")

            # Retrieve all file entries for this index
            async with db.execute(
                """
                SELECT collector_name, collector_project, collector_base_url,
                       collector_installed, collector_removed,
                       filename, file_url, file_type
                FROM file_cache
                WHERE index_url = ?
                """,
                (url,)
            ) as cursor:
                rows = await cursor.fetchall()

                file_entries = []
                for row in rows:
                    (collector_name, collector_project, collector_base_url,
                     collector_installed, collector_removed,
                     filename, file_url, file_type) = row

                    # Reconstruct CollectorInfo
                    collector = CollectorInfo(
                        name=collector_name,
                        project=collector_project,
                        base_url=collector_base_url,
                        installed=datetime.datetime.fromisoformat(collector_installed),
                        removed=datetime.datetime.fromisoformat(collector_removed) if collector_removed else None
                    )

                    # Reconstruct CollectorFileEntry
                    file_entry = CollectorFileEntry(
                        collector=collector,
                        filename=filename,
                        url=file_url,
                        file_type=file_type
                    )
                    file_entries.append(file_entry)

                logger.debug(f"Retrieved {len(file_entries)} file entries from cache for {url}")
                return file_entries

    except Exception as e:
        # If the database doesn't exist or there's an error, just return None
        logger.debug(f"Cache lookup failed for {url}: {e}")
        return None


async def store_index(
    url: str,
    file_entries: list[CollectorFileEntry],
    month_end_date: datetime.datetime,
    db_path: Optional[Path] = None
) -> None:
    """Store parsed file entries for an index in the cache.

    Args:
        url: The index URL
        file_entries: List of CollectorFileEntry objects parsed from the index
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
            # Store index metadata
            await db.execute(
                """
                INSERT OR REPLACE INTO index_cache (url, downloaded_at, month_end_date)
                VALUES (?, ?, ?)
                """,
                (url, now, month_end_str)
            )

            # Delete old file entries for this index (if replacing)
            await db.execute(
                "DELETE FROM file_cache WHERE index_url = ?",
                (url,)
            )

            # Store all file entries
            for entry in file_entries:
                await db.execute(
                    """
                    INSERT INTO file_cache (
                        index_url, collector_name, collector_project, collector_base_url,
                        collector_installed, collector_removed,
                        filename, file_url, file_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        url,
                        entry.collector.name,
                        entry.collector.project,
                        entry.collector.base_url,
                        entry.collector.installed.isoformat(),
                        entry.collector.removed.isoformat() if entry.collector.removed else None,
                        entry.filename,
                        entry.url,
                        entry.file_type
                    )
                )

            await db.commit()

        logger.debug(f"Stored {len(file_entries)} file entries in cache for {url}")
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


async def get_cached_collectors(
    project: str,
    force_refresh: bool = False,
    db_path: Optional[Path] = None
) -> Optional[list[CollectorInfo]]:
    """Get cached collectors for a project if they exist and are still valid.

    Args:
        project: The project name ("ris" or "routeviews")
        force_refresh: If True, ignore cache and return None
        db_path: Path to the database file. If None, uses default cache path.

    Returns:
        List of CollectorInfo objects if valid cache exists, None otherwise
    """
    if force_refresh:
        logger.debug(f"Force refresh enabled, skipping cache for {project} collectors")
        return None

    if db_path is None:
        db_path = get_cache_db_path()

    try:
        async with aiosqlite.connect(db_path) as db:
            # Get all collectors for this project and check if any are stale
            async with db.execute(
                "SELECT name, base_url, installed, removed, cached_at FROM collector_cache WHERE project = ?",
                (project,)
            ) as cursor:
                rows = await cursor.fetchall()

                if not rows:
                    logger.debug(f"No cached collectors found for {project}")
                    return None

                # Check if cache is still fresh (any stale entry invalidates the whole cache)
                now = datetime.datetime.now(datetime.timezone.utc).timestamp()
                collectors = []

                for row in rows:
                    name, base_url, installed_str, removed_str, cached_at = row

                    # Check if this entry is stale
                    age = now - cached_at
                    if age > COLLECTOR_CACHE_REFRESH_THRESHOLD_SECONDS:
                        logger.debug(
                            f"Collector cache for {project} is stale "
                            f"(age: {age:.0f}s > {COLLECTOR_CACHE_REFRESH_THRESHOLD_SECONDS}s)"
                        )
                        return None

                    # Reconstruct CollectorInfo
                    collector = CollectorInfo(
                        name=name,
                        project=project,
                        base_url=base_url,
                        installed=datetime.datetime.fromisoformat(installed_str),
                        removed=datetime.datetime.fromisoformat(removed_str) if removed_str else None
                    )
                    collectors.append(collector)

                logger.info(f"Using {len(collectors)} cached collectors for {project}")
                return collectors

    except Exception as e:
        logger.debug(f"Collector cache lookup failed for {project}: {e}")
        return None


async def store_collectors(
    project: str,
    collectors: list[CollectorInfo],
    db_path: Optional[Path] = None
) -> None:
    """Store collectors in the cache.

    Args:
        project: The project name ("ris" or "routeviews")
        collectors: List of CollectorInfo objects to cache
        db_path: Path to the database file. If None, uses default cache path.
    """
    if db_path is None:
        db_path = get_cache_db_path()

    # Ensure database is initialized
    await init_cache_db(db_path)

    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

    try:
        async with aiosqlite.connect(db_path) as db:
            # Delete old collectors for this project
            await db.execute(
                "DELETE FROM collector_cache WHERE project = ?",
                (project,)
            )

            # Store all collectors
            for collector in collectors:
                await db.execute(
                    """
                    INSERT INTO collector_cache (project, name, base_url, installed, removed, cached_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        project,
                        collector.name,
                        collector.base_url,
                        collector.installed.isoformat(),
                        collector.removed.isoformat() if collector.removed else None,
                        now
                    )
                )

            await db.commit()

        logger.debug(f"Stored {len(collectors)} collectors in cache for {project}")
    except Exception as e:
        # Log but don't fail if caching fails
        logger.warning(f"Failed to store collector cache for {project}: {e}")
