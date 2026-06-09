"""Index caching using SQLite to avoid re-downloading completed month indexes."""

import asyncio
import datetime
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, TypeVar

import aiosqlite

from mrt_downloader.models import CollectorFileEntry, CollectorInfo

LOG = logging.getLogger(__name__)
T = TypeVar("T")

# Cache refresh threshold: only refresh indexes for months that ended less than this many seconds ago
# Default: 7 days = 7 * 24 * 60 * 60 seconds
CACHE_REFRESH_THRESHOLD_SECONDS = 7 * 24 * 60 * 60

# Collector cache refresh threshold: refresh collector list if cached for longer than this
# Default: 24 hours = 24 * 60 * 60 seconds
COLLECTOR_CACHE_REFRESH_THRESHOLD_SECONDS = 24 * 60 * 60

SQLITE_CONNECT_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30_000
SQLITE_LOCK_RETRIES = 5
SQLITE_LOCK_RETRY_INITIAL_DELAY_SECONDS = 0.25
CURRENT_CACHE_SCHEMA_VERSION = 2

_CACHE_WRITE_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}


def get_cache_db_path() -> Path:
    """Get the path to the SQLite cache database.

    Returns:
        Path to ~/.cache/mrt-downloader/state.sqlite3
    """
    cache_dir = Path.home() / ".cache" / "mrt-downloader"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "state.sqlite3"


def _normalized_db_path(db_path: Path) -> Path:
    return db_path.expanduser().resolve()


def _get_write_lock(db_path: Path) -> asyncio.Lock:
    loop_key = id(asyncio.get_running_loop())
    lock_key = (loop_key, str(_normalized_db_path(db_path)))
    lock = _CACHE_WRITE_LOCKS.get(lock_key)
    if lock is None:
        lock = asyncio.Lock()
        _CACHE_WRITE_LOCKS[lock_key] = lock
    return lock


def _is_sqlite_locked(exc: Exception) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False

    message = str(exc).lower()
    return "database is locked" in message or "database table is locked" in message


async def _retry_on_sqlite_lock(
    operation_name: str, operation: Callable[[], Awaitable[T]]
) -> T:
    last_exception: Exception | None = None

    for attempt in range(SQLITE_LOCK_RETRIES + 1):
        try:
            return await operation()
        except Exception as exc:
            if not _is_sqlite_locked(exc):
                raise

            last_exception = exc
            if attempt >= SQLITE_LOCK_RETRIES:
                break

            delay = SQLITE_LOCK_RETRY_INITIAL_DELAY_SECONDS * (2**attempt)
            LOG.debug(
                "%s failed because the cache database is locked "
                "(attempt %d/%d); retrying in %.2fs",
                operation_name,
                attempt + 1,
                SQLITE_LOCK_RETRIES + 1,
                delay,
            )
            await asyncio.sleep(delay)

    assert last_exception is not None
    raise last_exception


@asynccontextmanager
async def _connect_cache_db(db_path: Path):
    async with aiosqlite.connect(
        db_path,
        timeout=SQLITE_CONNECT_TIMEOUT_SECONDS,
    ) as db:
        await db.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        await db.execute("PRAGMA foreign_keys = ON")
        yield db


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

    async def initialize() -> None:
        async with _connect_cache_db(db_path) as db:
            await db.execute("PRAGMA journal_mode = WAL")
            await db.execute("PRAGMA synchronous = NORMAL")
            async with db.execute("PRAGMA user_version") as cursor:
                row = await cursor.fetchone()
                cache_version = int(row[0]) if row else 0

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

            if cache_version < 2 <= CURRENT_CACHE_SCHEMA_VERSION:
                await db.execute(
                    "DELETE FROM collector_cache WHERE project = 'routeviews'"
                )
                await db.execute(
                    """
                    DELETE FROM index_cache
                    WHERE url LIKE 'https://archive.routeviews.org/%'
                       OR url LIKE 'https://archive2.routeviews.org/%'
                    """
                )
                LOG.info(
                    "Invalidated RouteViews cache entries while migrating cache "
                    "from version %d to %d",
                    cache_version,
                    CURRENT_CACHE_SCHEMA_VERSION,
                )

            if cache_version < CURRENT_CACHE_SCHEMA_VERSION:
                await db.execute(
                    f"PRAGMA user_version = {CURRENT_CACHE_SCHEMA_VERSION}"
                )

            await db.commit()

    async with _get_write_lock(db_path):
        await _retry_on_sqlite_lock("Initialize cache database", initialize)

    LOG.debug("Using cache database at %s", db_path)


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
        LOG.debug(
            f"Index is for current month ({month_end_date.year}-{month_end_date.month:02d}), will refresh"
        )
        return True

    # Check if the month ended recently (within threshold)
    time_since_month_end = (now - month_end_date).total_seconds()

    # If month_end_date is in the future (shouldn't happen with proper usage),
    # treat it as needing refresh
    if time_since_month_end < 0:
        LOG.warning(f"Month end date {month_end_date} is in the future, will refresh")
        return True

    should_refresh = time_since_month_end < CACHE_REFRESH_THRESHOLD_SECONDS

    LOG.debug(
        f"Month {month_end_date.year}-{month_end_date.month:02d} ended {time_since_month_end:.0f}s ago, "
        f"threshold is {CACHE_REFRESH_THRESHOLD_SECONDS}s, "
        f"should_refresh={should_refresh}"
    )

    return should_refresh


async def get_cached_index(
    url: str,
    month_end_date: datetime.datetime,
    force_refresh: bool = False,
    db_path: Optional[Path] = None,
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
        LOG.debug(f"Force refresh enabled, skipping cache for {url}")
        return None

    if db_path is None:
        db_path = get_cache_db_path()

    # If the month recently ended, we should refresh the index
    if should_refresh_index(month_end_date):
        LOG.debug(f"Month is recent, skipping cache for {url}")
        return None

    try:

        async def lookup() -> Optional[list[CollectorFileEntry]]:
            async with _connect_cache_db(db_path) as db:
                # Check if the index is in cache
                async with db.execute(
                    "SELECT downloaded_at FROM index_cache WHERE url = ?", (url,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if not row:
                        LOG.debug(f"No cache entry found for {url}")
                        return None

                    downloaded_at = row[0]
                    downloaded_at_str = datetime.datetime.fromtimestamp(
                        downloaded_at, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S UTC")
                    LOG.info(
                        "Using cached index for %s (downloaded at %s)",
                        url,
                        downloaded_at_str,
                    )

                # Retrieve all file entries for this index
                async with db.execute(
                    """
                    SELECT collector_name, collector_project, collector_base_url,
                           collector_installed, collector_removed,
                           filename, file_url, file_type
                    FROM file_cache
                    WHERE index_url = ?
                    """,
                    (url,),
                ) as cursor:
                    rows = await cursor.fetchall()

                    file_entries = []
                    for row in rows:
                        (
                            collector_name,
                            collector_project,
                            collector_base_url,
                            collector_installed,
                            collector_removed,
                            filename,
                            file_url,
                            file_type,
                        ) = row

                        # Reconstruct CollectorInfo
                        collector = CollectorInfo(
                            name=collector_name,
                            project=collector_project,
                            base_url=collector_base_url,
                            installed=datetime.datetime.fromisoformat(
                                collector_installed
                            ),
                            removed=datetime.datetime.fromisoformat(collector_removed)
                            if collector_removed
                            else None,
                        )

                        # Reconstruct CollectorFileEntry
                        file_entry = CollectorFileEntry(
                            collector=collector,
                            filename=filename,
                            url=file_url,
                            file_type=file_type,
                        )
                        file_entries.append(file_entry)

                    LOG.debug(
                        f"Retrieved {len(file_entries)} file entries from cache for {url}"
                    )
                    return file_entries

        return await _retry_on_sqlite_lock(f"Look up index cache for {url}", lookup)
    except Exception as e:
        # If the database doesn't exist or there's an error, just return None
        LOG.debug(f"Cache lookup failed for {url}: {e}")
        return None


async def _store_index_once(
    url: str,
    file_entries: list[CollectorFileEntry],
    month_end_date: datetime.datetime,
    db_path: Path,
) -> None:
    await init_cache_db(db_path)

    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    month_end_str = month_end_date.isoformat()
    file_rows = [
        (
            url,
            entry.collector.name,
            entry.collector.project,
            entry.collector.base_url,
            entry.collector.installed.isoformat(),
            entry.collector.removed.isoformat() if entry.collector.removed else None,
            entry.filename,
            entry.url,
            entry.file_type,
        )
        for entry in file_entries
    ]

    async with _get_write_lock(db_path):
        async with _connect_cache_db(db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                # Store index metadata.
                await db.execute(
                    """
                    INSERT OR REPLACE INTO index_cache (url, downloaded_at, month_end_date)
                    VALUES (?, ?, ?)
                    """,
                    (url, now, month_end_str),
                )
                await db.execute("DELETE FROM file_cache WHERE index_url = ?", (url,))
                await db.executemany(
                    """
                    INSERT INTO file_cache (
                        index_url, collector_name, collector_project, collector_base_url,
                        collector_installed, collector_removed,
                        filename, file_url, file_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    file_rows,
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise


async def store_index(
    url: str,
    file_entries: list[CollectorFileEntry],
    month_end_date: datetime.datetime,
    db_path: Optional[Path] = None,
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

    try:
        await _retry_on_sqlite_lock(
            f"Store index cache for {url}",
            lambda: _store_index_once(url, file_entries, month_end_date, db_path),
        )

        LOG.debug(f"Stored {len(file_entries)} file entries in cache for {url}")
    except Exception as e:
        # Log but don't fail if caching fails
        LOG.warning(f"Failed to store index cache for {url}: {e}")


async def get_cached_indexes_batch(
    urls_with_dates: list[tuple[str, datetime.datetime]],
    force_refresh: bool = False,
    db_path: Optional[Path] = None,
) -> dict[str, list[CollectorFileEntry]]:
    """Get cached file entries for multiple indexes in a single batch operation.

    This is much more efficient than calling get_cached_index() multiple times,
    reducing from N*2 queries to just 2 queries total.

    Args:
        urls_with_dates: List of (url, month_end_date) tuples
        force_refresh: If True, ignore cache and return empty dict
        db_path: Path to the database file. If None, uses default cache path.

    Returns:
        Dict mapping URL to list of CollectorFileEntry objects for cached indexes
    """
    if force_refresh:
        LOG.debug("Force refresh enabled, skipping batch cache lookup")
        return {}

    if not urls_with_dates:
        return {}

    if db_path is None:
        db_path = get_cache_db_path()

    # Filter out URLs that need refresh based on month
    valid_urls = []
    for url, month_end_date in urls_with_dates:
        if not should_refresh_index(month_end_date):
            valid_urls.append(url)

    if not valid_urls:
        LOG.debug("No indexes eligible for caching (all need refresh)")
        return {}

    try:

        async def lookup() -> dict[str, list[CollectorFileEntry]]:
            async with _connect_cache_db(db_path) as db:
                # Build parameterized query for all URLs
                placeholders = ",".join("?" * len(valid_urls))

                # Query 1: Get index metadata for all URLs
                query = (
                    f"SELECT url, downloaded_at FROM index_cache "
                    f"WHERE url IN ({placeholders})"
                )
                cached_urls = set()
                downloaded_times = {}

                async with db.execute(query, valid_urls) as cursor:
                    rows = await cursor.fetchall()
                    for url, downloaded_at in rows:
                        cached_urls.add(url)
                        downloaded_times[url] = downloaded_at

                if not cached_urls:
                    LOG.debug(f"No cached indexes found for {len(valid_urls)} URLs")
                    return {}

                LOG.info(
                    f"Found {len(cached_urls)} cached indexes out of {len(valid_urls)} requested"
                )

                # Query 2: Get all file entries for cached URLs in one query
                placeholders = ",".join("?" * len(cached_urls))
                query = f"""
                    SELECT index_url, collector_name, collector_project, collector_base_url,
                           collector_installed, collector_removed,
                           filename, file_url, file_type
                    FROM file_cache
                    WHERE index_url IN ({placeholders})
                """

                # Group file entries by URL
                result = {url: [] for url in cached_urls}

                async with db.execute(query, list(cached_urls)) as cursor:
                    async for row in cursor:
                        (
                            index_url,
                            collector_name,
                            collector_project,
                            collector_base_url,
                            collector_installed,
                            collector_removed,
                            filename,
                            file_url,
                            file_type,
                        ) = row

                        # Reconstruct CollectorInfo
                        collector = CollectorInfo(
                            name=collector_name,
                            project=collector_project,
                            base_url=collector_base_url,
                            installed=datetime.datetime.fromisoformat(
                                collector_installed
                            ),
                            removed=datetime.datetime.fromisoformat(collector_removed)
                            if collector_removed
                            else None,
                        )

                        # Reconstruct CollectorFileEntry
                        file_entry = CollectorFileEntry(
                            collector=collector,
                            filename=filename,
                            url=file_url,
                            file_type=file_type,
                        )
                        result[index_url].append(file_entry)

                # Log summary
                total_files = sum(len(entries) for entries in result.values())
                LOG.info(
                    f"Retrieved {total_files} file entries from cache for {len(result)} indexes"
                )

                # Log individual index times
                for url in result.keys():
                    if url in downloaded_times:
                        downloaded_at_str = datetime.datetime.fromtimestamp(
                            downloaded_times[url], tz=datetime.timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S UTC")
                        LOG.debug(
                            f"Using cached index for {url} (downloaded at {downloaded_at_str})"
                        )

                return result

        return await _retry_on_sqlite_lock("Batch index cache lookup", lookup)
    except Exception as e:
        LOG.warning(f"Batch cache lookup failed: {e}")
        return {}


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
    project: str, force_refresh: bool = False, db_path: Optional[Path] = None
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
        LOG.debug(f"Force refresh enabled, skipping cache for {project} collectors")
        return None

    if db_path is None:
        db_path = get_cache_db_path()

    try:

        async def lookup() -> Optional[list[CollectorInfo]]:
            async with _connect_cache_db(db_path) as db:
                # Get all collectors for this project and check if any are stale
                async with db.execute(
                    "SELECT name, base_url, installed, removed, cached_at FROM collector_cache WHERE project = ?",
                    (project,),
                ) as cursor:
                    rows = await cursor.fetchall()

                    if not rows:
                        LOG.debug(f"No cached collectors found for {project}")
                        return None

                    # Check if cache is still fresh (any stale entry invalidates the whole cache)
                    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
                    collectors = []

                    for row in rows:
                        name, base_url, installed_str, removed_str, cached_at = row

                        # Check if this entry is stale
                        age = now - cached_at
                        if age > COLLECTOR_CACHE_REFRESH_THRESHOLD_SECONDS:
                            LOG.debug(
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
                            removed=datetime.datetime.fromisoformat(removed_str)
                            if removed_str
                            else None,
                        )
                        collectors.append(collector)

                    # Format the cached_at timestamp from the first collector for display
                    if collectors:
                        cached_at_str = datetime.datetime.fromtimestamp(
                            cached_at, tz=datetime.timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S UTC")
                        LOG.info(
                            f"Using {len(collectors)} cached collectors for {project} (cached at {cached_at_str})"
                        )
                    return collectors

        return await _retry_on_sqlite_lock(
            f"Look up collector cache for {project}", lookup
        )
    except Exception as e:
        LOG.debug(f"Collector cache lookup failed for {project}: {e}")
        return None


async def _store_collectors_once(
    project: str, collectors: list[CollectorInfo], db_path: Path
) -> None:
    await init_cache_db(db_path)

    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    collector_rows = [
        (
            project,
            collector.name,
            collector.base_url,
            collector.installed.isoformat(),
            collector.removed.isoformat() if collector.removed else None,
            now,
        )
        for collector in collectors
    ]

    async with _get_write_lock(db_path):
        async with _connect_cache_db(db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    "DELETE FROM collector_cache WHERE project = ?", (project,)
                )
                await db.executemany(
                    """
                    INSERT INTO collector_cache (project, name, base_url, installed, removed, cached_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    collector_rows,
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise


async def store_collectors(
    project: str, collectors: list[CollectorInfo], db_path: Optional[Path] = None
) -> None:
    """Store collectors in the cache.

    Args:
        project: The project name ("ris" or "routeviews")
        collectors: List of CollectorInfo objects to cache
        db_path: Path to the database file. If None, uses default cache path.
    """
    if db_path is None:
        db_path = get_cache_db_path()

    try:
        await _retry_on_sqlite_lock(
            f"Store collector cache for {project}",
            lambda: _store_collectors_once(project, collectors, db_path),
        )

        LOG.debug(f"Stored {len(collectors)} collectors in cache for {project}")
    except Exception as e:
        # Log but don't fail if caching fails
        LOG.warning(f"Failed to store collector cache for {project}: {e}")
