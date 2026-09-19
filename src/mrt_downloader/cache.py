"""Index caching using SQLite to avoid re-downloading completed month indexes."""

import asyncio
import datetime
import logging
import sqlite3
import urllib.parse
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, TypeVar

import aiosqlite

from mrt_downloader.models import CollectorFileEntry, CollectorInfo
from mrt_downloader.url_utils import is_absolute_http_url

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

# Version 6 supersedes all schema versions previously used in releases or branches.
CURRENT_CACHE_SCHEMA_VERSION = 6

# Multi-year, all-collector runs can contain thousands of index URLs, so keep
# each IN clause well below SQLITE_MAX_VARIABLE_NUMBER.
SQL_PARAM_CHUNK = 500

_CACHE_WRITE_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}

_FILE_TYPE_TO_CODE: dict[str, int] = {"rib": 1, "update": 2}
_FILE_TYPE_BY_CODE: dict[int, str] = {
    code: name for name, code in _FILE_TYPE_TO_CODE.items()
}

_SCHEMA_DDL: tuple[str, ...] = (
    # cached_at/list_position are set only for collectors in the most recent
    # project listing; collectors known only through an index leave them NULL.
    """
    CREATE TABLE collector (
        id            INTEGER PRIMARY KEY,
        project       TEXT NOT NULL,
        name          TEXT NOT NULL,
        base_url      TEXT NOT NULL,
        installed     TEXT NOT NULL,
        removed       TEXT,
        cached_at     INTEGER,
        list_position INTEGER,
        UNIQUE (project, name)
    )
    """,
    """
    CREATE TABLE index_cache (
        id             INTEGER PRIMARY KEY,
        url            TEXT NOT NULL UNIQUE,
        downloaded_at  INTEGER NOT NULL,
        month_end_date TEXT NOT NULL
    )
    """,
    # Store only the part of the URL that cannot be derived from the index URL.
    # The composite primary key is also the only lookup index, so WITHOUT ROWID
    # avoids both a per-row rowid and a redundant secondary index.
    # collector_id is not indexed: collector deletion is rare, while an index
    # would add steady-state storage for every file row.
    """
    CREATE TABLE file_cache (
        index_id     INTEGER NOT NULL REFERENCES index_cache(id) ON DELETE CASCADE,
        collector_id INTEGER NOT NULL REFERENCES collector(id) ON DELETE CASCADE,
        filename     TEXT NOT NULL,
        url_suffix   TEXT,
        file_type    INTEGER,
        PRIMARY KEY (index_id, filename)
    ) WITHOUT ROWID
    """,
)


async def _drop_all_objects(db) -> bool:
    """Empty the database of everything this project may have put there.

    Drop every user table and view rather than relying on a version-specific list.

    PRAGMA foreign_keys cannot be changed inside a transaction, so defer the
    checks instead. By commit time nothing is left to reference anything.
    """
    await db.execute("PRAGMA defer_foreign_keys = ON")
    async with db.execute(
        """
        SELECT type, name FROM sqlite_master
        WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
        """
    ) as cursor:
        objects = await cursor.fetchall()

    for object_type, name in objects:
        await db.execute(f'DROP {object_type.upper()} IF EXISTS "{name}"')

    return bool(objects)


def _chunked(values: Sequence[T], size: int = SQL_PARAM_CHUNK) -> Iterator[Sequence[T]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _encode_file_type(file_type: Optional[str]) -> Optional[int]:
    if file_type is None:
        return None

    code = _FILE_TYPE_TO_CODE.get(file_type)
    if code is None:
        LOG.warning("Unknown file type %r, caching it as unknown", file_type)
    return code


def _url_suffix(index_url: str, entry: CollectorFileEntry) -> Optional[str]:
    """Reduce a file URL to the part that cannot be derived from the index URL.

    A derived URL needs no suffix. Subdirectory links retain their relative tail,
    while URLs outside the index are stored in full.
    """
    if entry.url.startswith(index_url):
        tail = entry.url[len(index_url) :]
        return None if tail == entry.filename else tail

    if is_absolute_http_url(entry.url):
        return entry.url

    return urllib.parse.urljoin(index_url, entry.url)


def _file_url(index_url: str, filename: str, url_suffix: Optional[str]) -> str:
    if url_suffix is None:
        return index_url + filename

    if is_absolute_http_url(url_suffix):
        return url_suffix

    return index_url + url_suffix


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
        # synchronous is connection-local; NORMAL is sufficient for a cache
        # that can be rebuilt from its upstream sources.
        await db.execute("PRAGMA synchronous = NORMAL")
        yield db


async def _read_user_version(db) -> int:
    async with db.execute("PRAGMA user_version") as cursor:
        row = await cursor.fetchone()
        return int(row[0]) if row else 0


async def init_cache_db(db_path: Optional[Path] = None) -> None:
    """Make sure the cache database holds the current schema.

    Rebuild mismatched schemas rather than migrating cached data. The
    already-current case requires only a version read.

    Args:
        db_path: Path to the database file. If None, uses default cache path.
    """
    if db_path is None:
        db_path = get_cache_db_path()

    # Safe outside the lock because the version is rechecked in the transaction.
    try:
        async with _connect_cache_db(db_path) as db:
            if await _read_user_version(db) == CURRENT_CACHE_SCHEMA_VERSION:
                return
    except sqlite3.DatabaseError as exc:
        LOG.debug("Could not read the cache schema version: %s", exc)

    async def initialize() -> None:
        async with _connect_cache_db(db_path) as db:
            await db.execute("PRAGMA journal_mode = WAL")

            await db.execute("BEGIN IMMEDIATE")
            try:
                # Re-read inside the transaction: the asyncio write lock is
                # per-process, so another process may have rebuilt the database
                # between the fast-path read and here.
                cache_version = await _read_user_version(db)
                if cache_version == CURRENT_CACHE_SCHEMA_VERSION:
                    await db.rollback()
                    return

                had_objects = await _drop_all_objects(db)
                discarded = cache_version != 0 or had_objects
                if discarded:
                    LOG.info(
                        "Cache schema is version %d rather than %d; discarding the "
                        "cached indexes and starting from an empty cache",
                        cache_version,
                        CURRENT_CACHE_SCHEMA_VERSION,
                    )

                for ddl in _SCHEMA_DDL:
                    await db.execute(ddl)

                await db.execute(
                    f"PRAGMA user_version = {CURRENT_CACHE_SCHEMA_VERSION}"
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

            if discarded:
                # Dropping the tables only moves their pages onto the freelist;
                # without this the file keeps its old size. Best-effort: a large
                # file is a much smaller problem than a failed run.
                try:
                    await db.execute("VACUUM")
                except Exception as exc:
                    LOG.debug("Could not compact the cache database: %s", exc)

    async with _get_write_lock(db_path):
        await _retry_on_sqlite_lock("Initialize cache database", initialize)

    LOG.debug("Using cache database at %s", db_path)


async def _load_collectors(db) -> dict[int, CollectorInfo]:
    """Load collectors once so file entries can share their instances."""
    collectors: dict[int, CollectorInfo] = {}
    async with db.execute(
        "SELECT id, project, name, base_url, installed, removed FROM collector"
    ) as cursor:
        async for (
            collector_id,
            project,
            name,
            base_url,
            installed,
            removed,
        ) in cursor:
            collectors[collector_id] = CollectorInfo(
                name=name,
                project=project,
                base_url=base_url,
                installed=datetime.datetime.fromisoformat(installed),
                removed=datetime.datetime.fromisoformat(removed) if removed else None,
            )
    return collectors


async def _intern_collector(db, collector: CollectorInfo) -> int:
    """Return the id of a collector row, inserting it when it is new.

    Leaves cached_at/list_position alone: membership of a project listing is
    store_collectors' business, and a collector that was only ever seen through
    an index must not be served as a cached listing.
    """
    async with db.execute(
        """
        INSERT INTO collector (project, name, base_url, installed, removed)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(project, name) DO UPDATE SET
            base_url  = excluded.base_url,
            installed = excluded.installed,
            removed   = excluded.removed
        RETURNING id
        """,
        (
            collector.project,
            collector.name,
            collector.base_url,
            collector.installed.isoformat(),
            collector.removed.isoformat() if collector.removed else None,
        ),
    ) as cursor:
        return (await cursor.fetchone())[0]


async def _upsert_index(db, url: str, downloaded_at: int, month_end: str) -> int:
    """Return the id of an index row, inserting or refreshing it as needed.

    Deliberately an upsert rather than INSERT OR REPLACE: a replace deletes the
    conflicting row, which would cascade the file rows away and hand out a new id.
    """
    async with db.execute(
        """
        INSERT INTO index_cache (url, downloaded_at, month_end_date)
        VALUES (?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            downloaded_at  = excluded.downloaded_at,
            month_end_date = excluded.month_end_date
        RETURNING id
        """,
        (url, downloaded_at, month_end),
    ) as cursor:
        return (await cursor.fetchone())[0]


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
                    "SELECT id, downloaded_at FROM index_cache WHERE url = ?", (url,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if not row:
                        LOG.debug(f"No cache entry found for {url}")
                        return None

                    index_id, downloaded_at = row
                    downloaded_at_str = datetime.datetime.fromtimestamp(
                        downloaded_at, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S UTC")
                    LOG.info(
                        "Using cached index for %s (downloaded at %s)",
                        url,
                        downloaded_at_str,
                    )

                collectors = await _load_collectors(db)

                # Retrieve all file entries for this index
                async with db.execute(
                    """
                    SELECT collector_id, filename, url_suffix, file_type
                    FROM file_cache
                    WHERE index_id = ?
                    """,
                    (index_id,),
                ) as cursor:
                    file_entries = [
                        CollectorFileEntry(
                            collector=collectors[collector_id],
                            filename=filename,
                            url=_file_url(url, filename, url_suffix),
                            file_type=_FILE_TYPE_BY_CODE.get(file_type),
                        )
                        async for (
                            collector_id,
                            filename,
                            url_suffix,
                            file_type,
                        ) in cursor
                    ]

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

    # CollectorInfo is unhashable, so deduplicate by its database identity.
    collectors: dict[tuple[str, str], CollectorInfo] = {}
    for entry in file_entries:
        collectors.setdefault(
            (entry.collector.project, entry.collector.name), entry.collector
        )

    async with _get_write_lock(db_path):
        async with _connect_cache_db(db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                collector_ids = {
                    key: await _intern_collector(db, collector)
                    for key, collector in collectors.items()
                }
                index_id = await _upsert_index(db, url, now, month_end_str)

                await db.execute(
                    "DELETE FROM file_cache WHERE index_id = ?", (index_id,)
                )
                # INSERT OR REPLACE so a listing that repeats a link collapses it
                # rather than failing the whole transaction on the primary key.
                await db.executemany(
                    """
                    INSERT OR REPLACE INTO file_cache (
                        index_id, collector_id, filename, url_suffix, file_type
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            index_id,
                            collector_ids[
                                (entry.collector.project, entry.collector.name)
                            ],
                            entry.filename,
                            _url_suffix(url, entry),
                            _encode_file_type(entry.file_type),
                        )
                        for entry in file_entries
                    ],
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
                # Query 1: Get index metadata for all URLs
                index_urls: dict[int, str] = {}
                downloaded_times: dict[str, int] = {}

                for chunk in _chunked(valid_urls):
                    placeholders = ",".join("?" * len(chunk))
                    async with db.execute(
                        f"SELECT id, url, downloaded_at FROM index_cache "
                        f"WHERE url IN ({placeholders})",
                        chunk,
                    ) as cursor:
                        async for index_id, index_url, downloaded_at in cursor:
                            index_urls[index_id] = index_url
                            downloaded_times[index_url] = downloaded_at

                if not index_urls:
                    LOG.debug(f"No cached indexes found for {len(valid_urls)} URLs")
                    return {}

                LOG.info(
                    f"Found {len(index_urls)} cached indexes out of {len(valid_urls)} requested"
                )

                # Materialise collectors once for all returned file entries.
                collectors = await _load_collectors(db)

                # Group file entries by URL
                result: dict[str, list[CollectorFileEntry]] = {
                    index_url: [] for index_url in index_urls.values()
                }

                for chunk in _chunked(list(index_urls)):
                    placeholders = ",".join("?" * len(chunk))
                    async with db.execute(
                        f"""
                        SELECT index_id, collector_id, filename, url_suffix, file_type
                        FROM file_cache
                        WHERE index_id IN ({placeholders})
                        """,
                        chunk,
                    ) as cursor:
                        async for (
                            index_id,
                            collector_id,
                            filename,
                            url_suffix,
                            file_type,
                        ) in cursor:
                            index_url = index_urls[index_id]
                            result[index_url].append(
                                CollectorFileEntry(
                                    collector=collectors[collector_id],
                                    filename=filename,
                                    url=_file_url(index_url, filename, url_suffix),
                                    file_type=_FILE_TYPE_BY_CODE.get(file_type),
                                )
                            )

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
                # Exclude collectors known only through an index.
                async with db.execute(
                    """
                    SELECT name, base_url, installed, removed, cached_at
                    FROM collector
                    WHERE project = ? AND cached_at IS NOT NULL
                    ORDER BY list_position
                    """,
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
    if not collectors:
        # Treat an empty response as an upstream failure, not as every collector
        # being retired; the latter would cascade-delete their cached file entries.
        LOG.warning(
            "Not caching an empty collector list for %s; keeping the previous one",
            project,
        )
        return

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
            position,
        )
        for position, collector in enumerate(collectors)
    ]

    async with _get_write_lock(db_path):
        async with _connect_cache_db(db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                # Retire the previous listing, then re-stamp whatever is still
                # in it. Marking first rather than comparing timestamps keeps
                # this correct when two refreshes land in the same second.
                await db.execute(
                    """
                    UPDATE collector
                    SET cached_at = NULL, list_position = NULL
                    WHERE project = ?
                    """,
                    (project,),
                )
                await db.executemany(
                    """
                    INSERT INTO collector (
                        project, name, base_url, installed, removed,
                        cached_at, list_position
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(project, name) DO UPDATE SET
                        base_url      = excluded.base_url,
                        installed     = excluded.installed,
                        removed       = excluded.removed,
                        cached_at     = excluded.cached_at,
                        list_position = excluded.list_position
                    """,
                    collector_rows,
                )
                # Delete only collectors not re-stamped by the upsert; replacing
                # the whole project would cascade-delete every cached file entry.
                await db.execute(
                    "DELETE FROM collector WHERE project = ? AND cached_at IS NULL",
                    (project,),
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
