"""Tests for the index caching functionality."""

import asyncio
import datetime
import logging
import sqlite3
import tempfile
from pathlib import Path

import pytest

from mrt_downloader import cache
from mrt_downloader.cache import (
    get_cached_collectors,
    get_cached_index,
    get_cached_indexes_batch,
    get_month_end_date,
    init_cache_db,
    should_refresh_index,
    store_collectors,
    store_index,
)
from mrt_downloader.models import CollectorFileEntry, CollectorInfo

# A month that ended long ago, so should_refresh_index() never overrides the
# cache in tests that are checking what was stored.
OLD_MONTH_END = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC)


def make_test_collector(name: str = "RRC00") -> CollectorInfo:
    return CollectorInfo(
        name=name,
        project="ris",
        base_url=f"https://data.ris.ripe.net/{name.lower()}/",
        installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
        removed=None,
    )


def make_test_file_entries(
    index_number: int,
    collector: CollectorInfo | None = None,
) -> list[CollectorFileEntry]:
    if collector is None:
        collector = make_test_collector()

    return [
        CollectorFileEntry(
            collector=collector,
            filename=f"updates.20230115.{index_number:04}.gz",
            url=f"{collector.base_url}2023.01/updates.20230115.{index_number:04}.gz",
            file_type="update",
        )
    ]


def set_fast_sqlite_lock_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cache, "SQLITE_CONNECT_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(cache, "SQLITE_BUSY_TIMEOUT_MS", 10)
    monkeypatch.setattr(cache, "SQLITE_LOCK_RETRIES", 3)
    monkeypatch.setattr(cache, "SQLITE_LOCK_RETRY_INITIAL_DELAY_SECONDS", 0.01)


@pytest.mark.asyncio
async def test_init_cache_db():
    """Test that the cache database can be initialized."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)
        assert db_path.exists()


@pytest.mark.asyncio
async def test_init_cache_db_sets_schema_version():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        with sqlite3.connect(db_path) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]

        assert version == cache.CURRENT_CACHE_SCHEMA_VERSION


# The schema as it was before normalisation. The test owns a copy because
# cache.py deliberately no longer knows how to create it.
_LEGACY_V2_SCHEMA = """
CREATE TABLE collector_cache (
    project TEXT NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    installed TEXT NOT NULL,
    removed TEXT,
    cached_at INTEGER NOT NULL,
    PRIMARY KEY (project, name)
);
CREATE TABLE index_cache (
    url TEXT PRIMARY KEY,
    downloaded_at INTEGER NOT NULL,
    month_end_date TEXT NOT NULL
);
CREATE TABLE file_cache (
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
);
CREATE INDEX idx_file_cache_index_url ON file_cache(index_url);
"""


def _write_legacy_cache(db_path: Path, version: int) -> None:
    with sqlite3.connect(db_path) as db:
        db.executescript(_LEGACY_V2_SCHEMA)
        db.execute(
            "INSERT INTO index_cache (url, downloaded_at, month_end_date) VALUES (?, ?, ?)",
            (
                "https://data.ris.ripe.net/rrc00/2025.03/",
                1,
                "2025-03-31T23:59:59+00:00",
            ),
        )
        db.execute(
            """
            INSERT INTO file_cache (
                index_url, collector_name, collector_project, collector_base_url,
                collector_installed, collector_removed, filename, file_url, file_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "https://data.ris.ripe.net/rrc00/2025.03/",
                "RRC00",
                "ris",
                "https://data.ris.ripe.net/rrc00/",
                "1999-10-01T00:00:00+00:00",
                None,
                "updates.20250311.1850.gz",
                "https://data.ris.ripe.net/rrc00/2025.03/updates.20250311.1850.gz",
                "update",
            ),
        )
        db.execute(f"PRAGMA user_version = {version}")
        db.commit()


@pytest.mark.asyncio
async def test_cache_migration_discards_legacy_cache():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        _write_legacy_cache(db_path, version=2)

        await init_cache_db(db_path)
        # Runs before every write, so it has to be idempotent.
        await init_cache_db(db_path)

        with sqlite3.connect(db_path) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%'"
                )
            }
            indexes = db.execute("SELECT COUNT(*) FROM index_cache").fetchone()[0]
            files = db.execute("SELECT COUNT(*) FROM file_cache").fetchone()[0]
            file_columns = {
                row[1] for row in db.execute("PRAGMA table_info(file_cache)")
            }

        assert version == cache.CURRENT_CACHE_SCHEMA_VERSION
        assert tables == {"collector", "index_cache", "file_cache"}
        assert indexes == 0
        assert files == 0
        assert file_columns == {
            "index_id",
            "collector_id",
            "filename",
            "url_suffix",
            "file_type",
        }

        url = "https://data.ris.ripe.net/rrc00/2023.01/"
        await store_index(url, make_test_file_entries(0), OLD_MONTH_END, db_path)
        assert await get_cached_index(url, OLD_MONTH_END, db_path=db_path)


@pytest.mark.asyncio
async def test_unversioned_cache_is_compacted_after_rebuild():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        _write_legacy_cache(db_path, version=0)
        with sqlite3.connect(db_path) as db:
            db.execute("CREATE TABLE padding (contents BLOB)")
            db.execute("INSERT INTO padding VALUES (randomblob(1024 * 1024))")
            db.commit()

        await init_cache_db(db_path)

        with sqlite3.connect(db_path) as db:
            free_pages = db.execute("PRAGMA freelist_count").fetchone()[0]

        assert free_pages == 0


@pytest.mark.asyncio
async def test_cache_from_an_unknown_schema_is_discarded():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        _write_legacy_cache(db_path, version=99)

        await init_cache_db(db_path)

        with sqlite3.connect(db_path) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            files = db.execute("SELECT COUNT(*) FROM file_cache").fetchone()[0]

        assert version == cache.CURRENT_CACHE_SCHEMA_VERSION
        assert files == 0


@pytest.mark.asyncio
async def test_schema_has_no_secondary_indexes():
    """Prevent redundant secondary indexes from returning to the schema."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        with sqlite3.connect(db_path) as db:
            extra_indexes = db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' "
                "AND name NOT LIKE 'sqlite_autoindex_%'"
            ).fetchall()

        assert extra_indexes == []


@pytest.mark.asyncio
async def test_store_and_retrieve_index():
    """Test storing and retrieving file entries from the cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2023.01/"
        # Use an old month that won't be refreshed
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC)

        # Create test data
        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
            removed=None,
        )

        file_entries = [
            CollectorFileEntry(
                collector=collector,
                filename="updates.20230115.0000.gz",
                url="https://data.ris.ripe.net/rrc00/2023.01/updates.20230115.0000.gz",
                file_type="update",
            ),
            CollectorFileEntry(
                collector=collector,
                filename="bview.20230101.0000.gz",
                url="https://data.ris.ripe.net/rrc00/2023.01/bview.20230101.0000.gz",
                file_type="rib",
            ),
        ]

        # Store the file entries
        await store_index(url, file_entries, month_end_date, db_path)

        # Retrieve them
        cached_entries = await get_cached_index(url, month_end_date, db_path=db_path)
        assert cached_entries is not None
        assert len(cached_entries) == 2

        by_name = {entry.filename: entry for entry in cached_entries}
        assert by_name["updates.20230115.0000.gz"].file_type == "update"
        assert by_name["bview.20230101.0000.gz"].file_type == "rib"
        assert by_name["updates.20230115.0000.gz"].collector.name == "RRC00"
        assert [entry.filename for entry in cached_entries] == [
            "bview.20230101.0000.gz",
            "updates.20230115.0000.gz",
        ]

        # The URLs survive even though they are not stored verbatim.
        assert (
            by_name["updates.20230115.0000.gz"].url
            == "https://data.ris.ripe.net/rrc00/2023.01/updates.20230115.0000.gz"
        )


@pytest.mark.asyncio
async def test_cache_miss():
    """Test that a cache miss returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2023.01/"
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC)

        # Try to retrieve without storing
        cached_entries = await get_cached_index(url, month_end_date, db_path=db_path)
        assert cached_entries is None


@pytest.mark.asyncio
async def test_recent_month_not_cached():
    """Test that recent months are not retrieved from cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2025.11/"
        # Use the current month (which is recent)
        now = datetime.datetime.now(datetime.UTC)
        month_end_date = get_month_end_date(now.year, now.month)

        # Create test data
        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
            removed=None,
        )

        file_entries = [
            CollectorFileEntry(
                collector=collector,
                filename="updates.20251101.0000.gz",
                url="https://data.ris.ripe.net/rrc00/2025.11/updates.20251101.0000.gz",
                file_type="update",
            ),
        ]

        # Store the file entries
        await store_index(url, file_entries, month_end_date, db_path)

        # Try to retrieve it - should return None because the month is recent
        cached_entries = await get_cached_index(url, month_end_date, db_path=db_path)
        assert cached_entries is None


def test_should_refresh_index_current_month():
    """Test that the current month should always be refreshed."""
    now = datetime.datetime.now(datetime.UTC)
    # Get the end of the current month
    current_month_end = get_month_end_date(now.year, now.month)
    assert should_refresh_index(current_month_end) is True


def test_should_refresh_index_recent():
    """Test that recent months (ended <7 days ago) should be refreshed."""
    now = datetime.datetime.now(datetime.UTC)
    # A month that ended 3 days ago (less than 7 days)
    recent_end = now - datetime.timedelta(days=3)
    assert should_refresh_index(recent_end) is True


def test_should_refresh_index_old():
    """Test that old months should not be refreshed."""
    now = datetime.datetime.now(datetime.UTC)
    # A month that ended 30 days ago (more than 7 days)
    old_end = now - datetime.timedelta(days=30)
    assert should_refresh_index(old_end) is False


def test_get_month_end_date():
    """Test getting the last moment of a month."""
    # Test January (31 days)
    jan_end = get_month_end_date(2023, 1)
    assert jan_end.year == 2023
    assert jan_end.month == 1
    assert jan_end.day == 31
    assert jan_end.hour == 23
    assert jan_end.minute == 59
    assert jan_end.second == 59

    # Test February (28 days in 2023)
    feb_end = get_month_end_date(2023, 2)
    assert feb_end.day == 28

    # Test February (29 days in 2024 - leap year)
    feb_leap_end = get_month_end_date(2024, 2)
    assert feb_leap_end.day == 29

    # Test December (edge case - next month is next year)
    dec_end = get_month_end_date(2023, 12)
    assert dec_end.year == 2023
    assert dec_end.month == 12
    assert dec_end.day == 31


@pytest.mark.asyncio
async def test_auto_init_on_store():
    """Test that store_index auto-initializes the database."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        # Don't call init_cache_db

        url = "https://example.com/2023.01/"
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC)

        # Create test data
        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
            removed=None,
        )

        file_entries = [
            CollectorFileEntry(
                collector=collector,
                filename="updates.20230115.0000.gz",
                url="https://data.ris.ripe.net/rrc00/2023.01/updates.20230115.0000.gz",
                file_type="update",
            ),
        ]

        # Store should auto-initialize
        await store_index(url, file_entries, month_end_date, db_path)
        assert db_path.exists()

        # Should be able to retrieve it
        cached_entries = await get_cached_index(url, month_end_date, db_path=db_path)
        assert cached_entries is not None
        assert len(cached_entries) == 1
        assert cached_entries[0].filename == "updates.20230115.0000.gz"


@pytest.mark.asyncio
async def test_store_and_retrieve_collectors():
    """Test storing and retrieving collectors from the cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        project = "ris"
        collectors = [
            CollectorInfo(
                name="RRC00",
                project="ris",
                base_url="https://data.ris.ripe.net/rrc00/",
                installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
                removed=None,
            ),
            CollectorInfo(
                name="RRC01",
                project="ris",
                base_url="https://data.ris.ripe.net/rrc01/",
                installed=datetime.datetime(2001, 5, 1, tzinfo=datetime.UTC),
                removed=None,
            ),
        ]

        # Store collectors
        await store_collectors(project, collectors, db_path)

        # Retrieve them
        cached = await get_cached_collectors(project, db_path=db_path)
        assert cached is not None
        assert len(cached) == 2
        assert cached[0].name == "RRC00"
        assert cached[1].name == "RRC01"
        assert cached[0].project == "ris"


@pytest.mark.asyncio
async def test_store_and_retrieve_routeviews_activity_cutoff():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = CollectorInfo(
            name="route-views.jinx",
            project="routeviews",
            base_url="https://archive.routeviews.org/route-views.jinx/bgpdata/",
            installed=datetime.datetime(2017, 1, 1, tzinfo=datetime.UTC),
            removed=datetime.datetime(2019, 9, 15, 2, 15, tzinfo=datetime.UTC),
        )

        await store_collectors("routeviews", [collector], db_path)

        cached = await get_cached_collectors("routeviews", db_path=db_path)
        assert cached == [collector]


@pytest.mark.asyncio
async def test_collector_cache_miss():
    """Test that a collector cache miss returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        # Try to retrieve without storing
        cached = await get_cached_collectors("ris", db_path=db_path)
        assert cached is None


@pytest.mark.asyncio
async def test_collector_force_refresh():
    """Test that force_refresh bypasses collector cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        project = "ris"
        collectors = [
            CollectorInfo(
                name="RRC00",
                project="ris",
                base_url="https://data.ris.ripe.net/rrc00/",
                installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
                removed=None,
            ),
        ]

        # Store collectors
        await store_collectors(project, collectors, db_path)

        # Should get from cache normally
        cached = await get_cached_collectors(project, db_path=db_path)
        assert cached is not None

        # Should NOT get from cache with force_refresh
        cached = await get_cached_collectors(
            project, force_refresh=True, db_path=db_path
        )
        assert cached is None


@pytest.mark.asyncio
async def test_index_force_refresh():
    """Test that force_refresh bypasses index cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2023.01/"
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC)

        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
            removed=None,
        )

        file_entries = [
            CollectorFileEntry(
                collector=collector,
                filename="updates.20230115.0000.gz",
                url="https://data.ris.ripe.net/rrc00/2023.01/updates.20230115.0000.gz",
                file_type="update",
            ),
        ]

        # Store the file entries
        await store_index(url, file_entries, month_end_date, db_path)

        # Should get from cache normally
        cached = await get_cached_index(url, month_end_date, db_path=db_path)
        assert cached is not None

        # Should NOT get from cache with force_refresh
        cached = await get_cached_index(
            url, month_end_date, force_refresh=True, db_path=db_path
        )
        assert cached is None


@pytest.mark.asyncio
async def test_concurrent_store_index_calls_are_serialized():
    """Test that concurrent cache writes do not fail with database lock errors."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC)

        async def store_one(index_number: int) -> None:
            await store_index(
                f"https://example.com/2023.01/{index_number}/",
                make_test_file_entries(index_number),
                month_end_date,
                db_path,
            )

        await asyncio.gather(*(store_one(index_number) for index_number in range(20)))

        for index_number in range(20):
            cached_entries = await get_cached_index(
                f"https://example.com/2023.01/{index_number}/",
                month_end_date,
                db_path=db_path,
            )
            assert cached_entries is not None
            assert len(cached_entries) == 1
            assert cached_entries[0].filename == (
                f"updates.20230115.{index_number:04}.gz"
            )


@pytest.mark.asyncio
async def test_store_index_retries_when_database_is_temporarily_locked(monkeypatch):
    """Test that a temporary external SQLite write lock is retried."""
    set_fast_sqlite_lock_retry(monkeypatch)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        lock_conn = sqlite3.connect(db_path)
        try:
            lock_conn.execute("BEGIN IMMEDIATE")

            month_end_date = datetime.datetime(
                2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC
            )
            task = asyncio.create_task(
                store_index(
                    "https://example.com/2023.01/locked/",
                    make_test_file_entries(1),
                    month_end_date,
                    db_path,
                )
            )

            await asyncio.sleep(0.05)
            lock_conn.rollback()
            await task
        finally:
            lock_conn.close()

        cached_entries = await get_cached_index(
            "https://example.com/2023.01/locked/",
            month_end_date,
            db_path=db_path,
        )
        assert cached_entries is not None
        assert cached_entries[0].filename == "updates.20230115.0001.gz"


@pytest.mark.asyncio
async def test_store_index_does_not_raise_when_database_stays_locked(
    monkeypatch, caplog
):
    """Test that persistent cache lock failures are logged but not raised."""
    set_fast_sqlite_lock_retry(monkeypatch)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        lock_conn = sqlite3.connect(db_path)
        try:
            lock_conn.execute("BEGIN IMMEDIATE")

            caplog.set_level(logging.WARNING, logger="mrt_downloader.cache")
            await store_index(
                "https://example.com/2023.01/locked/",
                make_test_file_entries(1),
                datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.UTC),
                db_path,
            )
        finally:
            lock_conn.rollback()
            lock_conn.close()

        assert "Failed to store index cache" in caplog.text
        assert "database is locked" in caplog.text


@pytest.mark.asyncio
async def test_collector_refresh_keeps_the_file_cache():
    """A listing refresh must not cascade-delete retained collectors' files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"

        await store_index(
            url, make_test_file_entries(0, collector), OLD_MONTH_END, db_path
        )
        await store_collectors("ris", [collector], db_path)

        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert cached is not None
        assert len(cached) == 1
        assert cached[0].collector == collector


@pytest.mark.asyncio
async def test_collector_dropped_from_listing_takes_its_files_with_it():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        kept = make_test_collector("RRC00")
        dropped = make_test_collector("RRC01")

        kept_url = f"{kept.base_url}2023.01/"
        dropped_url = f"{dropped.base_url}2023.01/"
        await store_index(
            kept_url, make_test_file_entries(0, kept), OLD_MONTH_END, db_path
        )
        await store_index(
            dropped_url, make_test_file_entries(1, dropped), OLD_MONTH_END, db_path
        )
        await store_collectors("ris", [kept, dropped], db_path)

        await store_collectors("ris", [kept], db_path)

        assert await get_cached_index(kept_url, OLD_MONTH_END, db_path=db_path)
        assert await get_cached_index(dropped_url, OLD_MONTH_END, db_path=db_path) == []

        with sqlite3.connect(db_path) as db:
            names = [row[0] for row in db.execute("SELECT name FROM collector")]
        assert names == ["RRC00"]


@pytest.mark.asyncio
async def test_collector_seen_only_through_an_index_is_not_a_listing():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()

        await store_index(
            f"{collector.base_url}2023.01/",
            make_test_file_entries(0, collector),
            OLD_MONTH_END,
            db_path,
        )

        assert await get_cached_collectors("ris", db_path=db_path) is None

        with sqlite3.connect(db_path) as db:
            count = db.execute("SELECT COUNT(*) FROM collector").fetchone()[0]
        assert count == 1


@pytest.mark.asyncio
async def test_collectors_are_interned_across_indexes():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()

        for month in ("2023.01", "2023.02"):
            await store_index(
                f"{collector.base_url}{month}/",
                make_test_file_entries(0, collector),
                OLD_MONTH_END,
                db_path,
            )

        with sqlite3.connect(db_path) as db:
            count = db.execute("SELECT COUNT(*) FROM collector").fetchone()[0]
        assert count == 1


@pytest.mark.asyncio
async def test_collector_instances_are_shared_between_entries():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        urls = [f"{collector.base_url}{month}/" for month in ("2023.01", "2023.02")]

        for index, url in enumerate(urls):
            await store_index(
                url, make_test_file_entries(index, collector), OLD_MONTH_END, db_path
            )

        cached = await get_cached_indexes_batch(
            [(url, OLD_MONTH_END) for url in urls], db_path=db_path
        )

        first, second = (cached[url][0] for url in urls)
        assert first.collector is second.collector


@pytest.mark.asyncio
async def test_derivable_urls_are_not_stored():
    """Derived URLs should not add per-row storage."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"

        await store_index(
            url, make_test_file_entries(0, collector), OLD_MONTH_END, db_path
        )

        with sqlite3.connect(db_path) as db:
            suffixes = [
                row[0] for row in db.execute("SELECT url_suffix FROM file_cache")
            ]
        assert suffixes == [None]

        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert cached[0].url == f"{url}updates.20230115.0000.gz"


@pytest.mark.asyncio
async def test_subdirectory_url_round_trips():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"
        entry = CollectorFileEntry(
            collector=collector,
            filename="rib.20230101.0000.bz2",
            url=f"{url}RIBS/rib.20230101.0000.bz2",
            file_type="rib",
        )

        await store_index(url, [entry], OLD_MONTH_END, db_path)

        with sqlite3.connect(db_path) as db:
            suffix = db.execute("SELECT url_suffix FROM file_cache").fetchone()[0]
        assert suffix == "RIBS/rib.20230101.0000.bz2"

        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert cached == [entry]


@pytest.mark.asyncio
async def test_url_outside_the_index_round_trips():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = "https://example.com/2023.01/"
        entry = CollectorFileEntry(
            collector=collector,
            filename="updates.20230115.0000.gz",
            url="https://data.ris.ripe.net/rrc00/2023.01/updates.20230115.0000.gz",
            file_type="update",
        )

        await store_index(url, [entry], OLD_MONTH_END, db_path)

        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert cached == [entry]


@pytest.mark.asyncio
async def test_file_type_round_trips():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"
        entries = [
            CollectorFileEntry(
                collector=collector,
                filename=f"{name}.gz",
                url=f"{url}{name}.gz",
                file_type=file_type,
            )
            for name, file_type in (
                ("bview.20230101.0000", "rib"),
                ("updates.20230101.0000", "update"),
                ("mystery.20230101.0000", None),
            )
        ]

        await store_index(url, entries, OLD_MONTH_END, db_path)

        with sqlite3.connect(db_path) as db:
            stored = sorted(
                row[0]
                for row in db.execute("SELECT file_type FROM file_cache")
                if row[0] is not None
            )
        assert stored == [1, 2]

        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert {entry.filename: entry.file_type for entry in cached} == {
            "bview.20230101.0000.gz": "rib",
            "updates.20230101.0000.gz": "update",
            "mystery.20230101.0000.gz": None,
        }


@pytest.mark.asyncio
async def test_store_index_replaces_the_previous_listing():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"

        first = [
            entry
            for number in range(3)
            for entry in make_test_file_entries(number, collector)
        ]
        await store_index(url, first, OLD_MONTH_END, db_path)
        await store_index(
            url, make_test_file_entries(0, collector), OLD_MONTH_END, db_path
        )

        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert [entry.filename for entry in cached] == ["updates.20230115.0000.gz"]


@pytest.mark.asyncio
async def test_repeated_filenames_in_one_index_collapse():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"
        entries = make_test_file_entries(0, collector) * 2

        await store_index(url, entries, OLD_MONTH_END, db_path)

        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert len(cached) == 1


@pytest.mark.asyncio
async def test_deleting_an_index_cascades_to_its_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"
        await store_index(
            url, make_test_file_entries(0, collector), OLD_MONTH_END, db_path
        )

        with sqlite3.connect(db_path) as db:
            db.execute("PRAGMA foreign_keys = ON")
            db.execute("DELETE FROM index_cache WHERE url = ?", (url,))
            db.commit()
            remaining = db.execute("SELECT COUNT(*) FROM file_cache").fetchone()[0]

        assert remaining == 0


@pytest.mark.asyncio
async def test_batch_lookup_round_trip():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        stored = [f"{collector.base_url}{month}/" for month in ("2023.01", "2023.02")]
        missing = f"{collector.base_url}2023.03/"

        for number, url in enumerate(stored):
            await store_index(
                url, make_test_file_entries(number, collector), OLD_MONTH_END, db_path
            )

        requested = [(url, OLD_MONTH_END) for url in stored + [missing]]
        cached = await get_cached_indexes_batch(requested, db_path=db_path)

        assert set(cached) == set(stored)
        assert all(len(entries) == 1 for entries in cached.values())
        assert cached[stored[0]][0].url == f"{stored[0]}updates.20230115.0000.gz"

        assert (
            await get_cached_indexes_batch(
                requested, force_refresh=True, db_path=db_path
            )
            == {}
        )


@pytest.mark.asyncio
async def test_batch_lookup_handles_more_urls_than_one_query_allows():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        count = cache.SQL_PARAM_CHUNK * 2 + 7
        urls = [f"{collector.base_url}index-{number}/" for number in range(count)]

        for number, url in enumerate(urls):
            await store_index(
                url, make_test_file_entries(number, collector), OLD_MONTH_END, db_path
            )

        cached = await get_cached_indexes_batch(
            [(url, OLD_MONTH_END) for url in urls], db_path=db_path
        )

        assert len(cached) == count


@pytest.mark.asyncio
async def test_empty_collector_list_is_not_cached():
    """An empty response must not retire collectors and their cached files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        collector = make_test_collector()
        url = f"{collector.base_url}2023.01/"

        await store_index(
            url, make_test_file_entries(0, collector), OLD_MONTH_END, db_path
        )
        await store_collectors("ris", [collector], db_path)

        await store_collectors("ris", [], db_path)

        assert await get_cached_collectors("ris", db_path=db_path) == [collector]
        cached = await get_cached_index(url, OLD_MONTH_END, db_path=db_path)
        assert len(cached) == 1
