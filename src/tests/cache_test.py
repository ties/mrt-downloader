"""Tests for the index caching functionality."""

import asyncio
import datetime
import logging
import sqlite3
import tempfile
from pathlib import Path

import pytest

import mrt_downloader.cache as cache
from mrt_downloader.cache import (
    get_cached_collectors,
    get_cached_index,
    get_month_end_date,
    init_cache_db,
    should_refresh_index,
    store_collectors,
    store_index,
)
from mrt_downloader.models import CollectorFileEntry, CollectorInfo


def make_test_collector(name: str = "RRC00") -> CollectorInfo:
    return CollectorInfo(
        name=name,
        project="ris",
        base_url=f"https://data.ris.ripe.net/{name.lower()}/",
        installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc),
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


@pytest.mark.asyncio
async def test_cache_migration_invalidates_routeviews_rows(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        monkeypatch.setattr(cache, "CURRENT_CACHE_SCHEMA_VERSION", 1)
        await init_cache_db(db_path)

        with sqlite3.connect(db_path) as db:
            db.executemany(
                """
                INSERT INTO collector_cache
                    (project, name, base_url, installed, removed, cached_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "routeviews",
                        "route-views8",
                        "https://archive.routeviews.org/route-views8/bgpdata/",
                        "2025-03-11T12:00:00+00:00",
                        None,
                        1,
                    ),
                    (
                        "ris",
                        "RRC00",
                        "https://data.ris.ripe.net/rrc00/",
                        "1999-10-01T00:00:00+00:00",
                        None,
                        1,
                    ),
                ],
            )
            db.executemany(
                """
                INSERT INTO index_cache (url, downloaded_at, month_end_date)
                VALUES (?, ?, ?)
                """,
                [
                    (
                        "https://archive.routeviews.org/route-views8/bgpdata/2025.03/UPDATES/",
                        1,
                        "2025-03-31T23:59:59+00:00",
                    ),
                    (
                        "https://data.ris.ripe.net/rrc00/2025.03/",
                        1,
                        "2025-03-31T23:59:59+00:00",
                    ),
                ],
            )
            db.executemany(
                """
                INSERT INTO file_cache (
                    index_url, collector_name, collector_project, collector_base_url,
                    collector_installed, collector_removed, filename, file_url, file_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "https://archive.routeviews.org/route-views8/bgpdata/2025.03/UPDATES/",
                        "route-views8",
                        "routeviews",
                        "https://archive.routeviews.org/route-views8/bgpdata/",
                        "2025-03-11T12:00:00+00:00",
                        None,
                        "updates.20250311.1852.bz2",
                        "https://archive.routeviews.org/route-views8/bgpdata/2025.03/UPDATES/updates.20250311.1852.bz2",
                        "update",
                    ),
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
                ],
            )
            db.commit()

        monkeypatch.setattr(cache, "CURRENT_CACHE_SCHEMA_VERSION", 2)
        await init_cache_db(db_path)
        await init_cache_db(db_path)

        with sqlite3.connect(db_path) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            routeviews_collectors = db.execute(
                "SELECT COUNT(*) FROM collector_cache WHERE project = 'routeviews'"
            ).fetchone()[0]
            ris_collectors = db.execute(
                "SELECT COUNT(*) FROM collector_cache WHERE project = 'ris'"
            ).fetchone()[0]
            routeviews_indexes = db.execute(
                """
                SELECT COUNT(*) FROM index_cache
                WHERE url LIKE 'https://archive.routeviews.org/%'
                   OR url LIKE 'https://archive2.routeviews.org/%'
                """
            ).fetchone()[0]
            ris_indexes = db.execute(
                "SELECT COUNT(*) FROM index_cache WHERE url LIKE 'https://data.ris.ripe.net/%'"
            ).fetchone()[0]
            routeviews_files = db.execute(
                "SELECT COUNT(*) FROM file_cache WHERE collector_project = 'routeviews'"
            ).fetchone()[0]
            ris_files = db.execute(
                "SELECT COUNT(*) FROM file_cache WHERE collector_project = 'ris'"
            ).fetchone()[0]

        assert version == 2
        assert routeviews_collectors == 0
        assert ris_collectors == 1
        assert routeviews_indexes == 0
        assert ris_indexes == 1
        assert routeviews_files == 0
        assert ris_files == 1


@pytest.mark.asyncio
async def test_store_and_retrieve_index():
    """Test storing and retrieving file entries from the cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2023.01/"
        # Use an old month that won't be refreshed
        month_end_date = datetime.datetime(
            2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
        )

        # Create test data
        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc),
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
        assert cached_entries[0].filename == "updates.20230115.0000.gz"
        assert cached_entries[0].file_type == "update"
        assert cached_entries[1].filename == "bview.20230101.0000.gz"
        assert cached_entries[1].file_type == "rib"
        assert cached_entries[0].collector.name == "RRC00"


@pytest.mark.asyncio
async def test_cache_miss():
    """Test that a cache miss returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2023.01/"
        month_end_date = datetime.datetime(
            2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
        )

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
        now = datetime.datetime.now(datetime.timezone.utc)
        month_end_date = get_month_end_date(now.year, now.month)

        # Create test data
        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc),
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
    now = datetime.datetime.now(datetime.timezone.utc)
    # Get the end of the current month
    current_month_end = get_month_end_date(now.year, now.month)
    assert should_refresh_index(current_month_end) is True


def test_should_refresh_index_recent():
    """Test that recent months (ended <7 days ago) should be refreshed."""
    now = datetime.datetime.now(datetime.timezone.utc)
    # A month that ended 3 days ago (less than 7 days)
    recent_end = now - datetime.timedelta(days=3)
    assert should_refresh_index(recent_end) is True


def test_should_refresh_index_old():
    """Test that old months should not be refreshed."""
    now = datetime.datetime.now(datetime.timezone.utc)
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
        month_end_date = datetime.datetime(
            2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
        )

        # Create test data
        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc),
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
                installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc),
                removed=None,
            ),
            CollectorInfo(
                name="RRC01",
                project="ris",
                base_url="https://data.ris.ripe.net/rrc01/",
                installed=datetime.datetime(2001, 5, 1, tzinfo=datetime.timezone.utc),
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
                installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc),
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
        month_end_date = datetime.datetime(
            2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
        )

        collector = CollectorInfo(
            name="RRC00",
            project="ris",
            base_url="https://data.ris.ripe.net/rrc00/",
            installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc),
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
        month_end_date = datetime.datetime(
            2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
        )

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
                2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
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
                datetime.datetime(
                    2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc
                ),
                db_path,
            )
        finally:
            lock_conn.rollback()
            lock_conn.close()

        assert "Failed to store index cache" in caplog.text
        assert "database is locked" in caplog.text
