"""Tests for the index caching functionality."""

import datetime
import tempfile
from pathlib import Path

import pytest

from mrt_downloader.cache import (
    CACHE_REFRESH_THRESHOLD_SECONDS,
    get_cached_index,
    get_month_end_date,
    init_cache_db,
    should_refresh_index,
    store_index,
)


@pytest.mark.asyncio
async def test_init_cache_db():
    """Test that the cache database can be initialized."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)
        assert db_path.exists()


@pytest.mark.asyncio
async def test_store_and_retrieve_index():
    """Test storing and retrieving an index from the cache."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2023.01/"
        content = "<html><body>Test content</body></html>"
        # Use an old month that won't be refreshed
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)

        # Store the index
        await store_index(url, content, month_end_date, db_path)

        # Retrieve it
        cached_content = await get_cached_index(url, month_end_date, db_path)
        assert cached_content == content


@pytest.mark.asyncio
async def test_cache_miss():
    """Test that a cache miss returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2023.01/"
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)

        # Try to retrieve without storing
        cached_content = await get_cached_index(url, month_end_date, db_path)
        assert cached_content is None


@pytest.mark.asyncio
async def test_recent_month_not_cached():
    """Test that recent months are not cached."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        await init_cache_db(db_path)

        url = "https://example.com/2025.11/"
        content = "<html><body>Recent content</body></html>"
        # Use the current month (which is recent)
        now = datetime.datetime.now(datetime.timezone.utc)
        month_end_date = get_month_end_date(now.year, now.month)

        # Store the index
        await store_index(url, content, month_end_date, db_path)

        # Try to retrieve it - should return None because the month is recent
        cached_content = await get_cached_index(url, month_end_date, db_path)
        assert cached_content is None


def test_should_refresh_index_recent():
    """Test that recent months should be refreshed."""
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
        content = "<html><body>Test content</body></html>"
        month_end_date = datetime.datetime(2023, 1, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)

        # Store should auto-initialize
        await store_index(url, content, month_end_date, db_path)
        assert db_path.exists()

        # Should be able to retrieve it
        cached_content = await get_cached_index(url, month_end_date, db_path)
        assert cached_content == content
