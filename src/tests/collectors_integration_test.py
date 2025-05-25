from datetime import UTC, datetime

import aiohttp
import pytest
import pytest_asyncio

from mrt_downloader.collectors import (
    get_ripe_ris_collectors,
    get_routeviews_collectors,
)
from mrt_downloader.http import build_session
from mrt_downloader.models import CollectorInfo


@pytest_asyncio.fixture
async def session() -> aiohttp.ClientSession:
    """Create a test aiohttp client session."""
    return build_session()


@pytest.mark.asyncio
async def test_get_ripe_ris_collectors(session: aiohttp.ClientSession):
    """Get the collectors and do some sanity checks."""
    async with session as sess:
        collectors = await get_ripe_ris_collectors(sess)

        assert len(collectors) > 10
        rrc00: CollectorInfo = next((c for c in collectors if c.name == "RRC00"), None)

        assert rrc00.name == "RRC00"
        assert rrc00.project == "RIS"
        assert rrc00.base_url == "https://data.ris.ripe.net/rrc00/"
        assert rrc00.installed == datetime(1999, 10, 1, tzinfo=UTC)
        assert rrc00.removed is None

        # Now get a deactivated collector
        deactivated_rrc: CollectorInfo = next(
            (c for c in collectors if c.name == "RRC02"), None
        )
        assert deactivated_rrc.removed == datetime(2008, 11, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_get_routeviews_collectors(session: aiohttp.ClientSession):
    """Get the RouteViews collectors and do some sanity checks."""
    async with session as sess:
        collectors = await get_routeviews_collectors(sess)

        assert len(collectors) >= 50
        routeviews8: CollectorInfo = [
            c for c in collectors if c.name == "route-views8"
        ][0]

        assert routeviews8.name == "route-views8"
        assert routeviews8.project == "RV"
        assert (
            routeviews8.base_url
            == "https://archive.routeviews.org/route-views8/bgpdata/"
        )
        assert routeviews8.installed == datetime(2025, 3, 11, 12, tzinfo=UTC)
