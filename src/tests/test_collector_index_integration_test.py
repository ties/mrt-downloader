import aiohttp
import pytest
import pytest_asyncio

from mrt_downloader.collector_index import get_ripe_ris_collectors, CollectorInfo
from mrt_downloader.http import build_session

@pytest_asyncio.fixture
async def session() -> aiohttp.ClientSession:
    """Create a test aiohttp client session."""
    return build_session()


@pytest.mark.asyncio
async def test_get_ripe_ris_collectors(session: aiohttp.ClientSession):
    """Get the collectors and do some sanity checks."""
    async with session as sess:
        collectors = [collector async for collector in get_ripe_ris_collectors(sess)]

        assert len(collectors) > 10
        rrc00: CollectorInfo = next((c for c in collectors if c.collector_name == "RRC00"), None)

        assert rrc00.collector_name == "RRC00"
        assert rrc00.project == "RIPE RIS"
        assert rrc00.base_url == "https://data.ris.ripe.net/rrc00/"
        # Do not depend on exact string values.
        assert rrc00.collector_location
        assert rrc00.logical_location

        import ipdb; ipdb.set_trace()  # noqa: T201