import asyncio
import datetime
import pathlib

import pytest

from mrt_downloader.files import ByCollectorNamingStrategy
from mrt_downloader.http import DownloadWorker, build_session
from mrt_downloader.models import CollectorFileEntry, CollectorInfo

BKNIX = CollectorInfo(
    name="route-views.bknix",
    project="RV",
    base_url="https://archive.routeviews.org/route-views.bknix/bgpdata/",
    installed=datetime.datetime(2019, 10, 29, 0, 0, tzinfo=datetime.timezone.utc),
    removed=None,
)

DOWNLOADS = [
    CollectorFileEntry(
        collector=BKNIX,
        filename="updates.20250401.0015.bz2",
        url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0015.bz2",
        file_type="update",
    ),
    CollectorFileEntry(
        collector=BKNIX,
        filename="updates.20250401.0000.bz2",
        url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        file_type="update",
    ),
]


@pytest.mark.asyncio
async def test_download_worker(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with build_session() as session:
        naming_strategy = ByCollectorNamingStrategy()
        queue = asyncio.Queue()
        worker = DownloadWorker(tmp_path, naming_strategy, session, queue)

        queue.put_nowait(DOWNLOADS[0])
        queue.put_nowait(DOWNLOADS[1])

        # Run the worker
        downloaded = await worker.run()
        assert downloaded == 2

        # Check if files were downloaded
        for entry in DOWNLOADS:
            file_path = naming_strategy.get_path(tmp_path, entry)
            assert file_path.exists(), f"File {entry.filename} was not downloaded."
            assert file_path.is_file(), f"{file_path} is not a file."
            assert file_path.stat().st_size > 0, f"{file_path} is empty."
