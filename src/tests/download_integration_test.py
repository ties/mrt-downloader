import datetime
import multiprocessing
import pathlib

import pytest  # type: ignore

from mrt_downloader.download import download_files
from mrt_downloader.files import PrefixCollectorStrategy


@pytest.mark.asyncio
async def test_mrt_download(tmp_path: pathlib.Path) -> None:
    # Download a limited number of files
    yesterday_midnight = (datetime.datetime.now() - datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.UTC
    )
    yesterday_one_am = yesterday_midnight.replace(hour=1)

    print(f"Download window: {yesterday_midnight} - {yesterday_one_am}")

    # download from two RRCs..
    await download_files(
        tmp_path,
        yesterday_midnight,
        yesterday_one_am,
        collectors=["rrc04", "rrc11"],
        num_workers=multiprocessing.cpu_count(),
        naming_strategy=PrefixCollectorStrategy(),
    )

    files = list(tmp_path.iterdir())
    # there should be 2x13 (updates - beginning/end are inclusive) + 2 files (ribs)
    # just the session
    assert len(files) > 24
    print(list(tmp_path.glob("*update*")))

    update_files = set(p.name for p in tmp_path.glob("*updates*"))
    bview_files = set(p.name for p in tmp_path.glob("*bview*"))

    assert len(bview_files) == 2
    assert len(update_files) == 26
