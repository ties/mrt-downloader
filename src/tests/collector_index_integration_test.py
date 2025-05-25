import asyncio
import datetime
from typing import Counter

import pytest

from mrt_downloader.collector_index import CollectorIndexEntry
from mrt_downloader.collectors import CollectorInfo
from mrt_downloader.http import IndexWorker, build_session
from tests.collector_index_test import (  # noqa: F401
    ris_collectors,
    routeviews_collectors,
)


@pytest.mark.asyncio
async def test_get_file_entries_ris(ris_collectors: list[CollectorInfo]):  # noqa: F811
    async with build_session() as sess:
        index_queue = asyncio.Queue()

        worker = IndexWorker(sess, index_queue)
        index_queue.put_nowait(
            CollectorIndexEntry(
                [r for r in ris_collectors if r.name == "RRC00"][0],
                "https://data.ris.ripe.net/rrc00/2025.04/",
                datetime.datetime(2025, 4, 1, tzinfo=datetime.UTC),
                file_types=frozenset({"rib", "update"}),
            )
        )
        index_queue.put_nowait(
            CollectorIndexEntry(
                [r for r in ris_collectors if r.name == "RRC25"][0],
                "https://data.ris.ripe.net/rrc25/2025.04/",
                datetime.datetime(2025, 4, 1, tzinfo=datetime.UTC),
                file_types=frozenset({"rib", "update"}),
            )
        )

        status = await asyncio.gather(
            worker.run(),
            worker.run(),
        )
        assert sum(status) == 2

        # We have indices for two collectors, each with ribs and updates
        assert len(worker.results) > 2 * 28 * 24 * 12

        # All urls are unique
        unique_urls = set(x.url for x in worker.results)
        assert len(unique_urls) == len(worker.results)
        # dates are slightly below 0.5x the number of unique entries, since they overlap
        # between collectors. And that ribs overlap with updates.
        unique_dates = set(x.date for x in worker.results)
        assert 0.4 * len(worker.results) < len(unique_dates) < 0.5 * len(worker.results)

        # We have both typs
        type_count = Counter(x.file_type for x in worker.results)
        assert type_count["rib"] > 2 * 28 * 3
        assert type_count["update"] > 2 * 28 * 24 * 12
