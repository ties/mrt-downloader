import datetime
import json
from pathlib import Path

import pytest

from mrt_downloader.collector_index import index_files_for_collector
from mrt_downloader.collectors import (
    CollectorInfo,
    parse_ripe_ris_collectors,
    parse_routeviews_collectors,
)


@pytest.fixture
def routeviews_collectors() -> list[CollectorInfo]:
    with Path("src/tests/fixtures/api-routeviews-guest-collector.json").open(
        "r", encoding="utf-8"
    ) as f:
        data = json.load(f)
        return parse_routeviews_collectors(data)


@pytest.fixture
def ris_collectors() -> list[CollectorInfo]:
    with Path("src/tests/fixtures/stat-ripe-net-data-rrc-info-data.json").open(
        "r", encoding="utf-8"
    ) as f:
        data = json.load(f)
        return parse_ripe_ris_collectors(data)


def test_index_files_for_collector_routeviews(
    routeviews_collectors: list[CollectorInfo],
) -> None:
    # Get index files for January/February 2024
    index_files = index_files_for_collector(
        routeviews_collectors[0],
        start_time=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        end_time=datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC),
    )

    # two months, RIB + UPDATE separate
    assert len(index_files) == 4
    assert len(list(filter(lambda x: "rib" in x.file_types, index_files))) == 2
    assert len(list(filter(lambda x: "update" in x.file_types, index_files))) == 2

    assert index_files[0].time_period == datetime.datetime(
        2024, 1, 1, tzinfo=datetime.UTC
    )
    assert index_files[0].collector == routeviews_collectors[0]
    assert (
        index_files[0].url
        == "https://archive.routeviews.org/route-views.flix/bgpdata/2024.01/RIBS/"
    )


def test_index_files_for_ris(ris_collectors: list[CollectorInfo]) -> None:
    # Get index files for January/February 2024
    index_files = index_files_for_collector(
        ris_collectors[0],
        start_time=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        end_time=datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC),
    )

    # Two index files, both contain ribs + updates
    assert len(index_files) == 2
    assert all(map(lambda idx: set(idx.file_types) == {"rib", "update"}, index_files))

    assert index_files[0].time_period == datetime.datetime(
        2024, 1, 1, tzinfo=datetime.UTC
    )
    assert index_files[0].collector == ris_collectors[0]
    assert index_files[0].url == "https://data.ris.ripe.net/rrc00/2024.01/"
