import datetime
import json
from pathlib import Path

import pytest

from mrt_downloader.collector_index import (
    CollectorIndexEntry,
    index_files_for_collector,
    process_index_entry,
)
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


def test_parse_index_file_routeviews_ribs() -> None:
    index_entry = CollectorIndexEntry(
        None,
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2020.04/RIBS/",
        datetime.datetime(2020, 4, 1, tzinfo=datetime.UTC),
        file_types=frozenset({"rib"}),
    )
    with Path("src/tests/fixtures/route-views-bknix-bgpdata-2020.04-ribs.html").open(
        "r", encoding="utf-8"
    ) as f:
        entries = process_index_entry(index_entry, f.read())
        assert len(entries) > 28 * 12

        urls = set(x.url for x in entries)
        dates = set(x.date for x in entries)
        types = set(x.file_type for x in entries)

        assert types == {"rib"}

        assert len(entries) == len(urls) == len(dates)


def test_parse_index_file_routeviews_updates() -> None:
    index_entry = CollectorIndexEntry(
        None,
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2020.04/UPDATES/",
        datetime.datetime(2020, 4, 1, tzinfo=datetime.UTC),
        file_types=frozenset({"rib"}),
    )
    with Path("src/tests/fixtures/route-views-bknix-bgpdata-2020.04-updates.html").open(
        "r", encoding="utf-8"
    ) as f:
        entries = process_index_entry(index_entry, f.read())
        assert len(entries) > 28 * 24 * 4  # 4 updates per hour

        urls = set(x.url for x in entries)
        dates = set(x.date for x in entries)
        types = set(x.file_type for x in entries)

        assert types == {"update"}

        assert len(entries) == len(urls) == len(dates)


def test_parse_index_file_ris() -> None:
    index_entry = CollectorIndexEntry(
        None,
        "https://data.ris.ripe.net/rrc08/2020.04/",
        datetime.datetime(2020, 4, 1, tzinfo=datetime.UTC),
        file_types=frozenset({"rib", "update"}),
    )
    with Path("src/tests/fixtures/ris-rrc08-2020.04.html").open(
        "r", encoding="utf-8"
    ) as f:
        entries = process_index_entry(index_entry, f.read())
        assert len(entries) > 24 * 12 + 3  # 12 updates per hour + 3 bviews.

        urls = set(x.url for x in entries)
        dates = set(x.date for x in entries)
        types = set(x.file_type for x in entries)

        assert types == {"update", "rib"}

        assert len(entries) == len(urls)
        # all the ribs will have the same date as update
        assert len(dates) == len([e for e in entries if e.file_type == "update"])
