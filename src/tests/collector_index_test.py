import datetime
import json
import logging
from pathlib import Path

import pytest

from mrt_downloader.collector_index import (
    index_files_for_collector,
    process_index_entry,
)
from mrt_downloader.collectors import (
    parse_ripe_ris_collectors,
    parse_routeviews_collectors,
)
from mrt_downloader.models import CollectorIndexEntry, CollectorInfo


@pytest.fixture
def routeviews_collectors() -> list[CollectorInfo]:
    with Path("src/tests/fixtures/api-routeviews-meta-collectors.json").open(
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


def test_parse_routeviews_collectors_uses_metadata_availability(
    routeviews_collectors: list[CollectorInfo],
) -> None:
    bknix = [c for c in routeviews_collectors if c.name == "route-views.bknix"][0]

    assert bknix.project == "routeviews"
    assert bknix.base_url == "https://archive.routeviews.org/route-views.bknix/bgpdata/"
    assert bknix.installed == datetime.datetime(
        2019, 10, 28, 23, 15, tzinfo=datetime.UTC
    )
    assert bknix.removed == datetime.datetime(2026, 6, 9, 12, 45, tzinfo=datetime.UTC)


BKNIX_COLLECTOR = CollectorInfo(
    name="route-views.bknix",
    project="routeviews",
    base_url="https://archive.routeviews.org/route-views.bknix/bgpdata/",
    installed=datetime.datetime(2019, 10, 29, 0, 0, 0, tzinfo=datetime.UTC),
    removed=None,
)

RRC08_COLLECTOR = CollectorInfo(
    name="RRC08",
    project="ris",
    base_url="https://data.ris.ripe.net/rrc08/",
    installed=datetime.datetime(2002, 5, 1, 0, 0, 0, tzinfo=datetime.UTC),
    removed=datetime.datetime(2004, 10, 1, 0, 0, 0, tzinfo=datetime.UTC),
)


def test_process_index_entry_skips_malformed_mrt_filenames(
    caplog: pytest.LogCaptureFixture,
) -> None:
    index_entry = CollectorIndexEntry(
        RRC08_COLLECTOR,
        "https://data.ris.ripe.net/rrc08/2026.05/",
        datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
        file_types=frozenset({"update"}),
    )
    html = """
    <a href="updates.20260521.1500.gz">updates.20260521.1500.gz</a>
    <a href="updates.20260521.1503.bad.gz">updates.20260521.1503.bad.gz</a>
    """

    with caplog.at_level(logging.WARNING):
        entries = process_index_entry(index_entry, html)

    assert [entry.filename for entry in entries] == ["updates.20260521.1500.gz"]
    assert entries[0].date == datetime.datetime(2026, 5, 21, 15, tzinfo=datetime.UTC)
    assert (
        "Invalid MRT filename for updates.20260521.1503.bad.gz, skipping" in caplog.text
    )


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


def test_index_files_for_routeviews_includes_first_partial_month(
    routeviews_collectors: list[CollectorInfo],
) -> None:
    routeviews8 = [c for c in routeviews_collectors if c.name == "route-views8"][0]

    index_files = index_files_for_collector(
        routeviews8,
        start_time=datetime.datetime(2025, 3, 1, tzinfo=datetime.UTC),
        end_time=datetime.datetime(2025, 3, 31, tzinfo=datetime.UTC),
    )

    assert [entry.url for entry in index_files] == [
        "https://archive.routeviews.org/route-views8/bgpdata/2025.03/RIBS/",
        "https://archive.routeviews.org/route-views8/bgpdata/2025.03/UPDATES/",
    ]


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
        BKNIX_COLLECTOR,
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
        BKNIX_COLLECTOR,
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
        RRC08_COLLECTOR,
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
