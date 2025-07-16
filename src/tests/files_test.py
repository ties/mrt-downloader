from pathlib import Path

from mrt_downloader.files import (
    ByCollectorPartitionedStategy,
    ByMonthStrategy,
    PrefixCollectorByHourStrategy,
    PrefixCollectorStrategy,
    split_on_dash_except_route_views,
)


def test_split_on_dash_except_route_views() -> None:
    """Test the split_on_dash_except_route_views function."""
    assert split_on_dash_except_route_views(
        "route-views.chicago-updates.20250714.2345.bz2"
    ) == ("route-views.chicago", "updates.20250714.2345.bz2")
    assert split_on_dash_except_route_views(
        "route-views8-updates.20250613.2245.bz2"
    ) == ("route-views8", "updates.20250613.2245.bz2")

    assert split_on_dash_except_route_views("foo-bla.baz") == ("foo", "bla.baz")


def test_collector_partitioned_yearmonth() -> None:
    """Test parsing paths prefixed by collector, followed by yearmonth, then files in dir"""
    chicago = [
        Path("route-views.chicago"),
        Path("2025.07"),
        Path("updates.20250714.2345.bz2"),
    ]
    rio = [Path("route-views.rio"), Path("2025.07"), Path("updates.20250714.2345.bz2")]

    strategy = ByCollectorPartitionedStategy(ByMonthStrategy())

    assert strategy.parse(chicago) == {
        "collector": "route-views.chicago",
        "year": "2025",
        "month": "07",
        "filename": "updates.20250714.2345.bz2",
    }

    assert strategy.parse(rio) == {
        "collector": "route-views.rio",
        "year": "2025",
        "month": "07",
        "filename": "updates.20250714.2345.bz2",
    }


def test_prefix_collector() -> None:
    """Test parsing paths prefixed by collector"""
    chicago = [Path("route-views.chicago-updates.20250714.2345.bz2")]
    rio = [Path("route-views.rio-updates.20250714.2345.bz2")]

    strategy = PrefixCollectorStrategy()

    assert strategy.parse(chicago) == {
        "collector": "route-views.chicago",
        "filename": "updates.20250714.2345.bz2",
    }

    assert strategy.parse(rio) == {
        "collector": "route-views.rio",
        "filename": "updates.20250714.2345.bz2",
    }


def test_collector_by_hour() -> None:
    """Test paths that are partitioned by year-month + hour, filename prefixed by collector."""
    chicago = [
        Path("2025.07.14"),
        Path("23"),
        Path("route-views.chicago-updates.20250714.2345.bz2"),
    ]
    rio = [
        Path("2025.06.13"),
        Path("22"),
        Path("route-views.rio-updates.20250613.2245.bz2"),
    ]

    strategy = PrefixCollectorByHourStrategy()

    assert strategy.parse(chicago) == {
        "collector": "route-views.chicago",
        "year": "2025",
        "month": "07",
        "day": "14",
        "hour": "23",
        "filename": "updates.20250714.2345.bz2",
    }

    assert strategy.parse(rio) == {
        "collector": "route-views.rio",
        "year": "2025",
        "month": "06",
        "day": "13",
        "hour": "22",
        "filename": "updates.20250613.2245.bz2",
    }
