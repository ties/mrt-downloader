import logging
import pathlib
from collections.abc import Sequence
from dataclasses import dataclass

from mrt_downloader.http import FileNamingStrategy
from mrt_downloader.models import CollectorFileEntry, parse_mrt_filename_date

LOG = logging.getLogger(__name__)


@dataclass
class ParsedFilenameSegments:
    year: str | None
    month: str | None
    day: str | None
    hour: str | None
    minute: str | None


def parse_standard_filename(filename: str) -> ParsedFilenameSegments:
    year, month, day, hour, minute = None, None, None, None, None

    dt = parse_mrt_filename_date(filename)
    if dt:
        year = str(dt.year)
        day = str(dt.day)
        hour = str(dt.hour)
        minute = str(dt.minute)
    else:
        LOG.debug(f"Filename does not match expected pattern: {filename}")

    return ParsedFilenameSegments(year, month, day, hour, minute)


def split_on_dash_except_route_views(inp: str) -> tuple[str, str]:
    """
    Split a string on dash, where the dash is not after the word route.

    Ensures that cases like route-views.chicago-updates.20250714.2345.bz2 are split correctly.
    """
    if "route-" in inp:
        idx = inp.index("route-") + len("route-")
        split_idx = inp.index("-", idx + 1)
        return inp[:split_idx], inp[split_idx + 1 :]
    else:
        tokens = inp.split("-", 1)
        assert len(tokens) == 2
        return tokens[0], tokens[1]


class ByCollectorPartitionedStategy(FileNamingStrategy):
    inner_strategy: FileNamingStrategy

    def __init__(self, inner: FileNamingStrategy):
        self.inner_strategy = inner

    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        inner_base = path / entry.collector.name.lower()

        return self.inner_strategy.get_path(inner_base, entry)

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        return {
            "collector": str(path[0]),
            **self.inner_strategy.parse(path[1:]),
        }


class IdentityStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / entry.filename

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        return {
            "filename": str(path[0]),
        }


class ByCollectorStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / entry.collector.name.lower() / entry.filename

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        return {
            "collector": str(path[0]),
            "filename": str(path[1]),
        }


class ByMonthStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / entry.date.strftime("%Y.%m") / entry.filename

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        year, month = str(path[0]).split(".")
        segments = parse_standard_filename(str(path[1]))

        return {
            "year": year,
            "month": month,
            "filename": str(path[1]),
            "day": segments.day,
            "hour": segments.hour,
            "minute": segments.minute,
        }


class ByDayStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / entry.date.strftime("%Y.%m.%d") / entry.filename

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        year, month, day = str(path[0]).split(".")
        segments = parse_standard_filename(str(path[1]))

        return {
            "year": year,
            "month": month,
            "filename": str(path[1]),
            "day": day,
            "hour": segments.hour,
            "minute": segments.minute,
        }


class ByYearMonthDayStrategy(FileNamingStrategy):
    """
    Paths like

    ```
    rrc26/2026/05/05/bview.20260505.1600.gz
    rrc26/2026/05/05/bview.20260505.1700.gz
    rrc26/2026/05/05/bview.20260505.1800.gz
    rrc26/2026/05/05/bview.20260505.1900.gz
    rrc26/2026/05/05/bview.20260505.2000.gz
    rrc26/2026/05/05/bview.20260505.2100.gz
    ```
    """

    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return (
            path
            / entry.date.strftime("%Y")
            / entry.date.strftime("%m")
            / entry.date.strftime("%d")
            / entry.filename
        )

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        year = int(str(path[0]))
        month = int(str(path[1]))
        day = int(str(path[2]))
        segments = parse_standard_filename(str(path[3]))

        return {
            "year": str(year),
            "month": str(month),
            "filename": str(path[3]),
            "day": str(day),
            "hour": segments.hour,
            "minute": segments.minute,
        }


class ByYearMonthDayHourStrategy(FileNamingStrategy):
    """
    Paths like

    ```
    rrc26/2026/05/21/21/bview.20260521.0400.gz
    ```
    """

    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return (
            path
            / entry.date.strftime("%Y")
            / entry.date.strftime("%m")
            / entry.date.strftime("%d")
            / entry.date.strftime("%H")
            / entry.filename
        )

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        year = int(str(path[0]))
        month = int(str(path[1]))
        day = int(str(path[2]))
        hour = str(path[3])
        segments = parse_standard_filename(str(path[4]))

        return {
            "year": str(year),
            "month": str(month),
            "filename": str(path[4]),
            "day": str(day),
            "hour": hour,
            "minute": segments.minute,
        }


class ByHourStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return (
            path
            / entry.date.strftime("%Y.%m.%d")
            / entry.date.strftime("%H")
            / entry.filename
        )

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        year, month, day = str(path[0]).split(".")
        return {
            "year": year,
            "month": month,
            "day": day,
            "hour": str(path[1]),
            "filename": str(path[2]),
        }


class PrefixCollectorStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / f"{entry.collector.name.lower()}-{entry.filename}"

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        collector, filename = split_on_dash_except_route_views(str(path[0]))
        return {
            "collector": collector,
            "filename": filename,
        }


class PrefixCollectorByHourStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return (
            path
            / entry.date.strftime("%Y.%m.%d")
            / entry.date.strftime("%H")
            / f"{entry.collector.name.lower()}-{entry.filename}"
        )

    def parse(self, path: Sequence[str | pathlib.Path]) -> dict[str, str | None]:
        assert len(path) == 3, (
            "Expected path to have 3 components: year.month.day, hour, filename"
        )
        year, month, day = str(path[0]).split(".")

        collector, filename = split_on_dash_except_route_views(str(path[2]))
        standard_filename = str(path[2]).split("-")[-1]
        segments = parse_standard_filename(standard_filename)

        return {
            "year": year,
            "month": month,
            "day": day,
            "hour": str(path[1]),
            "minute": segments.minute,
            "collector": collector,
            "filename": filename,
        }
