import logging
import pathlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from mrt_downloader.http import FileNamingStrategy
from mrt_downloader.models import CollectorFileEntry

LOG = logging.getLogger(__name__)

FILENAME_PATTERN = re.compile(r"^(?:bview|updates|rib)\.(\d{8}\.\d{4})\..*$")


@dataclass
class ParsedFilenameSegments:
    year: str | None
    month: str | None
    day: str | None
    hour: str | None
    minute: str | None


def parse_standard_filename(filename: str) -> ParsedFilenameSegments:
    year, month, day, hour, minute = None, None, None, None, None

    try:
        match = FILENAME_PATTERN.match(filename)
        if match:
            # Extract the datetime segment (e.g., '20240211.1600')
            datetime_segment = match.group(1)

            # Parse using datetime.strptime
            dt = datetime.strptime(datetime_segment, "%Y%m%d.%H%M")

            year = str(dt.year)
            day = str(dt.day)
            hour = str(dt.hour)
            minute = str(dt.minute)
    except ValueError:
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
