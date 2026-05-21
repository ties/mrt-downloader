import datetime
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

MRT_FILENAME_PATTERN = re.compile(
    r"^(?:bview|view|updates|rib)\.(\d{8}\.\d{4})\.(?:gz|bz2)$"
)


def parse_mrt_filename_date(filename: str) -> datetime.datetime | None:
    match = MRT_FILENAME_PATTERN.match(filename)
    if not match:
        return None

    try:
        return datetime.datetime.strptime(match.group(1), "%Y%m%d.%H%M").replace(
            tzinfo=datetime.UTC
        )
    except ValueError:
        return None


@dataclass
class CollectorInfo:
    name: str
    project: Literal["ris", "routeviews"]
    base_url: str
    installed: datetime.datetime
    removed: datetime.datetime | None = None


@dataclass
class CollectorIndexEntry:
    """An entry for a file listing for a collector."""

    collector: CollectorInfo
    url: str
    time_period: datetime.datetime
    file_types: frozenset[Literal["rib", "update"]] = frozenset()


@dataclass
class CollectorFileEntry:
    collector: CollectorInfo
    filename: str
    url: str

    file_type: Literal["rib", "update"] | None = None

    @property
    def date(self) -> datetime.datetime:
        """
        Extract the date from the file name.

        @raise ValueError if the date cannot be parsed
        """
        date = parse_mrt_filename_date(self.filename)
        if date is None:
            raise ValueError(
                f"Could not parse MRT filename date from {self.filename!r}"
            )
        return date


@dataclass
class Download:
    url: str
    target_file: Path
