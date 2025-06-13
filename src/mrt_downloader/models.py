import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


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
        date_tokens = ".".join(self.filename.split(".")[-3:-1])
        return datetime.datetime.strptime(date_tokens, "%Y%m%d.%H%M").replace(
            tzinfo=datetime.UTC
        )


@dataclass
class Download:
    url: str
    target_file: Path
