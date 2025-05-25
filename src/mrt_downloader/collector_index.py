import datetime
import urllib
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable, Literal

import aiohttp
import click

from mrt_downloader.collectors import CollectorInfo

BASE_URL_TEMPLATE = "https://data.ris.ripe.net/rrc{rrc:02}/{year:04}.{month:02}/"


@dataclass
class CollectorIndexEntry:
    """An entry for a file listing for a collector."""

    collector: CollectorInfo
    url: str
    time_period: datetime.datetime
    file_types: set[Literal["rib", "update"]] = frozenset()


@dataclass
class CollectorFileEntry:
    collector: CollectorInfo
    file_name: str
    url: str

    file_type: Literal["rib", "update"] | None = None

    @property
    def date(self) -> datetime.datetime | None:
        """Extract the date from the file name."""
        try:
            date_tokens = ".".join(self.file_name.split(".")[-3:-1])
            return datetime.datetime.strptime(date_tokens, "%Y%m%d.%H%M")
        except ValueError:
            return None


def round_to_five(then: datetime.datetime, up=False) -> datetime.datetime:
    """
    Round a datetime object to the nearest 5 minutes.
    """
    minutes = 5 * ((then.minute // 5) + (up and 1 or 0))
    return then.replace(minute=minutes, second=0, microsecond=0)


def index_files_for_collector(
    collector: CollectorInfo, start_time: datetime.datetime, end_time: datetime.datetime
) -> list[CollectorIndexEntry]:
    """Gather the index URLs for the given collector.

    Args:
        collector: CollectorInfo object for the collector
        start_time: Start time for gathering index files (inclusive)
        end_time: End time for gathering index files (exclusive)

    Returns:
        List of URLs to collector index files
    """
    # align start time to first day of the month at 00:00
    start_time = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    index_urls: list[CollectorIndexEntry] = []

    now = start_time
    while True:
        match collector.project:
            case "RV":
                index_urls.extend(
                    [
                        CollectorIndexEntry(
                            collector=collector,
                            url=f"{collector.base_url}{now.year:04}.{now.month:02}/RIBS/",
                            time_period=now,
                            file_types=frozenset(["rib"]),
                        ),
                        CollectorIndexEntry(
                            collector=collector,
                            url=f"{collector.base_url}{now.year:04}.{now.month:02}/UPDATES/",
                            time_period=now,
                            file_types=frozenset(["update"]),
                        ),
                    ]
                )

            case "RIS":
                index_urls.append(
                    CollectorIndexEntry(
                        collector=collector,
                        url=f"{collector.base_url}{now.year:04}.{now.month:02}/",
                        time_period=now,
                        file_types=frozenset(["rib", "update"]),
                    )
                )

        now = (now + datetime.timedelta(days=32)).replace(day=1)

        if now > end_time:
            break

    return index_urls


def index_files_for_rrcs(
    rrcs: Iterable[int], start_time: datetime.datetime, end_time: datetime.datetime
) -> list[str]:
    """Gather the index URLs for the RRCs given.

    Args:
        rrcs: List of RRC collector IDs
        start_time: Start time for gathering index files (inclusive)
        end_time: End time for gathering index files (exclusive)

    Returns:
        List of URLs to RRC index files
    """
    # align to first day of the month at 00:00
    start_time = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    index_urls = []

    for rrc in rrcs:
        now = start_time
        while True:
            index_url = BASE_URL_TEMPLATE.format(
                rrc=rrc, year=now.year, month=now.month
            )
            index_urls.append(
                CollectorIndexEntry(collector=rrc, url=index_url, time_period=now)
            )
            del index_url
            now = (now + datetime.timedelta(days=32)).replace(day=1)

            if now > end_time:
                break

    return index_urls


async def process_rrc_index(
    session: aiohttp.ClientSession, entry: CollectorIndexEntry
) -> list[CollectorFileEntry]:
    """Download the relevant indices for the given RRC and yield the updates in the interval."""
    result = []
    async with session.get(entry.url) as response:
        if response.status != 200:
            click.echo(f"Skipping {entry.url} due to HTTP error {response.status}")
        else:
            parser = AnchorTagParser()
            parser.feed(await response.text())

            for link in parser.links:
                file_name = link.split("/")[-1]
                if file_name.startswith("updates.") or file_name.startswith("bview."):
                    result.append(
                        CollectorFileEntry(
                            entry.collector,
                            file_name,
                            urllib.parse.urljoin(entry.url, link),
                        )
                    )

    return result


class AnchorTagParser(HTMLParser):
    """Parse out the A tags"""

    extension: str
    links: list[str]

    def __init__(self, extension: str = ".gz"):
        super().__init__()
        self.extension = extension
        self.links = []

    def handle_starttag(self, tag, attrs):
        """Handle open tags."""
        if tag == "a":
            for attr in attrs:
                if attr[0] == "href" and attr[1].endswith(self.extension):
                    self.links.append(attr[1])
