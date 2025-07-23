import datetime
import logging
import os
import urllib
import urllib.parse
from html.parser import HTMLParser
from typing import Iterable

import aiohttp
import click
from typing_extensions import deprecated

from mrt_downloader.models import CollectorFileEntry, CollectorIndexEntry, CollectorInfo

LOG = logging.getLogger(__name__)

# This constant will be removed on or after 2025-11-01
BASE_URL_TEMPLATE = "https://data.ris.ripe.net/rrc{rrc:02}/{year:04}.{month:02}/"


@deprecated(
    "This method will be removed on or after 2025-11-01. The method is no longer used, because files are now taken from the index."
)
def round_to_five(then: datetime.datetime, up: bool = False) -> datetime.datetime:
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
        if now >= collector.installed and (
            not collector.removed or now <= collector.removed
        ):
            match collector.project:
                case "routeviews":
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

                case "ris":
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


def process_index_entry(
    index: CollectorIndexEntry, html: str
) -> list[CollectorFileEntry]:
    """Extract the relevant files from the collector index entry."""
    result: list[CollectorFileEntry] = []
    parser = AnchorTagParser()
    parser.feed(html)

    for link in parser.links:
        # build the full url
        url = urllib.parse.urljoin(index.url, link)
        # skip links out of the base directory
        if not url.startswith(index.url):
            LOG.info(
                "Skipping link %s as it is not in the base directory %s",
                link,
                index.url,
            )
            continue

        path = urllib.parse.urlparse(url).path
        filename = os.path.basename(path)

        match filename:
            case f if f.startswith("bview.") or f.startswith("rib."):
                file_type = "rib"
            case f if f.startswith("updates."):
                file_type = "update"
            case _:
                LOG.warning("Unknown file type for %s, skipping", filename)
                continue

        result.append(
            CollectorFileEntry(
                index.collector,
                filename,
                url,
                file_type,
            )
        )

    return result


@deprecated("This method will be removed on or after 2025-11-01.")
def index_files_for_rrcs(
    rrcs: Iterable[int], start_time: datetime.datetime, end_time: datetime.datetime
) -> list[CollectorIndexEntry]:
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
    index_urls: list[CollectorIndexEntry] = []

    for rrc in rrcs:
        now = start_time
        while True:
            index_url = BASE_URL_TEMPLATE.format(
                rrc=rrc, year=now.year, month=now.month
            )
            # FIXME: This should be a property Collector.
            index_urls.append(
                CollectorIndexEntry(collector=rrc, url=index_url, time_period=now)
            )
            del index_url
            now = (now + datetime.timedelta(days=32)).replace(day=1)

            if now > end_time:
                break

    return index_urls


@deprecated("This method will be removed on or after 2025-11-01.")
async def process_rrc_index(
    session: aiohttp.ClientSession, entry: CollectorIndexEntry
) -> list[CollectorFileEntry]:
    """Download the relevant indices for the given RRC and yield the updates in the interval."""
    result: list[CollectorFileEntry] = []
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

    extensions: frozenset[str]
    links: list[str]

    def __init__(self, extensions: Iterable[str] = frozenset(["gz", "bz2"])):
        super().__init__()
        self.extensions = frozenset(extensions)
        self.links = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Handle open tags."""
        if tag == "a":
            for attr in attrs:
                attr, value = attr
                if not value:
                    continue

                if attr == "href" and value.split(".")[-1] in self.extensions:
                    self.links.append(value)
                else:
                    LOG.debug(
                        "Skipping link %s (extension not on allowlist)",
                        value,
                    )
