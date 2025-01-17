import asyncio
import datetime
import urllib
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, List, Optional

import aiohttp
import click

from mrt_downloader.http import Download, worker

BVIEW_DATE_TYPE = click.DateTime(
    formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"]
)

BASE_URL_TEMPLATE = "https://data.ris.ripe.net/rrc{rrc:02}/{year:04}.{month:02}/"


@dataclass
class RrcIndexEntry:
    rrc: int
    url: str


@dataclass
class RisFileEntry:
    rrc: int
    file_name: str
    url: str

    @property
    def date(self) -> Optional[datetime.datetime]:
        """Extract the date from the file name."""
        # import ipdb; ipdb.set_trace()
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


def index_files_for_rrcs(
    rrcs: Iterable[int], start_time: datetime.datetime, end_time: datetime.datetime
) -> List[str]:
    """Gather the index URLs for the RRCs given."""
    # align to first day of the month at 00:00
    start_time = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    index_urls = []

    for rrc in rrcs:
        now = start_time
        while True:
            index_url = BASE_URL_TEMPLATE.format(
                rrc=rrc, year=now.year, month=now.month
            )
            index_urls.append(RrcIndexEntry(rrc=rrc, url=index_url))
            del index_url
            now = (now + datetime.timedelta(days=32)).replace(day=1)

            if now > end_time:
                break

    return index_urls


async def process_rrc_index(
    session: aiohttp.ClientSession, entry: RrcIndexEntry
) -> List[RisFileEntry]:
    """Download the relevant indices for the given RRC and yield the updates in the interval."""
    result = []
    async with session.get(entry.url) as response:
        if response.status != 200:
            click.echo(f"Skipping {entry.url} due to HTTP error {response.status}")
        else:
            parser = RisArchiveLinkParser()
            parser.feed(await response.text())

            for link in parser.links:
                file_name = link.split("/")[-1]
                if file_name.startswith("updates.") or file_name.startswith("bview."):
                    result.append(
                        RisFileEntry(
                            entry.rrc, file_name, urllib.parse.urljoin(entry.url, link)
                        )
                    )

    return result


class RisArchiveLinkParser(HTMLParser):
    """Parse out the A tags"""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        """Handle open tags."""
        if tag == "a":
            for attr in attrs:
                if attr[0] == "href" and attr[1].endswith(".gz"):
                    self.links.append(attr[1])


async def download_files(
    target_dir: Path,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    num_workers: int,
    bview_only: bool = False,
    update_only: bool = False,
    rrc: Optional[List[str]] = None,
):
    """Gather the list of update files per timestamp per rrc and download them."""
    matches = []
    if rrc is not None and len(rrc) > 0:
        click.echo(
            click.style(f"Downloading updates from _only_ RRCs {rrc}", fg="green")
        )
        rrcs = [int(r) for r in rrc]
    else:
        # More RRCs than present
        rrcs = list(range(0, 28))

    index_urls = index_files_for_rrcs(rrcs, start_time, end_time)
    async with aiohttp.ClientSession() as session:
        indexes = await asyncio.gather(
            *[process_rrc_index(session, index) for index in index_urls]
        )
        for index in indexes:
            for entry in index:
                if entry.date >= start_time and entry.date <= end_time:
                    # bview only filtering
                    if bview_only and not entry.file_name.startswith("bview."):
                        continue
                    if update_only and not entry.file_name.startswith("updates."):
                        continue
                    matches.append(entry)

    queue = asyncio.Queue()

    for file in matches:
        # path name is prefixed with rrc to cluster files from the same rrc.
        await queue.put(
            Download(file.url, target_dir / f"rrc{file.rrc:02}-{file.file_name}")
        )

    click.echo(
        click.style(
            f"Downloading {len(matches)} files on {num_workers} workers", fg="green"
        )
    )

    async with aiohttp.ClientSession() as session:
        workers = [worker(session, queue) for i in range(num_workers)]

        statuses = await asyncio.gather(*workers)
        await queue.join()

        for status in statuses:
            click.echo(click.style(f"Downloaded {status} files"))
