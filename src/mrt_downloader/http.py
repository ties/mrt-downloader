import asyncio
import email
import logging
import os
import time
from importlib.metadata import PackageNotFoundError, version
from typing import Iterable, Literal

import aiohttp
from aiohttp import ClientTimeout

from mrt_downloader.collector_index import (
    process_index_entry,
)
from mrt_downloader.models import CollectorFileEntry, CollectorIndexEntry, Download

LOG = logging.getLogger(__name__)

try:
    __version__ = version("mrt-downloader")
except PackageNotFoundError:
    __version__ = "development"

USER_AGENT = f"mrt-downloader/{__version__} https://github.com/ties/mrt-downloader"


def build_session() -> aiohttp.ClientSession:
    """
    Build an aiohttp client session with default settings and user-agent.
    """
    # aiohttp TCPConnector users happy eyeballs by default
    #
    # We use a low total timeout (downloads can take minutes), but relatively quick connect timeout.
    return aiohttp.ClientSession(
        timeout=ClientTimeout(total=15 * 60, sock_connect=30),
        headers={"User-Agent": USER_AGENT},
    )


async def download_file(session: aiohttp.ClientSession, download: Download) -> None:
    t0 = time.time()
    if download.target_file.is_file():
        # check if file is modified
        async with session.head(download.url) as response:
            content_length = response.headers.get("Content-Length", None)
            last_modified = response.headers.get("Last-Modified", None)
            if content_length and last_modified:
                # Stat the current file
                stat = download.target_file.stat()
                last_modified_date = email.utils.parsedate_to_datetime(last_modified)
                if (
                    stat.st_size == int(content_length)
                    and stat.st_mtime == last_modified_date.timestamp()
                ):
                    LOG.debug(
                        "Skipping %s, already downloaded",
                        download.target_file,
                    )
                    return

    async with session.get(download.url) as response:
        LOG.debug("HTTP %d %.3fs", response.status, time.time() - t0)
        if response.status == 200:
            with download.target_file.open("wb") as f:
                async for data in response.content.iter_chunked(131072):
                    f.write(data)
            # Get last modified time from the response
            last_modified = response.headers.get("Last-Modified", None)
            if last_modified:
                last_modified_date = email.utils.parsedate_to_datetime(last_modified)
                os.utime(
                    download.target_file,
                    (last_modified_date.timestamp(), last_modified_date.timestamp()),
                )

            LOG.debug(
                "Downloaded %s to %s in %.3fs",
                download.url,
                download.target_file,
                time.time() - t0,
            )
        else:
            raise ValueError(f"Got status {response.status} for {download.url}")


async def worker(session: aiohttp.ClientSession, queue: asyncio.Queue[Download]) -> int:
    processed = 0
    while not queue.empty():
        download = await queue.get()
        processed += 1
        try:
            await download_file(session, download)
        except Exception as e:
            LOG.error(e)
        finally:
            queue.task_done()

    return processed


class IndexWorker:
    session: aiohttp.ClientSession
    queue: asyncio.Queue[CollectorIndexEntry]
    results: list[CollectorFileEntry] = []
    file_types: frozenset[Literal["rib", "update"]]

    def __init__(
        self,
        session: aiohttp.ClientSession,
        queue: asyncio.Queue[CollectorIndexEntry],
        file_types: Iterable[Literal["rib", "update"]] = frozenset(("rib", "update")),
    ):
        self.session = session
        self.queue = queue
        self.results = []
        self.file_types = frozenset(file_types)

    async def run(self) -> None:
        processed = 0
        while not self.queue.empty():
            index_entry = await self.queue.get()
            if not index_entry.file_types & self.file_types:
                LOG.debug(
                    "Skipping index %s, contains %s (want: %s)",
                    index_entry.url,
                    index_entry.file_types,
                    self.file_types,
                )
                self.queue.task_done()
                continue

            processed += 1
            try:
                async with self.session.get(index_entry.url) as response:
                    if response.status != 200:
                        LOG.error(
                            "Failed to download index %s: HTTP %d",
                            index_entry.url,
                            response.status,
                        )
                        continue

                    self.results.extend(
                        process_index_entry(index_entry, await response.text())
                    )
            except Exception as e:
                LOG.error(e)
            finally:
                self.queue.task_done()
        return processed
