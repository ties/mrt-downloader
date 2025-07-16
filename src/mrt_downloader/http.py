import asyncio
import email.utils
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Iterable, Literal, Sequence

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


def parse_last_modified(response: aiohttp.ClientResponse) -> datetime | None:
    """
    Parse the 'Last-Modified' header from the response and return it as a datetime object.

    Documentation is unclear if parsedate_to_datetime can return None, so we explicitly
    handle this case (as well as the ValueError).

    """
    last_modified = response.headers.get("Last-Modified", None)
    if last_modified:
        try:
            return email.utils.parsedate_to_datetime(last_modified)
        except ValueError as e:
            LOG.info(f"Failed to parse Last-Modified header: {e}")
    return None


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
            last_modified = parse_last_modified(response)
            if content_length and last_modified:
                # Stat the current file
                stat = download.target_file.stat()
                if (
                    stat.st_size == int(content_length)
                    and stat.st_mtime == last_modified.timestamp()
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
            last_modified = parse_last_modified(response)
            if last_modified:
                os.utime(
                    download.target_file,
                    (last_modified.timestamp(), last_modified.timestamp()),
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


class FileNamingStrategy(ABC):
    @abstractmethod
    def get_path(self, path: Path, entry: CollectorFileEntry) -> Path:
        pass

    @abstractmethod
    def parse(self, path: Sequence[Path]) -> dict[str, str]:
        pass


class DownloadWorker:
    base_dir: Path
    session: aiohttp.ClientSession
    queue: asyncio.Queue[CollectorFileEntry]
    naming_strategy: FileNamingStrategy
    check_modified: bool

    def __init__(
        self,
        base_dir: Path,
        naming_strategy: FileNamingStrategy,
        session: aiohttp.ClientSession,
        queue: asyncio.Queue[CollectorFileEntry],
        check_modified: bool = True,
    ):
        self.base_dir = base_dir
        self.session = session
        self.queue = queue
        self.naming_strategy = naming_strategy
        self.check_modified = check_modified

    async def download_file(self, entry: CollectorFileEntry) -> None:
        target_file = self.naming_strategy.get_path(self.base_dir, entry)

        # Create target directory if it does not exist
        target_file.parent.mkdir(parents=True, exist_ok=True)

        t0 = time.time()
        if target_file.is_file():
            if not self.check_modified:
                LOG.debug(
                    "Skipping %s w/o modification check, already downloaded",
                    target_file,
                )
                return

            # check if file is modified
            async with self.session.head(entry.url) as response:
                content_length = response.headers.get("Content-Length", None)
                last_modified = parse_last_modified(response)
                if content_length and last_modified:
                    # Stat the current file
                    stat = target_file.stat()
                    if (
                        stat.st_size == int(content_length)
                        and stat.st_mtime == last_modified.timestamp()
                    ):
                        LOG.debug(
                            "Skipping %s (%db at %s), already downloaded",
                            target_file,
                            stat.st_size,
                            last_modified,
                        )
                        return

        async with self.session.get(entry.url) as response:
            LOG.debug("HTTP %d %.3fs", response.status, time.time() - t0)
            if response.status == 200:
                with target_file.open("wb") as f:
                    async for data in response.content.iter_chunked(131072):
                        f.write(data)
                # Get last modified time from the response
                last_modified = parse_last_modified(response)
                if last_modified:
                    os.utime(
                        target_file,
                        (
                            last_modified.timestamp(),
                            last_modified.timestamp(),
                        ),
                    )

                LOG.debug(
                    "Downloaded %s to %s in %.3fs",
                    entry.url,
                    target_file,
                    time.time() - t0,
                )
            else:
                raise ValueError(f"Got status {response.status} for {entry.url}")

    async def run(self) -> int:
        processed = 0
        while not self.queue.empty():
            download = await self.queue.get()
            processed += 1
            try:
                await self.download_file(download)
            except Exception as e:
                LOG.error(e)
            finally:
                self.queue.task_done()
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

    async def run(self) -> int:
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
                            "Failed to download index %s: HTTP %d for %s",
                            index_entry.url,
                            response.status,
                            index_entry.collector,
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
