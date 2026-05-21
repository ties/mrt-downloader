import asyncio
import email.utils
import logging
import os
import random
import tempfile
import time
from abc import ABC, abstractmethod
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Awaitable, Callable, Iterable, Literal, Sequence, TypeVar

import aiohttp
import click
from aiohttp import ClientTimeout

from mrt_downloader.cache import (
    get_cached_indexes_batch,
    get_month_end_date,
    store_index,
)
from mrt_downloader.collector_index import (
    process_index_entry,
)
from mrt_downloader.mirrors import file_url_alternatives
from mrt_downloader.models import CollectorFileEntry, CollectorIndexEntry, Download

LOG = logging.getLogger(__name__)

try:
    __version__ = version("mrt-downloader")
except PackageNotFoundError:
    __version__ = "development"

USER_AGENT = f"mrt-downloader/{__version__} https://github.com/ties/mrt-downloader"
MIRRORED_FILE_RETRY_CLIENT_STATUSES = frozenset((404,))

T = TypeVar("T")


def retry_client_statuses_for_urls(urls: Sequence[str]) -> frozenset[int]:
    if len(urls) > 1:
        return MIRRORED_FILE_RETRY_CLIENT_STATUSES
    return frozenset()


class RetryHelper:
    """Helper class for retrying HTTP operations with exponential backoff.

    Implements retry logic with exponential backoff for network operations:
    - Initial delay: 2 seconds
    - Backoff multiplier: 2x (2s, 4s, 8s, 16s)
    - Default max retries: 4

    Retries on network errors (timeouts, connection errors, DNS failures).
    Does not retry on HTTP 4xx errors (client errors), unless a caller marks a
    specific client status as retryable.
    """

    def __init__(
        self,
        max_retries: int = 4,
        initial_delay: float = 2.0,
        random_start: Callable[[int], int] | None = None,
    ):
        """Initialize the retry helper.

        Args:
            max_retries: Maximum number of retry attempts (default: 4)
            initial_delay: Initial delay in seconds before first retry (default: 2.0)
            random_start: Optional random starting index provider for mirror rotation
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.random_start = random_start or random.randrange

    async def execute(
        self,
        operation: Callable[[], Awaitable[T]],
        operation_name: str,
    ) -> T:
        """Execute an async operation with retry logic.

        Args:
            operation: Async callable to execute
            operation_name: Human-readable name for logging

        Returns:
            Result from the operation

        Raises:
            The last exception encountered if all retries fail
        """
        return await self.execute_with_urls(
            lambda _url: operation(),
            operation_name,
            ("",),
        )

    async def execute_with_urls(
        self,
        operation: Callable[[str], Awaitable[T]],
        operation_name: str,
        urls: Sequence[str],
        retry_client_statuses: frozenset[int] = frozenset(),
    ) -> T:
        """Execute an async URL operation with retry logic.

        If more than one URL is supplied, the first attempt starts at a random
        URL and retries rotate through the alternatives in order.
        """
        if not urls:
            raise ValueError("At least one URL is required")

        start_index = self.random_start(len(urls)) if len(urls) > 1 else 0
        last_exception = None

        for attempt in range(self.max_retries + 1):
            attempt_url = urls[(start_index + attempt) % len(urls)]
            try:
                return await operation(attempt_url)
            except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as e:
                last_exception = e

                if isinstance(e, aiohttp.ClientResponseError) and 400 <= e.status < 500:
                    if e.status not in retry_client_statuses:
                        LOG.error(f"{operation_name} failed with client error: {e}")
                        raise

                # Calculate backoff delay
                if attempt < self.max_retries:
                    delay = self.initial_delay * (2**attempt)
                    target = f" via {attempt_url}" if attempt_url else ""
                    message = (
                        f"{operation_name}{target} failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}. "
                        f"Retrying in {delay}s..."
                    )

                    # Color based on attempt number
                    # Attempt 1 (attempt == 0): no color (first failure)
                    # Attempt 2 (attempt == 1): yellow
                    # Attempt 3+ (attempt >= 2): red
                    if attempt == 0:
                        # First retry - no color
                        click.echo(f"WARNING: {message}")
                    elif attempt == 1:
                        # Second retry - yellow
                        click.echo(click.style(f"WARNING: {message}", fg="yellow"))
                    else:
                        # Third+ retry - red
                        click.echo(click.style(f"WARNING: {message}", fg="red"))

                    await asyncio.sleep(delay)
                else:
                    error_message = (
                        f"{operation_name} failed after {self.max_retries + 1} "
                        f"attempts: {e}"
                    )
                    click.echo(click.style(f"ERROR: {error_message}", fg="red"))
                    LOG.error(error_message)
            except Exception as e:
                # Don't retry on unexpected errors
                LOG.error(f"{operation_name} failed with unexpected error: {repr(e)}")
                raise

        # This should only happen if all retries failed
        raise last_exception


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
    def parse(self, path: Sequence[Path | str]) -> dict[str, str | None]:
        pass


class DownloadWorker:
    base_dir: Path
    session: aiohttp.ClientSession
    queue: asyncio.Queue[CollectorFileEntry]
    naming_strategy: FileNamingStrategy
    check_modified: bool
    retry_helper: RetryHelper

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
        self.retry_helper = RetryHelper()

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

            # check if file is modified with retry logic
            urls = file_url_alternatives(entry)

            async def check_modified(url: str):
                async with self.session.head(url) as response:
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"HTTP {response.status}",
                            headers=response.headers,
                        )
                    return response.headers.get(
                        "Content-Length", None
                    ), parse_last_modified(response)

            content_length, last_modified = await self.retry_helper.execute_with_urls(
                check_modified,
                f"HEAD {entry.url}",
                urls,
                retry_client_statuses=retry_client_statuses_for_urls(urls),
            )

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

        # Download file with retry logic
        urls = file_url_alternatives(entry)

        async def download(url: str):
            async with self.session.get(url) as response:
                LOG.debug("HTTP %d %.3fs", response.status, time.time() - t0)
                if response.status == 200:
                    # Download to temporary file first
                    tmp_file: Path | None = None
                    try:
                        with tempfile.NamedTemporaryFile(
                            dir=target_file.parent, suffix=".tmp", delete=False
                        ) as f:
                            tmp_file = Path(f.name)
                            async for data in response.content.iter_chunked(131072):
                                f.write(data)
                            f.flush()

                        tmp_file.replace(target_file)
                        tmp_file = None
                    finally:
                        if tmp_file is not None:
                            tmp_file.unlink(missing_ok=True)

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
                        url,
                        target_file,
                        time.time() - t0,
                    )
                else:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP {response.status}",
                        headers=response.headers,
                    )

        await self.retry_helper.execute_with_urls(
            download,
            f"Download {entry.url}",
            urls,
            retry_client_statuses=retry_client_statuses_for_urls(urls),
        )

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
    db_path: Path | None
    force_cache_refresh: bool
    retry_helper: RetryHelper

    def __init__(
        self,
        session: aiohttp.ClientSession,
        queue: asyncio.Queue[CollectorIndexEntry],
        file_types: Iterable[Literal["rib", "update"]] = frozenset(("rib", "update")),
        db_path: Path | None = None,
        force_cache_refresh: bool = False,
    ):
        self.session = session
        self.queue = queue
        self.results = []
        self.file_types = frozenset(file_types)
        self.db_path = db_path
        self.force_cache_refresh = force_cache_refresh
        self.retry_helper = RetryHelper()

    async def run(self) -> int:
        # Drain all entries from queue into a list for batch processing
        entries_to_process = []
        while not self.queue.empty():
            entry = await self.queue.get()
            if not entry.file_types & self.file_types:
                LOG.debug(
                    "Skipping index %s, contains %s (want: %s)",
                    entry.url,
                    entry.file_types,
                    self.file_types,
                )
                self.queue.task_done()
                continue
            entries_to_process.append(entry)

        if not entries_to_process:
            return 0

        # Build list of (url, month_end_date) for batch cache lookup
        urls_with_dates = [
            (
                entry.url,
                get_month_end_date(entry.time_period.year, entry.time_period.month),
            )
            for entry in entries_to_process
        ]

        # Batch fetch all cached indexes
        batch_cache = await get_cached_indexes_batch(
            urls_with_dates, self.force_cache_refresh, self.db_path
        )

        # Process each entry
        processed = 0
        for index_entry in entries_to_process:
            processed += 1
            try:
                # Check if this entry is in batch cache
                if index_entry.url in batch_cache:
                    # Use cached file entries
                    cached_entries = batch_cache[index_entry.url]
                    LOG.debug(
                        f"Using cached index for {index_entry.url} ({len(cached_entries)} files)"
                    )
                    self.results.extend(cached_entries)
                else:
                    # Download and parse fresh content with retry logic
                    async def download_index():
                        async with self.session.get(index_entry.url) as response:
                            if response.status != 200:
                                LOG.error(
                                    "Failed to download index %s: HTTP %d for %s",
                                    index_entry.url,
                                    response.status,
                                    index_entry.collector,
                                )
                                raise aiohttp.ClientResponseError(
                                    request_info=response.request_info,
                                    history=response.history,
                                    status=response.status,
                                    message=f"HTTP {response.status}",
                                    headers=response.headers,
                                )
                            return await response.text()

                    content = await self.retry_helper.execute(
                        download_index, f"Download index {index_entry.url}"
                    )

                    # Parse the index
                    file_entries = process_index_entry(index_entry, content)

                    # Calculate month end date for storage
                    month_end_date = get_month_end_date(
                        index_entry.time_period.year, index_entry.time_period.month
                    )

                    # Store parsed entries in cache
                    await store_index(
                        index_entry.url, file_entries, month_end_date, self.db_path
                    )

                    self.results.extend(file_entries)
            except Exception as e:
                LOG.error(e)

        # Mark all queue items as done
        for _ in entries_to_process:
            self.queue.task_done()

        return processed
