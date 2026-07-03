import asyncio
import email.utils
import logging
import os
import random
import tempfile
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
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
from mrt_downloader.mirrors import (
    ArchiveRandomMirrorStrategy,
    FileMirrorStrategy,
    MirrorAttemptPlan,
    parse_duplicate_link_urls,
)
from mrt_downloader.models import CollectorFileEntry, CollectorIndexEntry, Download
from mrt_downloader.url_utils import is_absolute_http_url

LOG = logging.getLogger(__name__)

try:
    __version__ = version("mrt-downloader")
except PackageNotFoundError:
    __version__ = "development"

USER_AGENT = f"mrt-downloader/{__version__} https://github.com/ties/mrt-downloader"
DEFAULT_RETRY_CLIENT_STATUSES = frozenset((429,))

T = TypeVar("T")


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None

    try:
        delay = float(value)
    except ValueError:
        try:
            retry_at = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None

        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        delay = (retry_at - datetime.now(UTC)).total_seconds()

    return max(0.0, delay)


class RetryHelper:
    """Helper class for retrying HTTP operations with exponential backoff.

    Implements retry logic with exponential backoff for network operations:
    - Initial delay: 2 seconds
    - Backoff multiplier: 2x (2s, 4s, 8s, 16s)
    - Additive jitter: random delay from zero to the base delay
    - Default max retries: 4

    Retries on network errors (timeouts, connection errors, DNS failures).
    Retries on HTTP 429 by default. Does not retry on other HTTP 4xx errors
    (client errors), unless a caller marks a specific client status as retryable.
    """

    def __init__(
        self,
        max_retries: int = 4,
        initial_delay: float = 2.0,
        random_jitter: Callable[[float], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ):
        """Initialize the retry helper.

        Args:
            max_retries: Maximum number of retry attempts (default: 4)
            initial_delay: Initial delay in seconds before first retry (default: 2.0)
            random_jitter: Optional jitter provider for retry delay tests
            sleep: Optional async sleep function for retry delay tests
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.random_jitter = random_jitter or (lambda delay: random.uniform(0, delay))
        self.sleep = sleep or asyncio.sleep

    def _retry_delay(self, attempt: int, error: BaseException) -> float:
        base_delay = self.initial_delay * (2**attempt)
        if isinstance(error, aiohttp.ClientResponseError) and error.status == 429:
            retry_after = _parse_retry_after(
                error.headers.get("Retry-After") if error.headers else None
            )
            if retry_after is not None:
                base_delay = max(base_delay, retry_after)

        return base_delay + self.random_jitter(base_delay)

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
        retry_each_url_once: bool = False,
    ) -> T:
        """Execute an async URL operation with retry logic.

        If more than one URL is supplied, retries rotate through the alternatives
        in the supplied order unless retry_each_url_once is set.
        """
        if not urls:
            raise ValueError("At least one URL is required")

        last_exception = None
        attempt_limit = (
            min(self.max_retries + 1, len(urls))
            if retry_each_url_once
            else self.max_retries + 1
        )
        retryable_client_statuses = (
            DEFAULT_RETRY_CLIENT_STATUSES | retry_client_statuses
        )

        for attempt in range(attempt_limit):
            attempt_url = (
                urls[attempt] if retry_each_url_once else urls[attempt % len(urls)]
            )
            try:
                return await operation(attempt_url)
            except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as e:
                last_exception = e

                if isinstance(e, aiohttp.ClientResponseError) and 400 <= e.status < 500:
                    if e.status not in retryable_client_statuses:
                        LOG.error(f"{operation_name} failed with client error: {e}")
                        raise

                # Calculate backoff delay
                if attempt < attempt_limit - 1:
                    delay = self._retry_delay(attempt, e)
                    target = f" via {attempt_url}" if attempt_url else ""
                    message = (
                        f"{operation_name}{target} failed (attempt {attempt + 1}/{attempt_limit}): {e}. "
                        f"Retrying in {delay:.2f}s..."
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

                    await self.sleep(delay)
                else:
                    error_message = (
                        f"{operation_name} failed after {attempt_limit} attempts: {e}"
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


def _header_values(headers, name: str) -> tuple[str, ...]:
    try:
        return tuple(headers.getall(name, ()))
    except AttributeError:
        value = headers.get(name)
        return (value,) if value else ()


def _dedupe_url_groups(
    groups: Sequence[Sequence[str]],
) -> tuple[tuple[str, ...], ...]:
    deduped_groups: list[tuple[str, ...]] = []
    seen: set[str] = set()

    for group in groups:
        deduped_group = []
        for url in group:
            if url in seen:
                continue
            deduped_group.append(url)
            seen.add(url)
        if deduped_group:
            deduped_groups.append(tuple(deduped_group))

    return tuple(deduped_groups)


def _build_grouped_retry_sequence(
    groups: Sequence[Sequence[str]],
    attempt_budget: int,
) -> tuple[str, ...]:
    retry_sequence: list[str] = []
    remaining_budget = max(0, attempt_budget)
    groups = tuple(group for group in groups if group)

    for index, group in enumerate(groups):
        if remaining_budget <= 0:
            break

        remaining_groups = len(groups) - index - 1
        attempts_for_group = max(1, remaining_budget - remaining_groups)
        attempts_for_group = min(len(group), attempts_for_group)
        retry_sequence.extend(group[:attempts_for_group])
        remaining_budget -= attempts_for_group

    return tuple(retry_sequence)


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
    mirror_strategy: FileMirrorStrategy

    def __init__(
        self,
        base_dir: Path,
        naming_strategy: FileNamingStrategy,
        session: aiohttp.ClientSession,
        queue: asyncio.Queue[CollectorFileEntry],
        check_modified: bool = True,
        mirror_strategy: FileMirrorStrategy | None = None,
    ):
        self.base_dir = base_dir
        self.session = session
        self.queue = queue
        self.naming_strategy = naming_strategy
        self.check_modified = check_modified
        self.retry_helper = RetryHelper()
        self.mirror_strategy = mirror_strategy or ArchiveRandomMirrorStrategy()

    async def _resolve_file_plan(self, plan: MirrorAttemptPlan) -> MirrorAttemptPlan:
        if plan.osdf_director_url is None:
            return plan

        try:
            async with self.session.get(
                plan.osdf_director_url,
                allow_redirects=False,
            ) as response:
                if response.status not in {301, 302, 303, 307, 308}:
                    return plan

                osdf_urls: list[str] = []
                location = response.headers.get("Location")
                if location and is_absolute_http_url(location):
                    osdf_urls.append(location)
                osdf_urls.extend(
                    parse_duplicate_link_urls(_header_values(response.headers, "Link"))
                )
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError) as e:
            LOG.debug(
                "Failed to discover OSDF alternatives for %s: %s",
                plan.osdf_director_url,
                e,
            )
            return plan

        fallback_urls = tuple(url for url in plan.urls if url != plan.osdf_director_url)
        retry_groups = _dedupe_url_groups((tuple(osdf_urls), fallback_urls))
        if not retry_groups:
            return plan

        retry_sequence = _build_grouped_retry_sequence(
            retry_groups,
            self.retry_helper.max_retries + 1,
        )
        if not retry_sequence:
            return plan

        return MirrorAttemptPlan(
            urls=retry_sequence,
            retry_client_statuses=plan.retry_client_statuses,
            retry_each_url_once=True,
        )

    async def download_file(self, entry: CollectorFileEntry) -> None:
        target_file = self.naming_strategy.get_path(self.base_dir, entry)

        # Create target directory if it does not exist
        target_file.parent.mkdir(parents=True, exist_ok=True)

        base_plan = self.mirror_strategy.file_plan(entry)
        resolved_plan: MirrorAttemptPlan | None = None

        async def get_plan() -> MirrorAttemptPlan:
            nonlocal resolved_plan
            if resolved_plan is None:
                resolved_plan = await self._resolve_file_plan(base_plan)
            return resolved_plan

        t0 = time.time()
        if target_file.is_file():
            if not self.check_modified:
                LOG.debug(
                    "Skipping %s w/o modification check, already downloaded",
                    target_file,
                )
                return

            # check if file is modified with retry logic
            plan = await get_plan()
            head_kwargs = {"allow_redirects": True} if plan.head_allow_redirects else {}

            async def check_modified(url: str):
                async with self.session.head(url, **head_kwargs) as response:
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
                plan.urls,
                retry_client_statuses=plan.retry_client_statuses,
                retry_each_url_once=plan.retry_each_url_once,
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
        plan = await get_plan()

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
            plan.urls,
            retry_client_statuses=plan.retry_client_statuses,
            retry_each_url_once=plan.retry_each_url_once,
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
