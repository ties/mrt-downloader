import asyncio
import datetime
import email.utils
import os
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest

from mrt_downloader.files import ByCollectorStrategy
from mrt_downloader.http import DownloadWorker, RetryHelper
from mrt_downloader.mirrors import (
    ARCHIVE_MIRROR_POLICIES,
    file_url_alternatives,
)
from mrt_downloader.models import CollectorFileEntry, CollectorIndexEntry, CollectorInfo

ROUTEVIEWS_COLLECTOR = CollectorInfo(
    name="route-views.bknix",
    project="routeviews",
    base_url="https://archive.routeviews.org/route-views.bknix/bgpdata/",
    installed=datetime.datetime(2019, 10, 29, tzinfo=datetime.UTC),
)

RIS_COLLECTOR = CollectorInfo(
    name="RRC00",
    project="ris",
    base_url="https://data.ris.ripe.net/rrc00/",
    installed=datetime.datetime(1999, 10, 1, tzinfo=datetime.UTC),
)


class FakeContent:
    def __init__(self, body: bytes):
        self.body = body

    async def iter_chunked(self, _chunk_size: int):
        yield self.body


class FailingContent:
    def __init__(self, body: bytes, error: BaseException):
        self.body = body
        self.error = error

    async def iter_chunked(self, _chunk_size: int):
        yield self.body
        raise self.error


class FakeResponse:
    def __init__(
        self,
        url: str,
        status: int,
        *,
        body: bytes = b"",
        text: str = "",
        headers: dict[str, str] | None = None,
    ):
        self.url = url
        self.status = status
        self.headers = headers or {}
        self.history = ()
        self.request_info = SimpleNamespace(real_url=url)
        self.content = FakeContent(body)
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        return None

    async def text(self) -> str:
        return self._text


class FakeSession:
    def __init__(self, responses: dict[str, list[FakeResponse]]):
        self.responses = responses
        self.get_urls: list[str] = []
        self.head_urls: list[str] = []
        self.head_kwargs: list[dict[str, bool]] = []

    def get(self, url: str) -> FakeResponse:
        self.get_urls.append(url)
        return self.responses[url].pop(0)

    def head(self, url: str, **kwargs: bool) -> FakeResponse:
        self.head_urls.append(url)
        self.head_kwargs.append(kwargs)
        return self.responses[url].pop(0)


def _client_error(
    status: int, url: str, headers: dict[str, str] | None = None
) -> aiohttp.ClientResponseError:
    return aiohttp.ClientResponseError(
        request_info=SimpleNamespace(real_url=url),
        history=(),
        status=status,
        message=f"HTTP {status}",
        headers=headers or {},
    )


def test_routeviews_file_url_alternatives_use_osdf_then_archive_mirrors() -> None:
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        file_type="update",
    )

    assert file_url_alternatives(entry) == (
        "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
    )


def test_routeviews_secondary_file_url_alternatives_return_canonical_order() -> None:
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url="https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        file_type="update",
    )

    assert file_url_alternatives(entry) == (
        "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
    )


def test_routeviews_osdf_file_url_alternatives_return_canonical_order() -> None:
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url="https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        file_type="update",
    )

    assert file_url_alternatives(entry) == (
        "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
    )


def test_ris_file_url_alternatives_stay_primary_only() -> None:
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url="https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz",
        file_type="update",
    )

    assert file_url_alternatives(entry) == (
        "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz",
    )


def test_routeviews_index_url_alternatives_stay_primary_only() -> None:
    index = CollectorIndexEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/",
        time_period=datetime.datetime(2025, 5, 1, tzinfo=datetime.UTC),
        file_types=frozenset(("update",)),
    )

    policy = ARCHIVE_MIRROR_POLICIES[index.collector.project]
    assert policy.url_alternatives(index.url, "index") == (
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/",
    )


@pytest.mark.asyncio
async def test_retry_helper_starts_at_random_url() -> None:
    helper = RetryHelper(max_retries=0, initial_delay=0, random_start=lambda _n: 1)
    urls: list[str] = []

    async def operation(url: str) -> str:
        urls.append(url)
        return url

    result = await helper.execute_with_urls(
        operation,
        "Download example",
        ("https://archive.routeviews.org/file", "https://archive2.routeviews.org/file"),
    )

    assert result == "https://archive2.routeviews.org/file"
    assert urls == ["https://archive2.routeviews.org/file"]


@pytest.mark.asyncio
async def test_retry_helper_rotates_routeviews_404() -> None:
    helper = RetryHelper(max_retries=1, initial_delay=0, random_start=lambda _n: 0)
    urls: list[str] = []

    async def operation(url: str) -> str:
        urls.append(url)
        if len(urls) == 1:
            raise _client_error(404, url)
        return url

    result = await helper.execute_with_urls(
        operation,
        "Download example",
        ("https://archive.routeviews.org/file", "https://archive2.routeviews.org/file"),
        retry_client_statuses=frozenset((404,)),
    )

    assert result == "https://archive2.routeviews.org/file"
    assert urls == [
        "https://archive.routeviews.org/file",
        "https://archive2.routeviews.org/file",
    ]


@pytest.mark.asyncio
async def test_retry_helper_keeps_non_retryable_404_final() -> None:
    helper = RetryHelper(max_retries=1, initial_delay=0)

    async def operation(url: str) -> str:
        raise _client_error(404, url)

    with pytest.raises(aiohttp.ClientResponseError):
        await helper.execute_with_urls(
            operation,
            "Download example",
            ("https://data.ris.ripe.net/file",),
        )


@pytest.mark.asyncio
async def test_retry_helper_retries_429_by_default() -> None:
    sleeps: list[float] = []
    attempts = 0

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    helper = RetryHelper(
        max_retries=1,
        initial_delay=2,
        random_jitter=lambda delay: delay / 2,
        sleep=sleep,
    )

    async def operation(url: str) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _client_error(429, url)
        return url

    result = await helper.execute_with_urls(
        operation,
        "Download example",
        ("https://api.routeviews.org/meta/collectors",),
    )

    assert result == "https://api.routeviews.org/meta/collectors"
    assert attempts == 2
    assert sleeps == [3.0]


@pytest.mark.asyncio
async def test_retry_helper_uses_retry_after_as_minimum_delay_for_429() -> None:
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    helper = RetryHelper(
        max_retries=1,
        initial_delay=2,
        random_jitter=lambda delay: delay,
        sleep=sleep,
    )

    async def operation(url: str) -> str:
        raise _client_error(429, url, headers={"Retry-After": "5"})

    with pytest.raises(aiohttp.ClientResponseError):
        await helper.execute_with_urls(
            operation,
            "Download example",
            ("https://api.routeviews.org/meta/collectors",),
        )

    assert sleeps == [10.0]


@pytest.mark.asyncio
async def test_retry_helper_still_does_not_retry_other_client_errors() -> None:
    attempts = 0

    async def sleep(_delay: float) -> None:
        raise AssertionError("unexpected retry sleep")

    helper = RetryHelper(max_retries=1, initial_delay=0, sleep=sleep)

    async def operation(url: str) -> str:
        nonlocal attempts
        attempts += 1
        raise _client_error(403, url)

    with pytest.raises(aiohttp.ClientResponseError):
        await helper.execute_with_urls(
            operation,
            "Download example",
            ("https://api.routeviews.org/meta/collectors",),
        )

    assert attempts == 1


@pytest.mark.asyncio
async def test_download_worker_tries_routeviews_osdf_before_archive_mirrors(
    tmp_path: Path,
) -> None:
    osdf_url = (
        "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    archive_url = (
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    archive2_url = (
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    session = FakeSession(
        {
            osdf_url: [FakeResponse(osdf_url, 404)],
            archive_url: [FakeResponse(archive_url, 404)],
            archive2_url: [FakeResponse(archive2_url, 200, body=b"mrt")],
        }
    )
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url=archive_url,
        file_type="update",
    )
    worker = DownloadWorker(
        tmp_path,
        ByCollectorStrategy(),
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )
    worker.retry_helper = RetryHelper(
        max_retries=2,
        initial_delay=0,
        random_start=lambda _n: 2,
    )

    await worker.download_file(entry)

    assert session.get_urls == [osdf_url, archive_url, archive2_url]
    assert (
        tmp_path / "route-views.bknix" / "updates.20250501.0000.bz2"
    ).read_bytes() == b"mrt"


@pytest.mark.asyncio
async def test_download_worker_retries_incomplete_payload_without_partial_target(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    incomplete_response = FakeResponse(url, 200, body=b"partial")
    incomplete_response.content = FailingContent(
        b"partial",
        aiohttp.ClientPayloadError("Response payload is not completed"),
    )
    session = FakeSession(
        {
            url: [
                incomplete_response,
                FakeResponse(url, 200, body=b"complete"),
            ]
        }
    )
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    worker = DownloadWorker(
        tmp_path,
        ByCollectorStrategy(),
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )
    worker.retry_helper = RetryHelper(max_retries=1, initial_delay=0)

    await worker.download_file(entry)

    target_file = tmp_path / "RRC00" / "updates.20250501.0000.gz"
    assert session.get_urls == [url, url]
    assert target_file.read_bytes() == b"complete"
    assert not list(target_file.parent.glob("*.tmp"))


@pytest.mark.asyncio
async def test_download_worker_retries_routeviews_head_on_secondary(
    tmp_path: Path,
) -> None:
    osdf_url = (
        "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    archive_url = (
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    last_modified = datetime.datetime(2025, 5, 1, tzinfo=datetime.UTC)
    session = FakeSession(
        {
            osdf_url: [FakeResponse(osdf_url, 404)],
            archive_url: [
                FakeResponse(
                    archive_url,
                    200,
                    headers={
                        "Content-Length": "3",
                        "Last-Modified": email.utils.format_datetime(
                            last_modified, usegmt=True
                        ),
                    },
                )
            ],
        }
    )
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url=archive_url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"mrt")
    os.utime(target_file, (last_modified.timestamp(), last_modified.timestamp()))
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )
    worker.retry_helper = RetryHelper(
        max_retries=1,
        initial_delay=0,
        random_start=lambda _n: 2,
    )

    await worker.download_file(entry)

    assert session.head_urls == [osdf_url, archive_url]
    assert session.head_kwargs == [
        {"allow_redirects": True},
        {"allow_redirects": True},
    ]
    assert session.get_urls == []


@pytest.mark.asyncio
async def test_download_worker_does_not_retry_ris_404(tmp_path: Path) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    session = FakeSession({url: [FakeResponse(url, 404)]})
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    worker = DownloadWorker(
        tmp_path,
        ByCollectorStrategy(),
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )
    worker.retry_helper = RetryHelper(max_retries=1, initial_delay=0)

    with pytest.raises(aiohttp.ClientResponseError):
        await worker.download_file(entry)

    assert session.get_urls == [url]
