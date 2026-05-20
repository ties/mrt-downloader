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

    def get(self, url: str) -> FakeResponse:
        self.get_urls.append(url)
        return self.responses[url].pop(0)

    def head(self, url: str) -> FakeResponse:
        self.head_urls.append(url)
        return self.responses[url].pop(0)


def _client_error(status: int, url: str) -> aiohttp.ClientResponseError:
    return aiohttp.ClientResponseError(
        request_info=SimpleNamespace(real_url=url),
        history=(),
        status=status,
        message=f"HTTP {status}",
        headers={},
    )


def test_routeviews_file_url_alternatives_include_primary_and_secondary() -> None:
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        file_type="update",
    )

    assert file_url_alternatives(entry) == (
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
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
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
async def test_download_worker_retries_routeviews_file_on_secondary(
    tmp_path: Path,
) -> None:
    primary_url = (
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    secondary_url = (
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    session = FakeSession(
        {
            primary_url: [FakeResponse(primary_url, 404)],
            secondary_url: [FakeResponse(secondary_url, 200, body=b"mrt")],
        }
    )
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url=primary_url,
        file_type="update",
    )
    worker = DownloadWorker(
        tmp_path,
        ByCollectorStrategy(),
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )
    worker.retry_helper = RetryHelper(
        max_retries=1,
        initial_delay=0,
        random_start=lambda _n: 0,
    )

    await worker.download_file(entry)

    assert session.get_urls == [primary_url, secondary_url]
    assert (
        tmp_path / "route-views.bknix" / "updates.20250501.0000.bz2"
    ).read_bytes() == b"mrt"


@pytest.mark.asyncio
async def test_download_worker_retries_routeviews_head_on_secondary(
    tmp_path: Path,
) -> None:
    primary_url = (
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    secondary_url = (
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    last_modified = datetime.datetime(2025, 5, 1, tzinfo=datetime.UTC)
    session = FakeSession(
        {
            primary_url: [FakeResponse(primary_url, 404)],
            secondary_url: [
                FakeResponse(
                    secondary_url,
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
        url=primary_url,
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
        random_start=lambda _n: 0,
    )

    await worker.download_file(entry)

    assert session.head_urls == [primary_url, secondary_url]
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
