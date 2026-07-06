import asyncio
import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest

from mrt_downloader.files import ByCollectorStrategy
from mrt_downloader.http import (
    DownloadWorker,
    RetryHelper,
    _build_grouped_retry_sequence,
)
from mrt_downloader.mirrors import (
    ARCHIVE_MIRROR_POLICIES,
    ArchiveRandomMirrorStrategy,
    MirrorAttemptPlan,
    OsdfPreferredMirrorStrategy,
    ProjectMirrorStrategy,
    file_url_alternatives,
    parse_duplicate_link_urls,
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
        self.get_kwargs: list[dict[str, Any]] = []
        self.head_urls: list[str] = []
        self.head_kwargs: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.get_urls.append(url)
        self.get_kwargs.append(kwargs)
        return self.responses[url].pop(0)

    def head(self, url: str, **kwargs: Any) -> FakeResponse:
        self.head_urls.append(url)
        self.head_kwargs.append(kwargs)
        return self.responses[url].pop(0)


class StaticMirrorStrategy:
    def __init__(self, url: str):
        self.url = url

    def file_plan(self, _entry: CollectorFileEntry) -> MirrorAttemptPlan:
        return MirrorAttemptPlan(urls=(self.url,))


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


def _metadata_path(target_file: Path) -> Path:
    return target_file.with_name(f"{target_file.name}.download-metadata.json")


def _write_metadata(
    target_file: Path,
    *,
    etag: str = '"abc"',
    last_modified: str = "Thu, 01 May 2025 00:00:00 GMT",
    content_length: int = 3,
) -> None:
    _metadata_path(target_file).write_text(
        json.dumps(
            {
                "version": 1,
                "source_url": "https://example/source",
                "final_url": "https://example/source",
                "validation_url": "https://example/source",
                "etag": etag,
                "last_modified": last_modified,
                "content_length": content_length,
                "downloaded_at": "2025-05-01T00:00:00+00:00",
                "validated_at": "2025-05-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )


def test_routeviews_file_url_alternatives_include_archive_mirrors() -> None:
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


def test_archive_random_strategy_rotates_routeviews_archive_mirrors() -> None:
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        file_type="update",
    )

    plan = ArchiveRandomMirrorStrategy(random_start=lambda _n: 1).file_plan(entry)

    assert plan.urls == (
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
    )
    assert plan.retry_client_statuses == frozenset((404,))
    assert plan.head_allow_redirects is False


def test_osdf_preferred_strategy_uses_osdf_then_random_archive_mirrors() -> None:
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        file_type="update",
    )

    plan = OsdfPreferredMirrorStrategy(random_start=lambda _n: 1).file_plan(entry)

    assert plan.urls == (
        "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag",
    )
    assert plan.retry_client_statuses == frozenset((404,))
    assert plan.head_allow_redirects is True
    assert (
        plan.osdf_director_url
        == "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2?x=1#frag"
    )


def test_osdf_preferred_strategy_handles_osdf_input_url() -> None:
    entry = CollectorFileEntry(
        collector=ROUTEVIEWS_COLLECTOR,
        filename="updates.20250501.0000.bz2",
        url="https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        file_type="update",
    )

    plan = OsdfPreferredMirrorStrategy(random_start=lambda _n: 0).file_plan(entry)

    assert plan.urls == (
        "https://osdf-director.osg-htc.org/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
    )


def test_parse_duplicate_link_urls_orders_filters_and_dedupes() -> None:
    headers = [
        (
            '<https://cache3.example/file>; rel="duplicate"; pri=3; depth=4, '
            '<https://cache1.example/file>; rel="duplicate"; pri=1; depth=4, '
            '<https://ignored.example/file>; rel="describedby"; pri=0, '
            '<https://cache2.example/file>; rel="duplicate alternate"; pri=2, '
            '<https://cache1.example/file>; rel="duplicate"; pri=4, '
            '<ftp://bad.example/file>; rel="duplicate"; pri=0'
        ),
        '<https://cache4.example/file>; rel="duplicate"',
    ]

    assert parse_duplicate_link_urls(headers) == (
        "https://cache1.example/file",
        "https://cache2.example/file",
        "https://cache3.example/file",
        "https://cache4.example/file",
    )


def test_grouped_retry_sequence_reserves_fallback_attempts() -> None:
    assert _build_grouped_retry_sequence(
        (
            (
                "https://cache1.example/file",
                "https://cache2.example/file",
                "https://cache3.example/file",
                "https://cache4.example/file",
                "https://cache5.example/file",
                "https://cache6.example/file",
            ),
            (
                "https://archive2.routeviews.org/file",
                "https://archive.routeviews.org/file",
            ),
        ),
        attempt_budget=5,
    ) == (
        "https://cache1.example/file",
        "https://cache2.example/file",
        "https://cache3.example/file",
        "https://cache4.example/file",
        "https://archive2.routeviews.org/file",
    )

    assert _build_grouped_retry_sequence(
        (
            ("https://cache1.example/file", "https://cache2.example/file"),
            ("https://archive2.routeviews.org/file",),
        ),
        attempt_budget=2,
    ) == (
        "https://cache1.example/file",
        "https://archive2.routeviews.org/file",
    )


def test_project_mirror_strategy_dispatches_by_project() -> None:
    strategy = ProjectMirrorStrategy(
        {
            "ris": StaticMirrorStrategy("https://ris.example/file"),
            "routeviews": StaticMirrorStrategy("https://routeviews.example/file"),
        }
    )

    assert strategy.file_plan(
        CollectorFileEntry(
            collector=RIS_COLLECTOR,
            filename="updates.20250501.0000.gz",
            url="https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz",
            file_type="update",
        )
    ).urls == ("https://ris.example/file",)
    assert strategy.file_plan(
        CollectorFileEntry(
            collector=ROUTEVIEWS_COLLECTOR,
            filename="updates.20250501.0000.bz2",
            url="https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/updates.20250501.0000.bz2",
            file_type="update",
        )
    ).urls == ("https://routeviews.example/file",)


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
async def test_retry_helper_uses_supplied_url_order() -> None:
    helper = RetryHelper(max_retries=0, initial_delay=0)
    urls: list[str] = []

    async def operation(url: str) -> str:
        urls.append(url)
        return url

    result = await helper.execute_with_urls(
        operation,
        "Download example",
        ("https://archive.routeviews.org/file", "https://archive2.routeviews.org/file"),
    )

    assert result == "https://archive.routeviews.org/file"
    assert urls == ["https://archive.routeviews.org/file"]


@pytest.mark.asyncio
async def test_retry_helper_rotates_routeviews_404() -> None:
    helper = RetryHelper(max_retries=1, initial_delay=0)
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
async def test_retry_helper_can_walk_urls_without_wrapping() -> None:
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    helper = RetryHelper(
        max_retries=4,
        initial_delay=0,
        sleep=sleep,
    )
    urls: list[str] = []

    async def operation(url: str) -> str:
        urls.append(url)
        raise _client_error(500, url)

    with pytest.raises(aiohttp.ClientResponseError):
        await helper.execute_with_urls(
            operation,
            "Download example",
            ("https://cache1.example/file", "https://cache2.example/file"),
            retry_each_url_once=True,
        )

    assert urls == ["https://cache1.example/file", "https://cache2.example/file"]
    assert sleeps == [0]


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
async def test_download_worker_retries_routeviews_archive_mirrors(
    tmp_path: Path,
) -> None:
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
        mirror_strategy=ArchiveRandomMirrorStrategy(random_start=lambda _n: 0),
    )
    worker.retry_helper = RetryHelper(
        max_retries=1,
        initial_delay=0,
    )

    await worker.download_file(entry)

    assert session.get_urls == [archive_url, archive2_url]
    assert (
        tmp_path / "route-views.bknix" / "updates.20250501.0000.bz2"
    ).read_bytes() == b"mrt"


@pytest.mark.asyncio
async def test_download_worker_osdf_preferred_reserves_archive_fallback_attempt(
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
    cache_urls = tuple(
        f"https://cache{i}.example/routeviews/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
        for i in range(1, 7)
    )
    link_header = ", ".join(
        f'<{url}>; rel="duplicate"; pri={index}; depth=4'
        for index, url in enumerate(cache_urls, start=1)
    )
    session = FakeSession(
        {
            osdf_url: [
                FakeResponse(
                    osdf_url,
                    307,
                    headers={
                        "Location": cache_urls[0],
                        "Link": link_header,
                    },
                )
            ],
            cache_urls[0]: [FakeResponse(cache_urls[0], 404)],
            cache_urls[1]: [FakeResponse(cache_urls[1], 404)],
            cache_urls[2]: [FakeResponse(cache_urls[2], 404)],
            cache_urls[3]: [FakeResponse(cache_urls[3], 404)],
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
        mirror_strategy=OsdfPreferredMirrorStrategy(random_start=lambda _n: 1),
    )
    worker.retry_helper = RetryHelper(max_retries=4, initial_delay=0)

    await worker.download_file(entry)

    assert session.get_urls == [osdf_url, *cache_urls[:4], archive2_url]
    assert session.get_kwargs == [
        {"allow_redirects": False},
        {},
        {},
        {},
        {},
        {},
    ]
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
    naming_strategy = ByCollectorStrategy()
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )
    worker.retry_helper = RetryHelper(max_retries=1, initial_delay=0)

    await worker.download_file(entry)

    target_file = naming_strategy.get_path(tmp_path, entry)
    assert session.get_urls == [url, url]
    assert target_file.read_bytes() == b"complete"
    assert not list(target_file.parent.glob("*.tmp"))


@pytest.mark.asyncio
async def test_download_worker_default_policy_trusts_existing_target(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    session = FakeSession({})
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"existing")
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )

    await worker.download_file(entry)

    assert target_file.read_bytes() == b"existing"
    assert session.get_urls == []
    assert session.head_urls == []


@pytest.mark.asyncio
async def test_download_worker_validate_uses_conditional_get_304(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    last_modified = "Thu, 01 May 2025 00:00:00 GMT"
    session = FakeSession(
        {
            url: [
                FakeResponse(
                    url,
                    304,
                    headers={
                        "ETag": '"abc"',
                        "Last-Modified": last_modified,
                    },
                )
            ]
        }
    )
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"mrt")
    _write_metadata(target_file, last_modified=last_modified)
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
        existing_file_policy="validate",
    )
    worker.retry_helper = RetryHelper(max_retries=0, initial_delay=0)

    await worker.download_file(entry)

    assert target_file.read_bytes() == b"mrt"
    assert session.get_urls == [url]
    assert session.get_kwargs == [
        {
            "headers": {
                "If-None-Match": '"abc"',
                "If-Modified-Since": last_modified,
            }
        }
    ]
    assert session.head_urls == []
    metadata = json.loads(_metadata_path(target_file).read_text(encoding="utf-8"))
    assert metadata["etag"] == '"abc"'
    assert metadata["validated_at"] != "2025-05-01T00:00:00+00:00"


@pytest.mark.asyncio
async def test_download_worker_validate_conditional_get_200_replaces_target(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    last_modified = "Thu, 01 May 2025 00:00:00 GMT"
    session = FakeSession(
        {
            url: [
                FakeResponse(
                    url,
                    200,
                    body=b"new",
                    headers={
                        "Content-Length": "3",
                        "ETag": '"def"',
                        "Last-Modified": last_modified,
                    },
                )
            ]
        }
    )
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"old")
    _write_metadata(target_file, last_modified=last_modified)
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
        existing_file_policy="validate",
    )
    worker.retry_helper = RetryHelper(max_retries=0, initial_delay=0)

    await worker.download_file(entry)

    assert target_file.read_bytes() == b"new"
    assert session.get_urls == [url]
    assert session.get_kwargs == [
        {
            "headers": {
                "If-None-Match": '"abc"',
                "If-Modified-Since": last_modified,
            }
        }
    ]
    metadata = json.loads(_metadata_path(target_file).read_text(encoding="utf-8"))
    assert metadata["etag"] == '"def"'
    assert metadata["content_length"] == 3
    assert not list(target_file.parent.glob("*.download-metadata.json.tmp"))


@pytest.mark.asyncio
async def test_download_worker_validate_without_metadata_redownloads(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    session = FakeSession({url: [FakeResponse(url, 200, body=b"fresh")]})
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"old")
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
        existing_file_policy="validate",
    )
    worker.retry_helper = RetryHelper(max_retries=0, initial_delay=0)

    await worker.download_file(entry)

    assert target_file.read_bytes() == b"fresh"
    assert session.get_urls == [url]
    assert session.get_kwargs == [{}]


@pytest.mark.asyncio
async def test_download_worker_validate_304_size_mismatch_redownloads(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    last_modified = "Thu, 01 May 2025 00:00:00 GMT"
    session = FakeSession(
        {
            url: [
                FakeResponse(url, 304),
                FakeResponse(
                    url,
                    200,
                    body=b"complete",
                    headers={
                        "Content-Length": "8",
                        "Last-Modified": last_modified,
                    },
                ),
            ]
        }
    )
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"bad")
    _write_metadata(target_file, last_modified=last_modified, content_length=8)
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
        existing_file_policy="validate",
    )
    worker.retry_helper = RetryHelper(max_retries=0, initial_delay=0)

    await worker.download_file(entry)

    assert target_file.read_bytes() == b"complete"
    assert session.get_urls == [url, url]
    assert session.get_kwargs == [
        {
            "headers": {
                "If-None-Match": '"abc"',
                "If-Modified-Since": last_modified,
            }
        },
        {},
    ]


@pytest.mark.asyncio
async def test_download_worker_redownload_policy_ignores_existing_target(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    session = FakeSession({url: [FakeResponse(url, 200, body=b"fresh")]})
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"old")
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
        existing_file_policy="redownload",
    )
    worker.retry_helper = RetryHelper(max_retries=0, initial_delay=0)

    await worker.download_file(entry)

    assert target_file.read_bytes() == b"fresh"
    assert session.get_urls == [url]
    assert session.get_kwargs == [{}]


@pytest.mark.asyncio
async def test_download_worker_validate_retries_routeviews_archive_mirrors(
    tmp_path: Path,
) -> None:
    archive_url = (
        "https://archive.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    archive2_url = (
        "https://archive2.routeviews.org/route-views.bknix/bgpdata/2025.05/UPDATES/"
        "updates.20250501.0000.bz2"
    )
    last_modified = "Thu, 01 May 2025 00:00:00 GMT"
    session = FakeSession(
        {
            archive_url: [FakeResponse(archive_url, 404)],
            archive2_url: [
                FakeResponse(
                    archive2_url,
                    304,
                    headers={
                        "ETag": '"abc"',
                        "Last-Modified": last_modified,
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
    _write_metadata(target_file, last_modified=last_modified)
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
        mirror_strategy=ArchiveRandomMirrorStrategy(random_start=lambda _n: 0),
        existing_file_policy="validate",
    )
    worker.retry_helper = RetryHelper(max_retries=1, initial_delay=0)

    await worker.download_file(entry)

    assert session.get_urls == [archive_url, archive2_url]
    assert session.get_kwargs == [
        {
            "headers": {
                "If-None-Match": '"abc"',
                "If-Modified-Since": last_modified,
            }
        },
        {
            "headers": {
                "If-None-Match": '"abc"',
                "If-Modified-Since": last_modified,
            }
        },
    ]
    assert session.head_urls == []


@pytest.mark.asyncio
async def test_download_worker_failed_download_leaves_existing_target_untouched(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    response = FakeResponse(url, 200, body=b"partial")
    response.content = FailingContent(
        b"partial",
        aiohttp.ClientPayloadError("Response payload is not completed"),
    )
    session = FakeSession({url: [response]})
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    target_file.parent.mkdir(parents=True)
    target_file.write_bytes(b"existing")
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
        existing_file_policy="redownload",
    )
    worker.retry_helper = RetryHelper(max_retries=0, initial_delay=0)

    with pytest.raises(aiohttp.ClientPayloadError):
        await worker.download_file(entry)

    assert target_file.read_bytes() == b"existing"
    assert not list(target_file.parent.glob("*.tmp"))


@pytest.mark.asyncio
async def test_download_worker_failed_download_leaves_no_final_target(
    tmp_path: Path,
) -> None:
    url = "https://data.ris.ripe.net/rrc00/2025.05/updates.20250501.0000.gz"
    response = FakeResponse(url, 200, body=b"partial")
    response.content = FailingContent(
        b"partial",
        aiohttp.ClientPayloadError("Response payload is not completed"),
    )
    session = FakeSession({url: [response]})
    entry = CollectorFileEntry(
        collector=RIS_COLLECTOR,
        filename="updates.20250501.0000.gz",
        url=url,
        file_type="update",
    )
    naming_strategy = ByCollectorStrategy()
    target_file = naming_strategy.get_path(tmp_path, entry)
    worker = DownloadWorker(
        tmp_path,
        naming_strategy,
        session,  # type: ignore[arg-type]
        asyncio.Queue(),
    )
    worker.retry_helper = RetryHelper(max_retries=0, initial_delay=0)

    with pytest.raises(aiohttp.ClientPayloadError):
        await worker.download_file(entry)

    assert not target_file.exists()
    assert not list(target_file.parent.glob("*.tmp"))


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
