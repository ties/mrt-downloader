from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest

import mrt_downloader.collectors as collectors_module
from mrt_downloader.collectors import get_ripe_ris_collectors, get_routeviews_collectors
from mrt_downloader.http import RetryHelper


class FakeCollectorResponse:
    def __init__(
        self,
        url: str,
        status: int,
        payload: dict[str, Any] | None = None,
    ):
        self.url = url
        self.status = status
        self.payload = payload or {}
        self.headers: dict[str, str] = {}
        self.history = ()
        self.request_info = SimpleNamespace(real_url=url)
        self.raise_for_status = False

    async def __aenter__(self):
        if self.raise_for_status and self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=self.request_info,
                history=self.history,
                status=self.status,
                message=f"HTTP {self.status}",
                headers=self.headers,
            )
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        return None

    async def json(self, **_kwargs):
        return self.payload


class FakeCollectorSession:
    def __init__(self, responses: dict[str, list[FakeCollectorResponse]]):
        self.responses = responses
        self.get_urls: list[str] = []

    def get(self, url: str, *, raise_for_status: bool = False):
        self.get_urls.append(url)
        response = self.responses[url].pop(0)
        response.raise_for_status = raise_for_status
        return response


def install_fast_retry_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    async def sleep(_delay: float) -> None:
        return None

    helper = RetryHelper(
        max_retries=1,
        initial_delay=0,
        random_jitter=lambda _delay: 0,
        sleep=sleep,
    )
    monkeypatch.setattr(collectors_module, "RetryHelper", lambda: helper)


@pytest.mark.asyncio
async def test_get_routeviews_collectors_retries_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fast_retry_helper(monkeypatch)
    url = "https://api.routeviews.org/meta/collectors"
    session = FakeCollectorSession(
        {
            url: [
                FakeCollectorResponse(url, 429),
                FakeCollectorResponse(
                    url,
                    200,
                    {
                        "data": {
                            "collectors": {
                                "route-views.example": {
                                    "baseURL": "https://archive.routeviews.org/route-views.example/bgpdata/",
                                    "dataTypes": {
                                        "rib": {
                                            "oldestDumpTimeISO8601": "2025-01-01T00:00:00+00:00",
                                            "latestDumpTimeISO8601": "2025-01-02T00:00:00+00:00",
                                        }
                                    },
                                }
                            }
                        }
                    },
                ),
            ]
        }
    )

    result = await get_routeviews_collectors(session)  # type: ignore[arg-type]

    assert session.get_urls == [url, url]
    assert len(result) == 1
    assert result[0].name == "route-views.example"


@pytest.mark.asyncio
async def test_get_ripe_ris_collectors_retries_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fast_retry_helper(monkeypatch)
    url = "https://stat.ripe.net/data/rrc-info/data.json"
    session = FakeCollectorSession(
        {
            url: [
                FakeCollectorResponse(url, 429),
                FakeCollectorResponse(
                    url,
                    200,
                    {
                        "data": {
                            "rrcs": [
                                {
                                    "name": "RRC00",
                                    "activated_on": "1999-10",
                                }
                            ]
                        }
                    },
                ),
            ]
        }
    )

    result = await get_ripe_ris_collectors(session)  # type: ignore[arg-type]

    assert session.get_urls == [url, url]
    assert len(result) == 1
    assert result[0].name == "RRC00"
