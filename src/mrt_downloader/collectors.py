import datetime
import json
import logging
from typing import Any

import aiohttp

from mrt_downloader.models import CollectorInfo

LOG = logging.getLogger(__name__)


def parse_ripe_ris_collectors(obj: dict[str, dict[str, Any]]) -> list[CollectorInfo]:
    """Parse RIPEstat RIS collector data"""
    rrcs = obj["data"]["rrcs"]
    collectors: list[CollectorInfo] = []

    for rrc in rrcs:
        if rrc.get("deactivated_on", None):
            deactivated = datetime.datetime.strptime(
                rrc["deactivated_on"], "%Y-%m"
            ).replace(day=1, tzinfo=datetime.UTC)
            # Make sure we capture the final day of the month
            deactivated = (deactivated + datetime.timedelta(days=32)).replace(day=1)
        else:
            deactivated = None

        collectors.append(
            CollectorInfo(
                name=rrc["name"],
                project="ris",
                base_url=f"https://data.ris.ripe.net/{rrc['name'].lower()}/",
                installed=datetime.datetime.strptime(
                    rrc["activated_on"], "%Y-%m"
                ).replace(day=1, tzinfo=datetime.UTC),
                # "" for still active.
                removed=deactivated,
            )
        )

    return collectors


async def get_ripe_ris_collectors(
    session: aiohttp.ClientSession,
) -> list[CollectorInfo]:
    """
    Get the list of RIPE RIS collectors.
    """
    async with session.get(
        "https://stat.ripe.net/data/rrc-info/data.json", raise_for_status=True
    ) as resp:
        data = await resp.json()

        return parse_ripe_ris_collectors(data)


def _parse_iso8601_datetime(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value)


def parse_routeviews_collectors(obj: dict[str, Any]) -> list[CollectorInfo]:
    collectors: list[CollectorInfo] = []
    for name, collector in obj["data"]["collectors"].items():
        data_types = collector.get("dataTypes", {})
        oldest_dump_times = [
            _parse_iso8601_datetime(data_type["oldestDumpTimeISO8601"])
            for data_type in data_types.values()
            if data_type.get("oldestDumpTimeISO8601")
        ]
        latest_dump_times = [
            _parse_iso8601_datetime(data_type["latestDumpTimeISO8601"])
            for data_type in data_types.values()
            if data_type.get("latestDumpTimeISO8601")
        ]

        if not oldest_dump_times:
            LOG.warning(
                "Skipping RouteViews collector %s without oldest dump time", name
            )
            continue

        collectors.append(
            CollectorInfo(
                name=name,
                project="routeviews",
                base_url=collector["baseURL"],
                installed=min(oldest_dump_times),
                removed=max(latest_dump_times) if latest_dump_times else None,
            )
        )

    return collectors


async def get_routeviews_collectors(
    session: aiohttp.ClientSession,
) -> list[CollectorInfo]:
    """
    Get the list of RouteViews collectors.
    """
    async with session.get(
        "https://api.routeviews.org/meta/collectors", raise_for_status=True
    ) as resp:
        data = await resp.json(content_type=None)
        LOG.debug(
            "RouteViews collector metadata API response:\n%s",
            json.dumps(data, indent=2, sort_keys=True),
        )

        return parse_routeviews_collectors(data)
