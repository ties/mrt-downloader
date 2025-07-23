import datetime
from typing import Any

import aiohttp

from mrt_downloader.models import CollectorInfo


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


def parse_routeviews_collectors(obj: dict[str, dict[Any, str]]) -> list[CollectorInfo]:
    collectors: list[CollectorInfo] = []
    for collector in obj["results"]:
        base_url = f"https://archive.routeviews.org/{collector['name']}/bgpdata/"

        collectors.append(
            CollectorInfo(
                name=collector["name"],
                project="routeviews",
                base_url=base_url,
                installed=datetime.datetime.fromisoformat(collector["installed"]),
                removed=datetime.datetime.fromisoformat(collector["removed"])
                if collector["removed"]
                else None,
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
        "https://api.routeviews.org/guest/collector/", raise_for_status=True
    ) as resp:
        data = await resp.json()

        return parse_routeviews_collectors(data)
