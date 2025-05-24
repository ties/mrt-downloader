from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import AsyncGenerator

import aiohttp


@dataclass
class CollectorInfo:
    collector_name: str
    project: str
    base_url: str
    installed: datetime
    removed: datetime | None = None


async def get_ripe_ris_collectors(session: aiohttp.ClientSession) -> AsyncGenerator[CollectorInfo]:
    """
    Get the list of RIPE RIS collectors.
    """
    async with session.get("https://stat.ripe.net/data/rrc-info/data.json", raise_for_status=True) as resp:
        data = await resp.json()

        rrcs = data["data"]["rrcs"]
        for rrc in rrcs:
            if rrc.get("deactivated_on", None):
                deactivated = datetime.strptime(rrc["deactivated_on"], "%Y-%m").replace(day=1, tzinfo=UTC)
                # Make sure we capture the final day of the month
                deactivated = (deactivated + timedelta(days=32)).replace(day=1)
            else:
                deactivated = None

            yield CollectorInfo(
                collector_name=rrc["name"],
                project="RIS",
                base_url=f"https://data.ris.ripe.net/{rrc['name'].lower()}/",
                installed=datetime.strptime(rrc["activated_on"], "%Y-%m").replace(day=1, tzinfo=UTC),
                # "" for still active.
                removed=deactivated,
            )


async def get_routeviews_collectors(session: aiohttp.ClientSession) -> AsyncGenerator[CollectorInfo]:
    """
    Get the list of RouteViews collectors.
    """
    async with session.get("https://api.routeviews.org/guest/collector/", raise_for_status=True) as resp:
        data = await resp.json()

        for collector in data["results"]:
            yield CollectorInfo(
                collector_name=collector["name"],
                project="RV",
                base_url=collector["url"],
                installed=datetime.fromisoformat(collector["installed"]),
                removed=datetime.fromisoformat(collector["removed"]) if collector["removed"] else None,
            )