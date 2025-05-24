from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncGenerator

import aiohttp


@dataclass
class CollectorInfo:
    collector_name: str
    project: str
    base_url: str
    collector_location: str
    logical_location: str
    activated: datetime
    deactivated: datetime | None = None


async def get_ripe_ris_collectors(session: aiohttp.ClientSession) -> AsyncGenerator[CollectorInfo]:
    """
    Get the list of RIPE RIS collectors.
    """
    async with session.get("https://stat.ripe.net/data/rrc-info/data.json", raise_for_status=True) as resp:
        data = await resp.json()

        rrcs = data["data"]["rrcs"]
        for rrc in rrcs:
            if rrc.get("deactivated_on", None):
                deactivated = datetime.strptime(rrc["deactivated_on"], "%Y-%m").replace(day=1)
                # Make sure we capture the final day of the month
                deactivated = deactivated + timedelta(days=31)
            else:
                deactivated = None

            yield CollectorInfo(
                collector_name=rrc["name"],
                project="RIPE RIS",
                base_url=f"https://data.ris.ripe.net/{rrc['name'].lower()}/",
                collector_location=rrc["geographical_location"],
                logical_location=rrc["topological_location"],
                activated=datetime.strptime(rrc["activated_on"], "%Y-%m").replace(day=1),
                # "" for still active.
                deactivated=deactivated,
            )


async def get_routeviews_collectors(session: aiohttp.ClientSession) -> AsyncGenerator[CollectorInfo]:
    """
    Get the list of RouteViews collectors.
    """
    async with session.get("https://api.routeviews.org/guest/collector/", raise_for_status=True) as resp:
        data = await resp.json()

        for collector in data["collectors"]:
            yield CollectorInfo(
                collector_name=collector["name"],
                project="RouteViews",
                base_url=f"https://archive.routeviews.org/{collector['name'].lower()}/",
                collector_location=collector["location"],
                logical_location=collector["logical_location"],
                activated=datetime.fromisoformat(collector["activated"]),
                deactivated=datetime.fromisoformat(collector["deactivated"]) if collector["deactivated"] else None,
            )