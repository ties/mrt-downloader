import asyncio
import datetime
import itertools
import logging
from pathlib import Path
from types import CoroutineType
from typing import Any, Literal

import aiohttp
import click

from mrt_downloader.collector_index import (
    index_files_for_collector,
)
from mrt_downloader.collectors import get_ripe_ris_collectors, get_routeviews_collectors
from mrt_downloader.http import DownloadWorker, FileNamingStrategy, IndexWorker
from mrt_downloader.models import (
    CollectorFileEntry,
    CollectorIndexEntry,
    CollectorInfo,
)

LOG = logging.getLogger(__name__)

BVIEW_DATE_TYPE = click.DateTime(
    formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"]
)


async def download_files(
    target_dir: Path,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    num_workers: int,
    naming_strategy: FileNamingStrategy,
    rib_only: bool = False,
    update_only: bool = False,
    collectors: list[str] | None = None,
    project: frozenset[Literal["ris", "routeviews"]] = frozenset(["ris"]),
):
    """Gather the list of update files per timestamp per rrc and download them."""
    assert start_time.tzinfo == datetime.UTC, "Start time must be in UTC"
    assert end_time.tzinfo == datetime.UTC, "End time must be in UTC"
    assert start_time < end_time, "Start time must be before end time"

    file_types = frozenset(
        ["rib"] if rib_only else ["update"] if update_only else ["rib", "update"]
    )

    if collectors is not None and len(collectors) > 0:
        click.echo(
            click.style(
                f"Downloading updates from _only_ these collectors: {collectors}",
                fg="green",
            )
        )

    # Get the collectors
    async with aiohttp.ClientSession() as session:
        collector_tasks: list[CoroutineType[Any, Any, list[CollectorInfo]]] = []
        if "ris" in project:
            collector_tasks.append(get_ripe_ris_collectors(session))
        if "routeviews" in project:
            collector_tasks.append(get_routeviews_collectors(session))

        collector_infos = list(
            itertools.chain.from_iterable(await asyncio.gather(*collector_tasks))
        )
        # Filter collectors based on the provided list
        collector_infos = [
            collector
            for collector in collector_infos
            if (
                not collectors
                or (collector.name.lower() in set(c.lower() for c in collectors))
            )
        ]
        click.echo(
            click.style(
                f"Gathering index for {len(collector_infos)} collectors: "
                f"{', '.join(c.name for c in collector_infos)}",
                fg="green",
            )
        )
        # Now get the index paths - these are filtered for the relevant period.
        indices = list(
            itertools.chain.from_iterable(
                [
                    index_files_for_collector(collector, start_time, end_time)
                    for collector in collector_infos
                ]
            )
        )

        index_queue: asyncio.Queue[CollectorIndexEntry] = asyncio.Queue()
        for idx in indices:
            index_queue.put_nowait(idx)

        index_worker = IndexWorker(session, index_queue)
        # run num_workers workers to get the indices.
        status = await asyncio.gather(*[index_worker.run() for _ in range(num_workers)])

        LOG.info(
            "Processed %d directory indexes for %d collectors",
            sum(status),
            len(collector_infos),
        )

        queue: asyncio.Queue[CollectorFileEntry] = asyncio.Queue()
        download_worker = DownloadWorker(target_dir, naming_strategy, session, queue)

        # Add the relevant files to queue
        collected_files = 0
        for file in index_worker.results:
            if (
                file.date >= start_time
                and file.date <= end_time
                and file.file_type in file_types
            ):
                queue.put_nowait(file)
                collected_files += 1

        LOG.info(
            "Selected %d files for download out of %d",
            collected_files,
            len(index_worker.results),
        )

        # Now run the download workers.
        download_status = await asyncio.gather(
            *[download_worker.run() for _ in range(num_workers)]
        )
        LOG.info("Downloaded %d files", sum(download_status))
