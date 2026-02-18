import asyncio
import datetime
import itertools
import logging
from pathlib import Path
from typing import Generic, Literal, TypeVar

import aiohttp
import click
from tqdm import tqdm

from mrt_downloader.cache import (
    get_cache_db_path,
    get_cached_collectors,
    init_cache_db,
    store_collectors,
)
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

T = TypeVar("T")


class ProgressQueue(asyncio.Queue, Generic[T]):
    """Queue that updates a tqdm progress bar on each task_done() call."""

    def __init__(self, total: int, description: str, **kwargs):
        super().__init__(**kwargs)
        self._bar = tqdm(total=total, desc=description, unit="file")

    def task_done(self):
        super().task_done()
        self._bar.update(1)

    def close(self):
        self._bar.close()


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
    force_cache_refresh: bool = False,
):
    """Gather the list of update files per timestamp per rrc and download them."""
    assert start_time.tzinfo == datetime.UTC, "Start time must be in UTC"
    assert end_time.tzinfo == datetime.UTC, "End time must be in UTC"
    assert start_time < end_time, "Start time must be before end time"

    # Initialize cache database
    db_path = get_cache_db_path()
    await init_cache_db(db_path)
    LOG.info(f"Using index cache at {db_path}")

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

    # Get the collectors (from cache or API)
    async with aiohttp.ClientSession() as session:
        collector_infos_list: list[list[CollectorInfo]] = []

        for proj in project:
            # Try to get from cache first
            cached = await get_cached_collectors(proj, force_cache_refresh, db_path)

            if cached is not None:
                collector_infos_list.append(cached)
            else:
                # Fetch from API
                if proj == "ris":
                    infos = await get_ripe_ris_collectors(session)
                elif proj == "routeviews":
                    infos = await get_routeviews_collectors(session)
                else:
                    infos = []

                # Store in cache
                await store_collectors(proj, infos, db_path)
                collector_infos_list.append(infos)

        collector_infos = list(itertools.chain.from_iterable(collector_infos_list))
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

        index_queue: ProgressQueue[CollectorIndexEntry] = ProgressQueue(
            total=len(indices), description="Indexing"
        )
        for idx in indices:
            index_queue.put_nowait(idx)

        index_worker = IndexWorker(
            session,
            index_queue,
            file_types=file_types,
            db_path=db_path,
            force_cache_refresh=force_cache_refresh,
        )
        # run num_workers workers to get the indices.
        status = await asyncio.gather(*[index_worker.run() for _ in range(num_workers)])
        index_queue.close()

        LOG.info(
            "Processed %d directory indexes for %d collectors",
            sum(status),
            len(collector_infos),
        )

        # Add the relevant files to a list first to get the count
        files_to_download = []
        for file in index_worker.results:
            if (
                file.date >= start_time
                and file.date <= end_time
                and file.file_type in file_types
            ):
                files_to_download.append(file)

        LOG.info(
            "Selected %d files for download out of %d",
            len(files_to_download),
            len(index_worker.results),
        )

        queue: ProgressQueue[CollectorFileEntry] = ProgressQueue(
            total=len(files_to_download), description="Downloading"
        )
        download_worker = DownloadWorker(target_dir, naming_strategy, session, queue)

        for file in files_to_download:
            queue.put_nowait(file)

        # Now run the download workers.
        download_status = await asyncio.gather(
            *[download_worker.run() for _ in range(num_workers)]
        )
        queue.close()
        LOG.info("Downloaded %d files", sum(download_status))
