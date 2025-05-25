import asyncio
import datetime
from pathlib import Path

import aiohttp
import click

from mrt_downloader.collector_index import (
    CollectorFileEntry,
    index_files_for_rrcs,
    process_rrc_index,
)
from mrt_downloader.http import Download, build_session, worker

BVIEW_DATE_TYPE = click.DateTime(
    formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"]
)


async def download_files(
    target_dir: Path,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    num_workers: int,
    bview_only: bool = False,
    update_only: bool = False,
    rrc: list[str] | None = None,
    partition_directories: bool = False,
):
    """Gather the list of update files per timestamp per rrc and download them."""
    matches: list[CollectorFileEntry] = []
    if rrc is not None and len(rrc) > 0:
        click.echo(
            click.style(f"Downloading updates from _only_ RRCs {rrc}", fg="green")
        )
        rrcs = [int(r) for r in rrc]
    else:
        # More RRCs than present
        rrcs = list(range(0, 28))

    index_urls = index_files_for_rrcs(rrcs, start_time, end_time)
    async with aiohttp.ClientSession() as session:
        indexes = await asyncio.gather(
            *[process_rrc_index(session, index) for index in index_urls]
        )
        for index in indexes:
            for entry in index:
                if entry.date >= start_time and entry.date <= end_time:
                    # bview only filtering
                    if bview_only and not entry.filename.startswith("bview."):
                        continue
                    if update_only and not entry.filename.startswith("updates."):
                        continue
                    matches.append(entry)

    queue = asyncio.Queue()

    for file in matches:
        target_base_dir = target_dir
        if partition_directories:
            target_base_dir = target_base_dir / file.date.strftime("%d/%H/")
            target_base_dir.mkdir(parents=True, exist_ok=True)

        # path name is prefixed with rrc to cluster files from the same rrc.
        target_path = target_base_dir / f"{file.collector}-{file.filename}"
        await queue.put(Download(file.url, target_path))

    click.echo(
        click.style(
            f"Downloading {len(matches)} files on {num_workers} workers", fg="green"
        )
    )

    async with build_session() as session:
        workers = [worker(session, queue) for i in range(num_workers)]

        statuses = await asyncio.gather(*workers)
        await queue.join()

        total_files = 0

        for status_count in statuses:
            total_files += status_count
            click.echo(click.style(f"Downloaded {status_count} files"))

        click.echo(
            click.style(
                f"Downloaded {total_files} files to {str(target_dir)}", fg="green"
            )
        )
