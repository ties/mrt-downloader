"""
MRT download utility
"""

import asyncio
import datetime
import logging
import multiprocessing
import os
import sys
from pathlib import Path
from typing import Literal

import click

from mrt_downloader.download import download_files, validate_existing_file_policy_config
from mrt_downloader.files import (
    ByCollectorPartitionedStategy,
    ByMonthStrategy,
    ByYearStrategy,
    PrefixCollectorByHourStrategy,
    PrefixCollectorStrategy,
)
from mrt_downloader.mirrors import (
    DEFAULT_ROUTEVIEWS_MIRROR_STRATEGY,
    RouteviewsMirrorStrategyName,
)
from mrt_downloader.models import ExistingFilePolicy

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


CLICK_DATETIME_TYPE = click.DateTime(
    formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"]
)


@click.command()
@click.argument(
    "target_dir",
    type=click.Path(exists=False, file_okay=False, path_type=Path),
    default=Path.cwd() / "mrt",
)
@click.argument("start-time", type=CLICK_DATETIME_TYPE)
@click.argument("end-time", type=CLICK_DATETIME_TYPE)
@click.option(
    "--create-target", is_flag=True, default=False, help="Create target directory"
)
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
@click.option("--rib-only", is_flag=True, help="Download full RIB files only.")
@click.option("--update-only", is_flag=True, help="Download update files only")
@click.option(
    "--collector",
    type=str,
    multiple=True,
    default=[],
    help="collectors to download from (e.g. rrc00, ...)",
)
@click.option(
    "--project",
    type=click.Choice(["ris", "routeviews"]),
    multiple=True,
    default=["ris"],
    help="Project to download from: 'ris' (RIPE RIS) or 'routeviews'. Can be specified multiple times to select both.",
)
@click.option(
    "--partitioning",
    type=click.Choice(["hour", "collector-month", "collector-year", "flat"]),
    default="collector-month",
    help="Partitioning strategy for downloaded files: hour is one directory per hour (old --partition), collector-month is similar to structure on data.ris.ripe.net, flat is one directory (filename prefixed with the collector, followed by original name)",
)
@click.option(
    "--num-threads",
    type=int,
    default=os.environ.get(
        "MRT_DOWNLOADER_PARALLELISM", min(4, multiprocessing.cpu_count())
    ),
    help="Number of download worker threads (default: min(4, #cores). override using MRT_DOWNLOADER_PARALLELISM)",
)
@click.option(
    "--force-cache-refresh",
    is_flag=True,
    default=False,
    help="Force refresh all caches (collectors and indexes), ignoring cached data",
)
@click.option(
    "--routeviews-mirror-strategy",
    type=click.Choice(["archive-random", "osdf-preferred"]),
    default=DEFAULT_ROUTEVIEWS_MIRROR_STRATEGY,
    show_default=True,
    help="RouteViews file mirror strategy.",
)
@click.option(
    "--existing-file-policy",
    type=click.Choice(["trust-existing", "validate", "redownload"]),
    default="trust-existing",
    show_default=True,
    help="How to handle target files that already exist.",
)
def cli(
    target_dir: Path,
    create_target: bool,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    verbose: bool,
    update_only: bool,
    rib_only: bool,
    collector: list[str],
    num_threads: int,
    project: list[Literal["ris", "routeviews"]],
    partitioning: Literal["hour", "collector-month", "flat"] = "collector-month",
    force_cache_refresh: bool = False,
    routeviews_mirror_strategy: RouteviewsMirrorStrategyName = (
        DEFAULT_ROUTEVIEWS_MIRROR_STRATEGY
    ),
    existing_file_policy: ExistingFilePolicy = "trust-existing",
):
    """
    Download a set of BGP updates from RIS.
    """
    if not target_dir.exists():
        if create_target:
            # Make directory if needed
            target_dir.mkdir(exist_ok=True, parents=True)
        else:
            click.echo(
                click.style(
                    f"Target directory ({target_dir}) does not exist. Exiting. Use --create-target to automatically create it.",
                    fg="red",
                )
            )
            return

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        click.echo(click.style("Verbose mode enabled", fg="yellow"))
    else:
        logging.getLogger().setLevel(logging.INFO)

    if update_only and rib_only:
        click.echo(
            click.style(
                "Cannot specify both --update-only and --rib-only/--bview-only",
                fg="red",
            )
        )
        sys.exit(1)

    try:
        validate_existing_file_policy_config(
            frozenset(project),
            routeviews_mirror_strategy,
            existing_file_policy,
        )
    except ValueError as e:
        click.echo(click.style(f"Error: {e}", fg="red"))
        sys.exit(1)

    click.echo(
        click.style(
            f"Downloading updates from {start_time} to {end_time} to {target_dir}",
            fg="green",
        )
    )

    naming_strategy = PrefixCollectorStrategy()

    match partitioning:
        case "hour":
            click.echo(click.style("Partitioning directories by hour", fg="green"))
            naming_strategy = PrefixCollectorByHourStrategy()
        case "collector-month":
            click.echo(
                click.style(
                    "Partitioning directories by collector and month", fg="green"
                )
            )
            naming_strategy = ByCollectorPartitionedStategy(ByMonthStrategy())
        case "collector-year":
            click.echo(
                click.style(
                    "Partitioning directories by collector and year", fg="green"
                )
            )
            naming_strategy = ByCollectorPartitionedStategy(ByYearStrategy())

        case "flat":
            click.echo(
                click.style(
                    "Flat directory structure with collector prefix", fg="green"
                )
            )
            naming_strategy = PrefixCollectorStrategy()

    asyncio.run(
        download_files(
            target_dir,
            start_time.replace(tzinfo=datetime.UTC),
            end_time.replace(tzinfo=datetime.UTC),
            rib_only=rib_only,
            update_only=update_only,
            collectors=collector,
            num_workers=num_threads,
            naming_strategy=naming_strategy,
            project=frozenset(project),
            force_cache_refresh=force_cache_refresh,
            routeviews_mirror_strategy=routeviews_mirror_strategy,
            existing_file_policy=existing_file_policy,
        )
    )


if __name__ == "__main__":
    cli()
