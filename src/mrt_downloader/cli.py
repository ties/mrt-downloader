"""
MRT download utility
"""

import asyncio
import datetime
import logging
import multiprocessing
import sys
import warnings
from pathlib import Path
from typing import Literal, Optional

import click

from mrt_downloader.download import download_files
from mrt_downloader.files import (
    ByCollectorPartitionedStategy,
    ByMonthStrategy,
    PrefixCollectorByHourStrategy,
    PrefixCollectorStrategy,
)

LOG = logging.getLogger(__name__)  #
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
@click.option(
    "--partition-directories",
    is_flag=True,
    default=False,
    help="Partition directories by [year]/[month]/[day]/[hour] (deprecated)",
    deprecated=True,
)
@click.option("--verbose", is_flag=True, help="Enable verbose logging")
@click.option(
    "--bview-only",
    is_flag=True,
    help="Download bview files only (use --rib-only)",
    deprecated=True,
)
@click.option("--rib-only", is_flag=True, help="Download full RIB files only.")
@click.option("--update-only", is_flag=True, help="Download update files only")
@click.option(
    "--rrc",
    type=str,
    multiple=True,
    default=[],
    help="RRC (number) to download from (e.g. 1 for rrc01) - use --collector",
    deprecated=True,
)
@click.option(
    "--collector",
    type=str,
    multiple=True,
    default=[],
    help="collectors to download from (e.g. rrc00, ...)",
    deprecated=False,
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
    type=click.Choice(["hour", "collector-month", "flat"]),
    default="collector-month",
    help="Partitioning strategy for downloaded files: hour is one directory per hour (old --partition), collector-month is similar to structure on data.ris.ripe.net, flat is one directory (filename prefixed with the collector, followed by original name)",
)
@click.option(
    "--num-threads",
    type=int,
    default=multiprocessing.cpu_count(),
    help="Number of download worker threads",
)
def cli(
    target_dir: Path,
    create_target: bool,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    verbose: bool,
    update_only: bool,
    num_threads: int,
    partition_directories: bool,
    project: list[Literal["ris", "routeviews"]],
    partitioning: Literal["hour", "collector-month", "flat"] = "collector-month",
    collector: list[str] | None = None,
    rrc: Optional[list[str]] = None,
    rib_only: bool | None = None,
    bview_only: bool | None = None,
):
    """
    Download a set of BGP updates from RIS.
    """
    if rrc and collector:
        click.echo(
            click.style(
                "Cannot specify both --rrc and --collector. Please use --collector.",
                fg="red",
            )
        )
        sys.exit(1)

    if rrc:
        warnings.warn(
            "--rrc is deprecated. Please use --collector instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        click.echo(
            click.style(
                "Warning: --rrc is deprecated. This will be deprecated on/after 1-9-20205. Please use --collector instead.",
                fg="yellow",
            )
        )

    if bview_only:
        warnings.warn("--bview-only is deprecated.", DeprecationWarning, stacklevel=2)
        click.echo(
            click.style(
                "Warning: --bview-only is deprecated. This will be deprecated on/after 1-9-20205. Please use ---only instead.",
                fg="yellow",
            )
        )

    effective_rib_only = bool(rib_only or bview_only)
    effective_collectors = (
        collector if collector else [f"rrc{x:02}" for x in rrc] if rrc else []
    )

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

    if update_only and effective_rib_only:
        click.echo(
            click.style(
                "Cannot specify both --update-only and --rib-only/--bview-only",
                fg="red",
            )
        )
        sys.exit(1)

    click.echo(
        click.style(
            f"Downloading updates from {start_time} to {end_time} to {target_dir}",
            fg="green",
        )
    )

    if partitioning and partition_directories:
        click.echo(
            click.style(
                "Cannot specify both --partitioning and --partition-directories",
                fg="red",
            )
        )
        sys.exit(1)

    naming_strategy = PrefixCollectorStrategy()

    if partition_directories:
        click.echo(
            click.style(
                "Partitioning directories by hour (deprecated, use --partitioning=hour)",
                fg="yellow",
            )
        )
        naming_strategy = PrefixCollectorByHourStrategy()

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
            rib_only=effective_rib_only,
            update_only=update_only,
            collectors=effective_collectors,
            num_workers=num_threads,
            naming_strategy=naming_strategy,
            project=frozenset(project),
        )
    )


if __name__ == "__main__":
    cli()
