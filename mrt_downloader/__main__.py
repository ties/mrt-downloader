"""
MRT download utility
"""
import asyncio
import datetime
import logging
import multiprocessing

from pathlib import Path
from typing import Optional

import click

from mrt_downloader.download import main_process

LOG = logging.getLogger(__name__)#
logging.basicConfig(level=logging.INFO)


BVIEW_DATE_TYPE = click.DateTime(formats=[
    '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M'
])

@click.command()
@click.argument('target_dir', type=click.Path(exists=True, file_okay=False, path_type=Path), default=Path.cwd() / "mrt")
@click.argument('start-time', type=BVIEW_DATE_TYPE)
@click.argument('end-time', type=BVIEW_DATE_TYPE)
@click.option('--create-target', is_flag=True, default=False, help='Create target directory')
@click.option('--verbose', is_flag=True, help='Enable verbose logging')
@click.option('--bview-only', is_flag=True, help='Download bview files only')
@click.option('--update-only', is_flag=True, help='Download update files only')
@click.option('--rrc', type=str, multiple=True, default=[], help='RRCs to download from')
@click.option('--num-threads', type=int, default=multiprocessing.cpu_count(), help='Number of download worker threads')
def main(target_dir: Path, create_target: bool, start_time: datetime.datetime, end_time: datetime.datetime, verbose: bool, bview_only: bool, update_only: bool, num_threads: int, rrc: Optional[str]):
    """
    Download a set of BGP updates from RIS.
    """
    if not target_dir.exists():
        if create_target:
            # Make directory if needed
            target_dir.mkdir(exist_ok=True)
        else:
            click.echo(click.style(f"Target directory ({target_dir}) does not exist. Exiting. Use --create-target to automatically create it.", fg="red"))
            return


    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        click.echo(click.style("Verbose mode enabled", fg='yellow'))
    else:
        logging.getLogger().setLevel(logging.INFO)

    if update_only and bview_only:
        click.echo(click.style("Cannot specify both --update-only and --bview-only", fg='red'))
        return

    click.echo(click.style(f"Downloading updates from {start_time} to {end_time} to {target_dir}", fg='green'))
    asyncio.run(main_process(target_dir, start_time, end_time, bview_only=bview_only, update_only=update_only, rrc=rrc, num_workers=num_threads))

if __name__ == "__main__":
    main()