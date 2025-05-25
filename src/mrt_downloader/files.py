import pathlib

from mrt_downloader.http import FileNamingStrategy
from mrt_downloader.models import CollectorFileEntry


class ByCollectorNamingStrategy(FileNamingStrategy):
    base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path):
        self.base_path = base_path

    def get_path(self, entry: CollectorFileEntry) -> pathlib.Path:
        return self.base_path / entry.collector.name / entry.filename
