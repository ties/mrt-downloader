import pathlib

from mrt_downloader.http import FileNamingStrategy
from mrt_downloader.models import CollectorFileEntry


class ByCollectorPartitionedStategy(FileNamingStrategy):
    inner_strategy: FileNamingStrategy

    def __init__(self, inner: type[FileNamingStrategy] = None):
        self.inner_strategy = inner

    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        inner_base = path / entry.collector.name.lower()

        return self.inner_strategy.get_path(inner_base, entry)


class ByCollectorNamingStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / entry.collector.name.lower() / entry.filename


class ByMonthStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / entry.date.strftime("%Y.%m") / entry.filename


class ByDayStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return path / entry.date.strftime("%Y.%m.%d") / entry.filename


class ByHourStrategy(FileNamingStrategy):
    def get_path(self, path: pathlib.Path, entry: CollectorFileEntry) -> pathlib.Path:
        return (
            path
            / entry.date.strftime("%Y.%m.%d")
            / entry.date.strftime("%H")
            / entry.filename
        )
