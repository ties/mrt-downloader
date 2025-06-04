import pathlib

from mrt_downloader.http import FileNamingStrategy
from mrt_downloader.models import CollectorFileEntry


class ByCollectorPartitionedStategy(FileNamingStrategy):
    base_path: pathlib.Path
    inner_strategy: FileNamingStrategy

    def __init__(self, base_path: pathlib.Path, inner: type[FileNamingStrategy] = None):
        self.base_path = base_path
        self.inner_strategy = inner

    def get_path(
        self, entry: CollectorFileEntry, override_path: pathlib.Path | None = None
    ) -> pathlib.Path:
        path = override_path if override_path else self.base_path
        inner_base = path / entry.collector.name.lower()

        return self.inner_strategy.get_path(entry, override_path=inner_base)


class ByCollectorNamingStrategy(FileNamingStrategy):
    base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path):
        self.base_path = base_path

    def get_path(
        self, entry: CollectorFileEntry, override_path: pathlib.Path | None = None
    ) -> pathlib.Path:
        path = override_path if override_path else self.base_path
        return path / entry.collector.name.lower() / entry.filename


class ByMonthStrategy(FileNamingStrategy):
    base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path):
        self.base_path = base_path

    def get_path(
        self, entry: CollectorFileEntry, override_path: pathlib.Path | None = None
    ) -> pathlib.Path:
        path = override_path if override_path else self.base_path
        return path / entry.date.strftime("%Y.%m") / entry.filename


class ByDayStrategy(FileNamingStrategy):
    base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path):
        self.base_path = base_path

    def get_path(
        self, entry: CollectorFileEntry, override_path: pathlib.Path | None = None
    ) -> pathlib.Path:
        path = override_path if override_path else self.base_path
        return path / entry.date.strftime("%Y.%m.%d") / entry.filename


class ByHourStrategy(FileNamingStrategy):
    base_path: pathlib.Path

    def __init__(self, base_path: pathlib.Path):
        self.base_path = base_path

    def get_path(
        self, entry: CollectorFileEntry, override_path: pathlib.Path | None = None
    ) -> pathlib.Path:
        path = override_path if override_path else self.base_path
        return (
            path
            / entry.date.strftime("%Y.%m.%d")
            / entry.date.strftime("%H")
            / entry.filename
        )
