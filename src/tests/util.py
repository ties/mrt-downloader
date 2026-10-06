from collections.abc import Iterable

from mrt_downloader.models import CollectorInfo


def find_collector(
    collectors: Iterable[CollectorInfo], name: str | None = None
) -> CollectorInfo:
    return next(filter(lambda c: c.name == name, collectors))
