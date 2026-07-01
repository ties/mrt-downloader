import random
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Literal, Protocol

from mrt_downloader.models import CollectorFileEntry

MirrorUse = Literal["file", "index"]
Project = Literal["ris", "routeviews"]
RouteviewsMirrorStrategyName = Literal["archive-random", "osdf-preferred"]
DEFAULT_ROUTEVIEWS_MIRROR_STRATEGY: RouteviewsMirrorStrategyName = "archive-random"
ROUTEVIEWS_OSDF_HOST = "osdf-director.osg-htc.org"
ROUTEVIEWS_OSDF_PATH_PREFIX = "/routeviews"
MIRRORED_FILE_RETRY_CLIENT_STATUSES = frozenset((404,))


@dataclass(frozen=True)
class MirrorAttemptPlan:
    urls: tuple[str, ...]
    retry_client_statuses: frozenset[int] = frozenset()
    head_allow_redirects: bool = False


class FileMirrorStrategy(Protocol):
    def file_plan(self, entry: CollectorFileEntry) -> MirrorAttemptPlan:
        pass


@dataclass(frozen=True)
class ProjectMirrorStrategy:
    strategies: Mapping[Project, FileMirrorStrategy]
    default_strategy: FileMirrorStrategy = field(
        default_factory=lambda: ArchiveRandomMirrorStrategy()
    )

    def file_plan(self, entry: CollectorFileEntry) -> MirrorAttemptPlan:
        strategy = self.strategies.get(entry.collector.project, self.default_strategy)
        return strategy.file_plan(entry)


@dataclass(frozen=True)
class ArchiveMirrorPolicy:
    project: Project
    primary_host: str
    mirror_hosts: tuple[str, ...] = ()
    mirror_uses: frozenset[MirrorUse] = frozenset()

    @property
    def hosts(self) -> tuple[str, ...]:
        return (self.primary_host, *self.mirror_hosts)

    def url_alternatives(self, url: str, use: MirrorUse) -> tuple[str, ...]:
        parsed = urllib.parse.urlsplit(url)
        if parsed.hostname not in self.hosts:
            return (url,)

        if use not in self.mirror_uses:
            return (self._replace_host(url, self.primary_host),)

        return tuple(self._replace_host(url, host) for host in self.hosts)

    def _replace_host(self, url: str, host: str) -> str:
        parsed = urllib.parse.urlsplit(url)
        netloc = host
        if parsed.port is not None:
            netloc = f"{netloc}:{parsed.port}"
        return urllib.parse.urlunsplit(
            (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
        )


ARCHIVE_MIRROR_POLICIES: dict[Project, ArchiveMirrorPolicy] = {
    "ris": ArchiveMirrorPolicy(
        project="ris",
        primary_host="data.ris.ripe.net",
    ),
    "routeviews": ArchiveMirrorPolicy(
        project="routeviews",
        primary_host="archive.routeviews.org",
        mirror_hosts=("archive2.routeviews.org",),
        mirror_uses=frozenset(("file",)),
    ),
}


def _rotate(
    urls: tuple[str, ...],
    random_start: Callable[[int], int],
) -> tuple[str, ...]:
    if len(urls) <= 1:
        return urls

    start = random_start(len(urls))
    return urls[start:] + urls[:start]


def _retry_client_statuses_for_urls(urls: tuple[str, ...]) -> frozenset[int]:
    if len(urls) > 1:
        return MIRRORED_FILE_RETRY_CLIENT_STATUSES
    return frozenset()


def _routeviews_archive_path(url: str) -> tuple[str, str, str] | None:
    parsed = urllib.parse.urlsplit(url)
    policy = ARCHIVE_MIRROR_POLICIES["routeviews"]

    if parsed.hostname == ROUTEVIEWS_OSDF_HOST:
        archive_path = parsed.path.removeprefix(ROUTEVIEWS_OSDF_PATH_PREFIX)
        if archive_path == parsed.path:
            return None
    elif parsed.hostname in policy.hosts:
        archive_path = parsed.path
    else:
        return None

    return archive_path, parsed.query, parsed.fragment


def _routeviews_osdf_url(url: str) -> str | None:
    archive_parts = _routeviews_archive_path(url)
    if archive_parts is None:
        return None

    archive_path, query, fragment = archive_parts
    return urllib.parse.urlunsplit(
        (
            "https",
            ROUTEVIEWS_OSDF_HOST,
            f"{ROUTEVIEWS_OSDF_PATH_PREFIX}{archive_path}",
            query,
            fragment,
        )
    )


def _routeviews_archive_urls(url: str) -> tuple[str, ...] | None:
    archive_parts = _routeviews_archive_path(url)
    if archive_parts is None:
        return None

    archive_path, query, fragment = archive_parts
    policy = ARCHIVE_MIRROR_POLICIES["routeviews"]
    return tuple(
        urllib.parse.urlunsplit(("https", host, archive_path, query, fragment))
        for host in policy.hosts
    )


@dataclass(frozen=True)
class ArchiveRandomMirrorStrategy:
    random_start: Callable[[int], int] = random.randrange

    def file_plan(self, entry: CollectorFileEntry) -> MirrorAttemptPlan:
        urls = file_url_alternatives(entry)
        urls = _rotate(urls, self.random_start)
        return MirrorAttemptPlan(
            urls=urls,
            retry_client_statuses=_retry_client_statuses_for_urls(urls),
        )


@dataclass(frozen=True)
class OsdfPreferredMirrorStrategy:
    random_start: Callable[[int], int] = random.randrange

    def file_plan(self, entry: CollectorFileEntry) -> MirrorAttemptPlan:
        archive_strategy = ArchiveRandomMirrorStrategy(random_start=self.random_start)
        if entry.collector.project != "routeviews":
            return archive_strategy.file_plan(entry)

        osdf_url = _routeviews_osdf_url(entry.url)
        archive_urls = _routeviews_archive_urls(entry.url)
        if osdf_url is None or archive_urls is None:
            return archive_strategy.file_plan(entry)

        urls = tuple(
            dict.fromkeys((osdf_url, *_rotate(archive_urls, self.random_start)))
        )
        return MirrorAttemptPlan(
            urls=urls,
            retry_client_statuses=_retry_client_statuses_for_urls(urls),
            head_allow_redirects=True,
        )


def mirror_strategy_from_name(
    name: RouteviewsMirrorStrategyName,
) -> FileMirrorStrategy:
    match name:
        case "archive-random":
            return ArchiveRandomMirrorStrategy()
        case "osdf-preferred":
            return OsdfPreferredMirrorStrategy()
    raise ValueError(f"Unknown RouteViews mirror strategy: {name}")


def file_url_alternatives(entry: CollectorFileEntry) -> tuple[str, ...]:
    policy = ARCHIVE_MIRROR_POLICIES.get(entry.collector.project)
    if policy is None:
        return (entry.url,)
    return policy.url_alternatives(entry.url, "file")
