import random
import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Literal, Protocol

from mrt_downloader.models import CollectorFileEntry
from mrt_downloader.url_utils import is_absolute_http_url

MirrorUse = Literal["file", "index"]
Project = Literal["ris", "routeviews"]
RouteviewsMirrorStrategyName = Literal["archive-random", "osdf-preferred"]
DEFAULT_ROUTEVIEWS_MIRROR_STRATEGY: RouteviewsMirrorStrategyName = "osdf-preferred"
ROUTEVIEWS_OSDF_HOST = "osdf-director.osg-htc.org"
ROUTEVIEWS_OSDF_PATH_PREFIX = "/routeviews"
MIRRORED_FILE_RETRY_CLIENT_STATUSES = frozenset((404,))


@dataclass(frozen=True)
class MirrorAttemptPlan:
    urls: tuple[str, ...]
    retry_client_statuses: frozenset[int] = frozenset()
    head_allow_redirects: bool = False
    osdf_director_url: str | None = None
    retry_each_url_once: bool = False


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


def _split_link_header(value: str) -> tuple[str, ...]:
    links: list[str] = []
    start = 0
    in_quote = False
    in_angle = False
    escaped = False

    for index, char in enumerate(value):
        if escaped:
            escaped = False
            continue

        if in_quote and char == "\\":
            escaped = True
            continue

        if char == '"' and not in_angle:
            in_quote = not in_quote
            continue

        if not in_quote:
            if char == "<":
                in_angle = True
            elif char == ">":
                in_angle = False
            elif char == "," and not in_angle:
                links.append(value[start:index].strip())
                start = index + 1

    links.append(value[start:].strip())
    return tuple(link for link in links if link)


def _strip_quoted(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    return value


def _parse_link_params(value: str) -> dict[str, str]:
    params: dict[str, str] = {}
    for part in value.split(";"):
        name, separator, raw_value = part.strip().partition("=")
        if not name:
            continue
        params[name.lower()] = _strip_quoted(raw_value) if separator else ""
    return params


def parse_duplicate_link_urls(header_values: Iterable[str]) -> tuple[str, ...]:
    links: list[tuple[int | None, int, str]] = []
    order = 0

    for header_value in header_values:
        for link_value in _split_link_header(header_value):
            link_order = order
            order += 1
            if not link_value.startswith("<"):
                continue

            url_end = link_value.find(">")
            if url_end == -1:
                continue

            url = link_value[1:url_end].strip()
            if not is_absolute_http_url(url):
                continue

            params = _parse_link_params(link_value[url_end + 1 :])
            rels = {rel.lower() for rel in params.get("rel", "").split()}
            if "duplicate" not in rels:
                continue

            try:
                priority = int(params["pri"])
            except (KeyError, ValueError):
                priority = None

            links.append((priority, link_order, url))

    links.sort(
        key=lambda link: (
            link[0] is None,
            link[0] if link[0] is not None else 0,
            link[1],
        )
    )

    return tuple(dict.fromkeys(url for _priority, _index, url in links))


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
            osdf_director_url=osdf_url,
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
