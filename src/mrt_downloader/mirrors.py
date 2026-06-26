import urllib.parse
from dataclasses import dataclass
from typing import Literal

from mrt_downloader.models import CollectorFileEntry

MirrorUse = Literal["file", "index"]
Project = Literal["ris", "routeviews"]
ROUTEVIEWS_OSDF_HOST = "osdf-director.osg-htc.org"
ROUTEVIEWS_OSDF_PATH_PREFIX = "/routeviews"


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


def _routeviews_file_url_alternatives(url: str) -> tuple[str, ...]:
    parsed = urllib.parse.urlsplit(url)
    policy = ARCHIVE_MIRROR_POLICIES["routeviews"]

    if parsed.hostname == ROUTEVIEWS_OSDF_HOST:
        archive_path = parsed.path.removeprefix(ROUTEVIEWS_OSDF_PATH_PREFIX)
        if archive_path == parsed.path:
            return (url,)
    elif parsed.hostname in policy.hosts:
        archive_path = parsed.path
    else:
        return (url,)

    alternatives = [
        urllib.parse.urlunsplit(
            (
                "https",
                ROUTEVIEWS_OSDF_HOST,
                f"{ROUTEVIEWS_OSDF_PATH_PREFIX}{archive_path}",
                parsed.query,
                parsed.fragment,
            )
        ),
        *(
            urllib.parse.urlunsplit(
                ("https", host, archive_path, parsed.query, parsed.fragment)
            )
            for host in policy.hosts
        ),
    ]

    return tuple(dict.fromkeys(alternatives))


def file_url_alternatives(entry: CollectorFileEntry) -> tuple[str, ...]:
    if entry.collector.project == "routeviews":
        return _routeviews_file_url_alternatives(entry.url)

    policy = ARCHIVE_MIRROR_POLICIES.get(entry.collector.project)
    if policy is None:
        return (entry.url,)
    return policy.url_alternatives(entry.url, "file")
