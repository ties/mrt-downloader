import urllib.parse


def is_absolute_http_url(url: str) -> bool:
    parsed = urllib.parse.urlsplit(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
