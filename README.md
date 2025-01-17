# Download MRT updates

Download MRT updates. Just from RIPE RIS for now.

```
mkdir mrt
# download a day's MRT files into the mrt directory.
poetry run python -m mrt_downloader mrt 2025-01-16T00:50 2025-01-17T00:00
```

## Installation

```
pipx install git+https://github.com/ties/mrt-downloader.git
# Now the command should be available as `mrt-downloader`
mrt-downloader mrt 2025-01-16T00:50 2025-01-17T00:00
```
