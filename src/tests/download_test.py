import datetime
import logging

import pytest

from mrt_downloader.download import select_files_for_download
from mrt_downloader.models import CollectorFileEntry, CollectorInfo


def test_select_files_for_download_skips_malformed_cached_filename(
    caplog: pytest.LogCaptureFixture,
) -> None:
    collector = CollectorInfo(
        name="RRC00",
        project="ris",
        base_url="https://data.ris.ripe.net/rrc00/",
        installed=datetime.datetime(2001, 1, 1, tzinfo=datetime.UTC),
    )
    valid_entry = CollectorFileEntry(
        collector=collector,
        filename="updates.20260521.1500.gz",
        url="https://data.ris.ripe.net/rrc00/2026.05/updates.20260521.1500.gz",
        file_type="update",
    )
    malformed_entry = CollectorFileEntry(
        collector=collector,
        filename="updates.20260521.1503.bad.gz",
        url="https://data.ris.ripe.net/rrc00/2026.05/updates.20260521.1503.bad.gz",
        file_type="update",
    )

    with caplog.at_level(logging.WARNING):
        selected = select_files_for_download(
            [valid_entry, malformed_entry],
            datetime.datetime(2026, 5, 21, 15, tzinfo=datetime.UTC),
            datetime.datetime(2026, 5, 21, 16, tzinfo=datetime.UTC),
            frozenset({"update"}),
        )

    assert selected == [valid_entry]
    assert "Skipping file with invalid MRT filename" in caplog.text
    assert "updates.20260521.1503.bad.gz" in caplog.text
