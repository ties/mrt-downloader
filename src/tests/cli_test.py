from pathlib import Path
from typing import Any

from click.testing import CliRunner

import mrt_downloader.cli as cli_module
from mrt_downloader.cli import cli


def test_cli_passes_routeviews_mirror_strategy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_download_files(*args: Any, **kwargs: Any) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(cli_module, "download_files", fake_download_files)

    result = CliRunner().invoke(
        cli,
        [
            str(tmp_path),
            "2025-05-01",
            "2025-05-02",
            "--project",
            "routeviews",
            "--routeviews-mirror-strategy",
            "osdf-preferred",
        ],
    )

    assert result.exit_code == 0
    assert captured["kwargs"]["routeviews_mirror_strategy"] == "osdf-preferred"
    assert captured["kwargs"]["existing_file_policy"] == "trust-existing"


def test_cli_passes_existing_file_policy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_download_files(*args: Any, **kwargs: Any) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(cli_module, "download_files", fake_download_files)

    result = CliRunner().invoke(
        cli,
        [
            str(tmp_path),
            "2025-05-01",
            "2025-05-02",
            "--existing-file-policy",
            "validate",
        ],
    )

    assert result.exit_code == 0
    assert captured["kwargs"]["existing_file_policy"] == "validate"


def test_cli_rejects_routeviews_osdf_validate_policy(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [
            str(tmp_path),
            "2025-05-01",
            "2025-05-02",
            "--project",
            "routeviews",
            "--existing-file-policy",
            "validate",
        ],
        color=True,
    )

    assert result.exit_code == 1
    assert "existing-file policy 'validate' cannot be used" in result.output
    assert "\x1b[31m" in result.output
