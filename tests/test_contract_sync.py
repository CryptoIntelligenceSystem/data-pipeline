"""The vendored contract (contracts/v1) must be an untouched copy of infrastructure/contracts/v1."""

import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sync_contracts", REPO / "scripts" / "sync_contracts.py")
sync_contracts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync_contracts)


def test_vendored_files_match_their_checksums():
    assert sync_contracts.file_sums(sync_contracts.VENDORED) == sync_contracts.read_sums_file(sync_contracts.VENDORED), (
        "contracts/v1 was edited by hand; change it in infrastructure and run scripts/sync_contracts.py"
    )


@pytest.mark.skipif(not sync_contracts.DEFAULT_SOURCE.is_dir(), reason="../infrastructure not cloned next to this repo")
def test_vendored_copy_matches_infrastructure_original():
    assert sync_contracts.drift(sync_contracts.DEFAULT_SOURCE) == [], (
        "contracts/v1 is out of date: run `python scripts/sync_contracts.py`"
    )


def test_drift_is_detected(tmp_path):
    source = tmp_path / "source"
    vendored = tmp_path / "vendored"
    for directory, version in ((source, "1.0.1\n"), (vendored, "1.0.0\n")):
        directory.mkdir()
        (directory / "VERSION").write_text(version)
        sums = sync_contracts.file_sums(directory)
        (directory / "SHA256SUMS").write_text("".join(f"{d}  {n}\n" for n, d in sums.items()))
    assert sync_contracts.drift(source, vendored) == [f"vendored copy differs from source {source}"]

    (vendored / "VERSION").write_text("edited\n")
    assert "vendored files do not match their SHA256SUMS (edited by hand?)" in sync_contracts.drift(source, vendored)
