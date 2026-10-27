"""Copy the shared data contract from the infrastructure repo into contracts/v1.

The vendored copy must never be edited by hand: tests/test_contract_sync.py fails when it no
longer matches its SHA256SUMS or the original in ../infrastructure.

    python scripts/sync_contracts.py            # copy ../infrastructure/contracts/v1
    python scripts/sync_contracts.py --check    # only report drift, exit 1 if any
    python scripts/sync_contracts.py --source <path-to-infrastructure>/contracts/v1
"""

import argparse
import hashlib
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
VENDORED = REPO / "contracts" / "v1"
DEFAULT_SOURCE = REPO.parent / "infrastructure" / "contracts" / "v1"
SUMS_NAME = "SHA256SUMS"


def file_sums(directory: Path) -> dict[str, str]:
    """sha256 of every file except SHA256SUMS, with CRLF normalised so Windows checkouts match."""
    sums = {}
    for path in directory.rglob("*"):
        if path.is_file() and path.name != SUMS_NAME:
            data = path.read_bytes().replace(b"\r\n", b"\n")
            sums[path.relative_to(directory).as_posix()] = hashlib.sha256(data).hexdigest()
    return sums


def read_sums_file(directory: Path) -> dict[str, str]:
    sums = {}
    for line in (directory / SUMS_NAME).read_text(encoding="utf-8").splitlines():
        if line.strip():
            digest, name = line.split(maxsplit=1)
            sums[name] = digest
    return sums


def drift(source: Path, vendored: Path = VENDORED) -> list[str]:
    """Human-readable differences between the source contract and the vendored copy."""
    problems = []
    if file_sums(vendored) != read_sums_file(vendored):
        problems.append("vendored files do not match their SHA256SUMS (edited by hand?)")
    if source.is_dir() and read_sums_file(source) != read_sums_file(vendored):
        problems.append(f"vendored copy differs from source {source}")
    return problems


def sync(source: Path, vendored: Path = VENDORED) -> None:
    if file_sums(source) != read_sums_file(source):
        sys.exit(f"source {source} does not match its own SHA256SUMS; fix it in infrastructure first")
    if vendored.exists():
        shutil.rmtree(vendored)
    shutil.copytree(source, vendored)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    if args.check:
        if not args.source.is_dir():
            print(f"note: source {args.source} not found, only checking vendored checksums")
        problems = drift(args.source)
        for problem in problems:
            print(f"DRIFT: {problem}")
        sys.exit(1 if problems else 0)

    if not args.source.is_dir():
        sys.exit(f"source not found: {args.source} (clone infrastructure next to data-pipeline or pass --source)")
    sync(args.source)
    print(f"contracts/v1 synced ({len(read_sums_file(VENDORED))} files)")


if __name__ == "__main__":
    main()
