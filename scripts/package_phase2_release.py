"""Bundle manifest + Phase 2 artifacts for release verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path
from typing import Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = Path("data/artifacts/phase1/phase1_run_manifest.json")
DEFAULT_OUTPUT = Path("data/releases/latest")
DEFAULT_CHANGE_GLOB = Path("data/artifacts/phase1/metrics/narratives_change_*.parquet")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help=f"Path to the Phase 1 manifest (default: {DEFAULT_MANIFEST}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Destination folder for the packaged artifacts (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--change-glob",
        type=Path,
        default=DEFAULT_CHANGE_GLOB,
        help="Glob pattern (relative or absolute) for change exports to include.",
    )
    parser.add_argument(
        "--extra",
        type=Path,
        nargs="*",
        default=(),
        help="Optional additional files to copy into the package.",
    )
    return parser.parse_args()


def _resolve(path: Path | str, repo_root: Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return (repo_root / candidate).resolve()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_file(src: Path, dest_root: Path, repo_root: Path) -> Path:
    src = src.resolve()
    if not src.exists():
        raise FileNotFoundError(f"Required artifact not found: {src}")
    try:
        relative = src.relative_to(repo_root)
    except ValueError:
        relative = Path(src.name)
    dest = dest_root / relative
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return relative


def _collect_phase2_paths(manifest_data: Dict, repo_root: Path) -> List[Path]:
    targets: List[Path] = []
    phase2 = manifest_data.get("phase2_artifacts") or {}
    for key in ("latest_sentence_events", "narratives_label_kpi_csv", "narratives_unlabeled_csv"):
        value = phase2.get(key)
        if value:
            targets.append(_resolve(value, repo_root))
    kpi_config = (phase2.get("kpi_config") or {}).get("path")
    if kpi_config:
        targets.append(_resolve(kpi_config, repo_root))
    change_glob = phase2.get("change_exports")
    if change_glob:
        targets.extend(_resolve(pattern, repo_root) for pattern in change_glob)
    return targets


def main() -> None:
    args = _parse_args()
    manifest_path = _resolve(args.manifest, REPO_ROOT)
    output_dir = args.output.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    files_to_copy: List[Path] = [manifest_path]

    files_to_copy.extend(_collect_phase2_paths(manifest_data, REPO_ROOT))

    change_glob = _resolve(args.change_glob, REPO_ROOT)
    files_to_copy.extend(sorted(change_glob.parent.glob(change_glob.name)))

    extra_files = [_resolve(path, REPO_ROOT) for path in args.extra]
    files_to_copy.extend(extra_files)

    copied: List[Dict[str, str]] = []
    for path in files_to_copy:
        relative = _copy_file(path, output_dir, REPO_ROOT)
        copied.append(
            {
                "source": str(path),
                "release_path": str(relative),
                "sha256": _sha256(output_dir / relative),
            }
        )

    manifest_copy = output_dir / manifest_path.relative_to(REPO_ROOT)
    summary = {
        "release_root": str(output_dir),
        "manifest": str(manifest_copy),
        "artifacts": copied,
    }
    summary_path = output_dir / "release_artifacts.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote release bundle summary to {summary_path}")


if __name__ == "__main__":
    main()
