"""Generate a Markdown summary for a packaged Phase 2 release bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List


DEFAULT_BUNDLE = Path("data/releases/run_20260108")
DEFAULT_NOTES = Path("data/releases/run_20260108/release_notes.md")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bundle",
        type=Path,
        default=DEFAULT_BUNDLE,
        help=f"Directory containing release_artifacts.json (default: {DEFAULT_BUNDLE}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_NOTES,
        help=f"Markdown file to write (default: {DEFAULT_NOTES}).",
    )
    parser.add_argument(
        "--title",
        default="Phase 2 Release Notes",
        help="Optional heading for the notes (default: Phase 2 Release Notes).",
    )
    return parser.parse_args()


def _load_summary(bundle_dir: Path) -> Dict:
    summary_path = bundle_dir / "release_artifacts.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"No release_artifacts.json found in {bundle_dir}")
    return json.loads(summary_path.read_text(encoding="utf-8"))


def _markdown_table(rows: List[Dict[str, str]]) -> str:
    header = "| Artifact | Release Path | SHA-256 |\n| --- | --- | --- |"
    lines = [header]
    for row in rows:
        name = row["label"]
        path = row["release_path"]
        sha = row["sha256"]
        lines.append(f"| {name} | `{path}` | `{sha}` |")
    return "\n".join(lines)


def _classify(rel_path: str) -> str:
    lower = rel_path.lower()
    if "phase1_run_manifest" in lower:
        return "Manifest"
    if "latest_sentence_events" in lower:
        return "Latest sentence events"
    if "narratives_label_kpi" in lower:
        return "Narratives KPI sample"
    if "narratives_unlabeled" in lower:
        return "Narratives unlabeled audit"
    if "narratives_change" in lower:
        return "Narratives change export"
    if lower.endswith("narratives_kpis.json"):
        return "KPI config"
    return Path(rel_path).name


def main() -> None:
    args = _parse_args()
    bundle_dir = args.bundle.resolve()
    summary = _load_summary(bundle_dir)
    artifacts = summary.get("artifacts", [])

    rows: List[Dict[str, str]] = []
    for artifact in artifacts:
        rows.append(
            {
                "label": _classify(artifact["release_path"]),
                "release_path": artifact["release_path"],
                "sha256": artifact["sha256"],
            }
        )

    lines = [
        f"# {args.title}",
        "",
        f"- **Bundle root:** `{bundle_dir}`",
        f"- **Manifest copy:** `{summary.get('manifest')}`",
        "- **Artifact hashes:** see table below (source: `release_artifacts.json`)",
        "",
        _markdown_table(rows),
    ]

    notes_path = args.output.resolve()
    notes_path.parent.mkdir(parents=True, exist_ok=True)
    notes_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote release notes to {notes_path}")


if __name__ == "__main__":
    main()
