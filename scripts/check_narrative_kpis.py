"""Validate configured Phase 2 KPI artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

REPO_ROOT_DEFAULT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from src.analytics.narrative_kpis import NarrativeKPISpec, load_narrative_kpis


def _resolve(base: Path, candidate: str) -> Path:
    path = Path(candidate)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def _parse_bool(value: str | bool | None, *, context: str) -> bool:
    if isinstance(value, bool):
        return value
    text = (str(value or "")).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    raise SystemExit(f"Row '{context}' lacks a valid is_correct flag (expected true/false).")


def _load_label_sample(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader if any(row.values())]
    return rows


def _ensure_label_precision_sample(
    data_root: Path,
    sample_path: str,
    spec: NarrativeKPISpec,
) -> None:
    path = _resolve(data_root, sample_path)
    if not path.exists():
        raise SystemExit(f"Label precision sample not found at {path}. Run export_label_precision_sample.py.")

    rows = _load_label_sample(path)
    _assert(
        len(rows) >= spec.label_precision.sample_size,
        f"Label precision sample has {len(rows)} rows, need {spec.label_precision.sample_size}.",
    )

    totals: Dict[str, List[bool]] = {}
    correct_flags: List[bool] = []
    for row in rows:
        narrative_type = (row.get("narrative_type") or "").strip().lower()
        narrative_subtype = (row.get("narrative_subtype") or "").strip().lower()
        is_correct = _parse_bool(row.get("is_correct"), context=row.get("sentence_id", "unknown"))
        if not narrative_type:
            raise SystemExit(f"Row '{row}' missing narrative_type.")
        totals.setdefault(narrative_type, []).append(is_correct)
        if narrative_subtype:
            totals.setdefault(narrative_subtype, []).append(is_correct)
        correct_flags.append(is_correct)

    overall_precision = sum(correct_flags) / len(correct_flags)
    _assert(
        overall_precision >= spec.label_precision.min_precision,
        (
            f"Overall label precision {overall_precision:.2%} is below "
            f"{spec.label_precision.min_precision:.0%}. Review sample in {path}."
        ),
    )

    for label in spec.label_precision.high_risk_types:
        bucket = totals.get(label.lower())
        _assert(bucket, f"No sample rows found for high-risk narrative '{label}'. Re-sample before release.")
        precision = sum(bucket) / len(bucket)
        _assert(
            precision >= spec.label_precision.high_risk_min_precision,
            (
                f"High-risk narrative '{label}' precision {precision:.2%} is below "
                f"{spec.label_precision.high_risk_min_precision:.0%}."
            ),
        )


def _read_json_or_raise(path: Path) -> list[dict]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except UnicodeDecodeError as exc:
        raise SystemExit(
            f"Unable to parse {path} without pandas/pyarrow. Install the deps to generate parquet artifacts."
        ) from exc
    if isinstance(data, dict):
        return [data]
    return list(data)


def _read_table(path: Path) -> Sequence[dict]:
    try:
        import pandas as pd  # type: ignore
    except ImportError:
        pd = None  # type: ignore

    if not path.exists():
        raise SystemExit(f"Required metrics file missing: {path}")

    if pd is None:
        return _read_json_or_raise(path)

    try:
        frame = pd.read_parquet(path)
        return frame.to_dict("records")
    except Exception:
        return _read_json_or_raise(path)


def _ensure_change_exports(data_root: Path, export_glob: str, spec: NarrativeKPISpec) -> None:
    matches = sorted(data_root.glob(export_glob))
    _assert(matches, f"No change exports found using glob {export_glob!r} under {data_root}.")

    statuses_needing_delta = {"significant_increase", "significant_decrease"}
    new_statuses = {"new"}

    violations: List[str] = []
    for path in matches:
        rows = _read_table(path)
        if not rows:
            violations.append(f"{path} is empty.")
            continue
        for row in rows:
            status = str(
                row.get(spec.change_significance.status_field)
                or row.get("status")
                or ""
            ).strip()
            if not status:
                continue
            count = float(row.get("count") or 0)
            delta_ratio = row.get("delta_ratio")
            if status in statuses_needing_delta:
                if count < spec.change_significance.min_sentence_count:
                    violations.append(
                        f"{path}: {row.get('narrative_type')} count {count} below "
                        f"{spec.change_significance.min_sentence_count} for status {status}."
                    )
                if delta_ratio is None or abs(float(delta_ratio)) < spec.change_significance.min_relative_delta:
                    violations.append(
                        f"{path}: {row.get('narrative_type')} delta {delta_ratio} below "
                        f"{spec.change_significance.min_relative_delta:.2f}."
                    )
            elif status in new_statuses:
                if count < spec.change_significance.min_sentence_count:
                    violations.append(
                        f"{path}: {row.get('narrative_type')} marked NEW with only {count} sentences."
                    )
    if violations:
        raise SystemExit("Change export validation failed:\n- " + "\n- ".join(violations))


def _ensure_confidence_metrics(db_path: Path, spec: NarrativeKPISpec) -> None:
    if not db_path.exists():
        raise SystemExit(f"SQLite DB not found at {db_path}.")

    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute(
            """
            SELECT narrative_type, narrative_subtype, narrative_confidence
            FROM sentence_events
            WHERE narrative_type IS NOT NULL
              AND narrative_confidence IS NOT NULL
            """
        )
        rows = cursor.fetchall()
    finally:
        con.close()

    _assert(rows, "sentence_events table has no narrative confidence values; rerun label_sentence_events.py.")

    buckets: Dict[str, List[tuple[float, str | None]]] = {}
    for narrative_type, narrative_subtype, confidence in rows:
        if confidence is None:
            continue
        normalized_type = str(narrative_type).strip().lower()
        buckets.setdefault(normalized_type, []).append((float(confidence), narrative_subtype))

    violations: List[str] = []
    flag = (spec.confidence_breakdown.low_confidence_flag or "").strip().lower()

    for narrative_type, values in buckets.items():
        values.sort(key=lambda item: item[0], reverse=True)
        top_conf = values[0][0]
        if top_conf < spec.confidence_breakdown.min_sentence_confidence:
            violations.append(
                f"{narrative_type} top confidence {top_conf:.2f} below "
                f"{spec.confidence_breakdown.min_sentence_confidence:.2f}."
            )

        unique_confidences = []
        for confidence, _ in values:
            if not unique_confidences or confidence != unique_confidences[-1]:
                unique_confidences.append(confidence)
        if len(unique_confidences) > 1:
            gap = unique_confidences[0] - unique_confidences[1]
        else:
            gap = spec.confidence_breakdown.min_top_gap
        top_flagged = (
            values[0][1] and str(values[0][1]).strip().lower() == flag and bool(flag)
        )
        if gap < spec.confidence_breakdown.min_top_gap and not top_flagged:
            violations.append(
                f"{narrative_type} confidence gap {gap:.2f} below "
                f"{spec.confidence_breakdown.min_top_gap:.2f}."
            )

        if spec.confidence_breakdown.require_sum_to_one:
            for confidence, _sub in values:
                if confidence < -0.05 or confidence > 1.05:
                    violations.append(
                        f"{narrative_type} confidence {confidence:.2f} falls outside expected 0-1 range."
                    )
                    break

    if violations:
        raise SystemExit("Confidence validation failed:\n- " + "\n- ".join(violations))


def _ensure_competitive_reference(repo_root: Path, reference_path: str, sample_size: int) -> list[dict]:
    path = _resolve(repo_root, reference_path)
    if not path.exists():
        raise SystemExit(f"Competitive KPI reference file missing at {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, Sequence):
        raise SystemExit(f"Competitive KPI reference file at {path} must be a JSON array.")
    _assert(
        len(data) >= sample_size,
        f"Competitive KPI reference has {len(data)} entries; need at least {sample_size}.",
    )
    return list(data)


def _load_directional_metrics(metrics_dir: Path) -> Sequence[dict]:
    for name in ("directional_w.parquet", "directional_m.parquet"):
        path = metrics_dir / name
        if path.exists():
            return _read_table(path)
    raise SystemExit(
        f"No directional metrics found in {metrics_dir}. Expected directional_w.parquet or directional_m.parquet."
    )


def _extract_value(row: dict, keys: Iterable[str]) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return str(value)
    return None


def _ensure_competitive_accuracy(
    metrics_dir: Path,
    repo_root: Path,
    spec: NarrativeKPISpec,
) -> None:
    references = _ensure_competitive_reference(
        repo_root, spec.competitive_direction.reference_file, spec.competitive_direction.sample_size
    )
    metrics_rows = _load_directional_metrics(metrics_dir)

    matches = 0
    total = len(references)
    for entry in references:
        pair = entry.get("product_pair") or ""
        if " vs " not in pair:
            raise SystemExit(f"Invalid product_pair format in reference: {pair!r}")
        product, partner = [part.strip().lower() for part in pair.split(" vs ", 1)]
        expected_role = str(entry.get("expected_role") or "").strip().lower()
        if not expected_role:
            raise SystemExit(f"Reference entry for '{pair}' missing expected_role.")

        found = False
        for row in metrics_rows:
            row_product = (_extract_value(row, ("product", "product_a")) or "").strip().lower()
            row_partner = (_extract_value(row, ("partner", "product_b")) or "").strip().lower()
            row_role = (_extract_value(row, ("role", "product_role")) or "").strip().lower()
            if not row_product or not row_partner or not row_role:
                continue
            if row_product == product and row_partner == partner and row_role == expected_role:
                found = True
                break
        if found:
            matches += 1

    accuracy = matches / total if total else 0.0
    _assert(
        accuracy >= spec.competitive_direction.min_accuracy,
        (
            f"Directional role accuracy {accuracy:.2%} below "
            f"{spec.competitive_direction.min_accuracy:.0%}. "
            "Refresh metrics or update competitive_kpi.json."
        ),
    )


def _ensure_narrative_invariants(db_path: Path, spec: NarrativeKPISpec) -> None:
    if not db_path.exists():
        raise SystemExit(f"SQLite DB not found at {db_path}.")

    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute(
            """
            SELECT
                SUM(CASE WHEN narrative_type IS NOT NULL THEN 1 ELSE 0 END) AS total_rows,
                SUM(CASE WHEN narrative_type IS NOT NULL AND narrative_invariant_ok = 0 THEN 1 ELSE 0 END) AS failing_rows,
                SUM(CASE WHEN narrative_type IS NOT NULL AND narrative_invariant_ok IS NULL THEN 1 ELSE 0 END) AS missing_flags
            FROM sentence_events
            """
        )
        total_rows, failing_rows, missing_flags = cursor.fetchone()
    except sqlite3.OperationalError as exc:  # pragma: no cover - schema drift
        raise SystemExit(
            "sentence_events table is missing invariant tracking columns. "
            "Re-run label_sentence_events.py to refresh narratives."
        ) from exc
    finally:
        con.close()

    total_rows = total_rows or 0
    failing_rows = failing_rows or 0
    missing_flags = missing_flags or 0

    if missing_flags:
        raise SystemExit(
            f"{missing_flags} narrative rows missing invariant flags; rerun label_sentence_events.py."
        )
    if total_rows == 0:
        return
    pass_ratio = (total_rows - failing_rows) / total_rows
    _assert(
        pass_ratio >= spec.narrative_invariants.min_pass_rate,
        (
            f"Narrative invariant pass rate {pass_ratio:.2%} below "
            f"{spec.narrative_invariants.min_pass_rate:.0%}. "
            "Review sentence_events for misclassified narratives."
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data/powershell-checks"),
        help="Root directory for pipeline artifacts (default: data/powershell-checks).",
    )
    parser.add_argument(
        "--metrics-dir",
        type=Path,
        default=None,
        help="Optional override for the metrics directory (default: <data-root>/metrics).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Optional override for the ingestion SQLite DB (default: <data-root>/europepmc.sqlite).",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root (used for resolving reference fixtures).",
    )
    parser.add_argument(
        "--kpi-config",
        type=Path,
        default=Path("config/narratives_kpis.json"),
        help="Path to the KPI configuration JSON.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_root = args.data_root.resolve()
    metrics_dir = args.metrics_dir or (data_root / "metrics")
    db_path = Path(args.db) if args.db else (data_root / "europepmc.sqlite")
    repo_root = args.repo_root.resolve()
    kpi_spec = load_narrative_kpis(args.kpi_config)

    _ensure_label_precision_sample(data_root, kpi_spec.label_precision.sample_export, kpi_spec)
    _ensure_confidence_metrics(db_path, kpi_spec)
    _ensure_change_exports(data_root, kpi_spec.change_significance.export_glob, kpi_spec)
    _ensure_competitive_accuracy(metrics_dir, repo_root, kpi_spec)
    _ensure_narrative_invariants(db_path, kpi_spec)
    print("Narrative KPI checks passed.")


if __name__ == "__main__":
    main()
