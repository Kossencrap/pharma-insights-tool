from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import sqrt
from typing import Dict, Iterable, List, Optional, Sequence


@dataclass
class TimeSeriesConfig:
    """Configuration for time-based aggregation."""

    timestamp_column: str
    freq: str = "W"  # Supported: "W" (week starting Monday) or "M" (month)
    group_columns: Optional[Sequence[str]] = None
    value_column: Optional[str] = None
    sum_value: bool = False


def _parse_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        ts = value
    else:
        ts = datetime.fromisoformat(str(value))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _bucket_start(ts: datetime, freq: str) -> datetime:
    if freq == "M":
        return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if freq != "W":
        raise ValueError(f"Unsupported freq '{freq}'. Use 'W' or 'M'.")

    monday = ts - timedelta(days=ts.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)


def bucket_counts(config: TimeSeriesConfig, rows: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    """Aggregate rows into time buckets."""

    group_cols = list(config.group_columns or [])
    counts: Dict[tuple, float] = {}

    for row in rows:
        ts_raw = row.get(config.timestamp_column)
        if ts_raw is None:
            continue
        ts = _parse_timestamp(ts_raw)
        bucket = _bucket_start(ts, config.freq)
        group_key = tuple(row.get(col) for col in group_cols)
        key = (*group_key, bucket)

        increment = float(row.get(config.value_column, 1)) if config.sum_value else 1.0
        counts[key] = counts.get(key, 0.0) + increment

    results: List[Dict[str, object]] = []
    for key, count in counts.items():
        entry = {col: key[i] for i, col in enumerate(group_cols)}
        entry["bucket_start"] = key[len(group_cols)]
        entry["count"] = count
        results.append(entry)

    sort_keys = group_cols + ["bucket_start"]
    results.sort(key=lambda r: tuple(r.get(k) for k in sort_keys))
    return results


def _rolling_stats(values: List[float], window: int) -> tuple[Optional[float], Optional[float]]:
    if len(values) < window:
        return None, None

    mean = sum(values[-window:]) / window
    variance = sum((v - mean) ** 2 for v in values[-window:]) / window
    std = sqrt(variance)
    return mean, std


def add_change_metrics(
    agg_rows: List[Dict[str, object]],
    value_column: str = "count",
    group_columns: Optional[Sequence[str]] = None,
    window: int = 4,
) -> List[Dict[str, object]]:
    """Add percent change and z-score columns to aggregated rows."""

    group_cols = list(group_columns or [])
    results: List[Dict[str, object]] = []

    # Group rows by key
    groups: Dict[tuple, List[Dict[str, object]]] = {}
    for row in agg_rows:
        key = tuple(row.get(col) for col in group_cols)
        groups.setdefault(key, []).append(row)

    for key, rows in groups.items():
        rows.sort(key=lambda r: r["bucket_start"])
        history: List[float] = []
        prev: Optional[float] = None

        for row in rows:
            value = float(row.get(value_column, 0))
            wow_change = None
            if prev not in (None, 0):
                wow_change = (value - prev) / prev

            mean, std = _rolling_stats(history, window)
            z_score = None
            if mean is not None and std and std > 0:
                z_score = (value - mean) / std

            enriched = dict(row)
            enriched["wow_change"] = wow_change
            enriched["z_score"] = z_score
            results.append(enriched)

            history.append(value)
            prev = value

    sort_keys = group_cols + ["bucket_start"]
    results.sort(key=lambda r: tuple(r.get(k) for k in sort_keys))
    return results


def add_sentiment_ratios(
    agg_rows: List[Dict[str, object]],
    *,
    label_column: str = "sentiment_label",
    group_columns: Optional[Sequence[str]] = None,
) -> List[Dict[str, object]]:
    group_cols = list(group_columns or [])
    totals: Dict[tuple, float] = {}

    for row in agg_rows:
        bucket = row.get("bucket_start")
        key = (*tuple(row.get(col) for col in group_cols), bucket)
        totals[key] = totals.get(key, 0.0) + float(row.get("count", 0) or 0)

    enriched: List[Dict[str, object]] = []
    for row in agg_rows:
        bucket = row.get("bucket_start")
        key = (*tuple(row.get(col) for col in group_cols), bucket)
        total = totals.get(key) or 0.0
        ratio = (float(row.get("count", 0) or 0) / total) if total else None
        updated = dict(row)
        updated["ratio"] = ratio
        enriched.append(updated)

    sort_keys = group_cols + [label_column, "bucket_start"]
    enriched.sort(key=lambda r: tuple(r.get(k) for k in sort_keys))
    return enriched


def sentiment_bucket_counts(
    rows: Iterable[Dict[str, object]],
    *,
    timestamp_column: str = "date",
    label_column: str = "sentiment_label",
    freq: str = "W",
    group_columns: Optional[Sequence[str]] = None,
) -> List[Dict[str, object]]:
    group_cols = list(group_columns or []) + [label_column]
    config = TimeSeriesConfig(
        timestamp_column=timestamp_column,
        freq=freq,
        group_columns=group_cols,
    )
    agg = bucket_counts(config, rows)
    return add_sentiment_ratios(agg, label_column=label_column, group_columns=group_columns)
