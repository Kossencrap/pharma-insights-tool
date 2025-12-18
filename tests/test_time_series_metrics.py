from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.analytics.time_series import TimeSeriesConfig, add_change_metrics, bucket_counts


def test_weekly_wow_change_detects_spike(execution_log):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    weekly_counts = [2, 2, 2, 10]
    rows = []

    for week, count in enumerate(weekly_counts):
        for i in range(count):
            rows.append(
                {
                    "publication_date": start + timedelta(weeks=week, days=i % 3),
                    "product_canonical": "drugA",
                }
            )

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq="W",
        group_columns=["product_canonical"],
    )
    agg = bucket_counts(config, rows)
    metrics = add_change_metrics(agg, group_columns=["product_canonical"], window=3)

    last_row = metrics[-1]
    assert last_row["count"] == 10
    assert last_row["wow_change"] == (10 - 2) / 2
    execution_log.record(
        "Time-series WoW",
        "drugA weekly counts [2,2,2,10] flagged with +400% week-over-week change",
    )


def test_z_score_flags_large_jump(execution_log):
    start = datetime(2024, 2, 5, tzinfo=timezone.utc)
    weekly_counts = [5, 6, 7, 20]
    rows = []

    for week, count in enumerate(weekly_counts):
        for i in range(count):
            rows.append(
                {
                    "publication_date": start + timedelta(weeks=week, days=i % 2),
                    "product_canonical": "drugB",
                }
            )

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq="W",
        group_columns=["product_canonical"],
    )
    agg = bucket_counts(config, rows)
    metrics = add_change_metrics(agg, group_columns=["product_canonical"], window=3)

    z_score = metrics[-1]["z_score"]
    assert z_score > 10  # Rolling baseline std from first 3 weeks is small
    execution_log.record(
        "Time-series z-score",
        "drugB weekly jump to 20 produces z-score spike above threshold",
    )
