# Phase 2 Backlog & Release Checklist

This document links the Phase 2 definition (docs/phase2-narrative.md) to concrete
delivery epics, KPI governance, and the one-command checks that gate a release.

## Canonical epics and acceptance tests

1. **Narrative taxonomy & labeling**
   - Scope: deterministic schema updates in `config/narratives.json`, BERT fine-tune
     spikes, and reviewer workflow for `artifacts/kpi/narratives_label_kpi.csv`.
   - Acceptance: `powershell/functional_checks.ps1` ➝ `Assert-Phase2Metrics`
     verifies ≥3 narrative labels exist in SQLite; `Run-NarrativeKpiChecks` enforces
     KPI precision thresholds.
2. **Deterministic change attribution**
   - Scope: keeps `narratives_change_{w,m}.parquet` exports in sync with
     KPI sampling scripts plus change-status heuristics.
   - Acceptance: `scripts/check_narrative_kpis.py` validates change exports obey
     the config in `config/narratives_kpis.json` and the resulting cards expose
     sentence-level evidence.
3. **Narrative cards & reviewer UX**
   - Scope: dashboard/CLI evidence cards (directional roles, KPI surfaces) and UX
     validation for reviewers.
   - Acceptance: runbook requires a UI walkthrough of the Streamlit dashboard plus
     a reviewer sign-off linked to the KPI artifacts.

Each epic stays open until its linked gate (CLI/PowerShell) passes on the
latest artifacts.

## KPI governance

- `config/narratives_kpis.json` is the single source of truth.
- Manual review sample is regenerated with `scripts/export_label_precision_sample.py`
  (see command below). The same CSV feeds `scripts/check_narrative_kpis.py`.
- Competitive heuristics are curated in `tests/data/competitive_kpi.json`.
- Updates to any of the above require documenting the change in the PR description
  and referencing this checklist.

### Manual review command

```powershell
py scripts/export_label_precision_sample.py --db data/powershell-checks/europepmc.sqlite `
    --output data/powershell-checks/artifacts/kpi/narratives_label_kpi.csv
```

The script enforces stratified sampling (covers every high-risk narrative) and
pre-populates review columns so auditors can append `is_correct` results.

## Release checklist

1. Run `powershell/functional_checks.ps1` end-to-end with production-like data.
2. Re-run `scripts/export_label_precision_sample.py` and capture review outcomes in
   `artifacts/kpi/narratives_label_kpi.csv`.
3. Confirm `scripts/check_narrative_kpis.py` passes (precision, confidence deltas,
   change thresholds, directional accuracy).
4. Review Streamlit narrative cards + evidence views with UX/MA sign-off.
5. Attach the KPI CSV, change exports, and Streamlit review notes to the release
   artifact/manifest and note the config hash from `config/narratives_kpis.json`.

Release is blocked until every item above is checked and the gating scripts pass.
