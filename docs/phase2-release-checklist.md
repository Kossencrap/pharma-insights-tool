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

## Guardrailed Phase 2 workflow

1. **Ingest and label with the co-mention guardrail**
   - Set the environment flag once per PowerShell session so every downstream script keeps the strict filtering enabled (remember to clear it before running pytest or other non-guardrail workflows):
     ```powershell
     $env:PHARMA_REQUIRE_COMENTIONS = 1
     ```
   - Run the Phase 1 pipeline wrapper (5,000 doc cap) to regenerate Phase 2 artifacts in-place:
     ```powershell
     py scripts/run_phase1_pipeline.py `
         --products config/products.json `
         --from-date 2022-01-01 `
         --max-records 5000 `
         --db data/europepmc.sqlite `
         --manifest-path data/artifacts/phase1/phase1_run_manifest.json
     ```
2. **Export the canonical labeled events snapshot for reviewers**
   ```powershell
   py scripts/export_sentence_events_jsonl.py `
       --db data/europepmc.sqlite `
       --output data/processed/latest_sentence_events.jsonl
   ```
3. **Generate KPI reviewer samples + unlabeled audit CSVs**
   ```powershell
   py scripts/export_label_precision_sample.py `
       --db data/europepmc.sqlite `
       --output data/artifacts/kpi/narratives_label_kpi.csv `
       --require-subtypes comparative_safety

   py scripts/export_unlabeled_sentences.py `
       --db data/europepmc.sqlite `
       --output data/artifacts/kpi/narratives_unlabeled.csv
   ```
   Capture reviewer outcomes directly in `narratives_label_kpi.csv` before moving on.
4. **Validate KPI thresholds against production artifacts**
   ```powershell
   py scripts/check_narrative_kpis.py `
       --db data/europepmc.sqlite `
       --data-root . `
       --metrics-dir data/artifacts/phase1/metrics `
       --kpi-config config/narratives_kpis.json
   ```
   Record the KPI config hash alongside the manifest:
   ```powershell
   Get-FileHash config/narratives_kpis.json -Algorithm SHA256
   ```

## Release checklist

1. Run `powershell/functional_checks.ps1` end-to-end with the guardrail flag set (temporarily set `$env:PHARMA_REQUIRE_COMENTIONS=1` before the ingestion steps, then clear it before running pytest or other default-mode workflows).
2. Execute the guardrailed workflow above (ingestion/pipeline, `latest_sentence_events.jsonl` export, reviewer sample, unlabeled audit).
3. Ensure `scripts/export_label_precision_sample.py` (`--require-subtypes comparative_safety`) is re-run after ingestion and reviewer outcomes are logged in `data/artifacts/kpi/narratives_label_kpi.csv`.
4. Confirm `scripts/export_unlabeled_sentences.py` refreshed `data/artifacts/kpi/narratives_unlabeled.csv`.
5. Run `scripts/check_narrative_kpis.py --db data/europepmc.sqlite --data-root . --metrics-dir data/artifacts/phase1/metrics --kpi-config config/narratives_kpis.json` and capture the `Get-FileHash config/narratives_kpis.json -Algorithm SHA256` output.
6. Review Streamlit narrative cards + evidence views with UX/MA sign-off (document reviewer, date, and dashboard state).
7. Attach the following to the release manifest: `data/processed/latest_sentence_events.jsonl`, `data/artifacts/kpi/narratives_label_kpi.csv`, `data/artifacts/kpi/narratives_unlabeled.csv`, the latest change exports under `data/artifacts/phase1/metrics/`, Streamlit review notes, and the KPI config hash from step 5. Use the helper below to copy everything into a single bundle:
   ```powershell
   py scripts/package_phase2_release.py `
       --manifest data/artifacts/phase1/phase1_run_manifest.json `
       --output data/releases/run_20260108
   ```
   The script mirrors the repo-relative structure, copies all referenced artifacts (manifest, latest sentence events, KPI CSVs, change exports, KPI config), and emits `release_artifacts.json` with SHA-256 hashes for audit.
   Generate a Markdown summary for the release ticket:
   ```powershell
   py scripts/generate_release_notes.py `
       --bundle data/releases/run_20260108 `
       --output data/releases/run_20260108/release_notes.md
   ```

Release is blocked until every item above is checked and the gating scripts pass.
