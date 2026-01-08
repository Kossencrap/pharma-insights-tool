# Phase 2 – Narrative Structure & Change Attribution

_Status checkpoint: 2026-01-05 — Narrative labeling, directional roles, change exports, and dashboard cards are implemented; pending work centers on KPI validation, functional-check coverage, and doc/UX sign-off as described below._

## One-sentence definition
Phase 2 organizes Phase-1 signals into explicit narratives and records what content changes, with sentence-level evidence.

## Why Phase 2
- Phase 1 already yields reliable signals (volume, sentiment, co-mentions) with full traceability.
- Phase 2 adds a semantic layer: grouping signals, attaching interpretable labels, and comparing them over time so reviewers see *what* changed, not just *that* something changed.
- The approach remains deterministic, explainable, and auditable.

## Core capabilities
1. **Narrative typing**
   - Every sentence gets a narrative label (e.g., efficacy framing, safety reassurance, line-of-therapy positioning, comparative claims, real-world vs. trial).
   - Labels are deterministically or lightly classifier-assisted, with every decision pointing back to the source text.
   - Output is labeled evidence sentences with metadata, not generated prose.
2. **Change attribution**
   - Narrative-level time series track new/persistent/fading status (e.g., “appeared after date X,” “safety framing +40%”).
   - Before/after contrasts per narrative with thresholds for a “significant shift.”
   - Visible in dashboards and exports, always linked to source evidence.
3. **Competitive narrative context**
   - Co-mentions are semantically classified (alternative, add-on, inferior, equivalent).
   - Directional sentiment asymmetry per product pair and narrative type.
   - Produces interpretable, citable comparisons for CI/launch teams.
   - Directional heuristics now mark each product as *favored/disfavored*, *add-on/backbone*, or *switch-source/destination*, and the dashboard surfaces partner-aware trend charts plus sentence-level evidence filtered by these roles.
4. **Reviewer-grade evidence views**
   - “Narrative cards” show the label, confidence breakdown, top evidence sentences, and change indicator (↑/↓/new).
   - Cards integrate with existing dashboard/CLI and are exportable as JSON/CSV with the manifest.

## Narrative schema configuration
- The deterministic taxonomy that powers the capabilities above lives in `config/narratives.json`.
- The file contains both the term lists (comparative, risk, relationship, etc.) used during context parsing and the ordered narrative rules with priorities, sentiment constraints, and confidence scores.
- Updating the JSON is the single supported way to add/retire narratives in Phase 2; scripts like `label_sentence_events.py` and both dashboards automatically pick up changes after re-running the pipeline.
- Tests cover the schema loader plus representative rules so the taxonomy stays auditable.
- Safety narratives now emit explicit *risk posture* metadata (`acknowledgment`, `reassurance`, or `minimization`) so reviewers can distinguish cautious reporting from risk erosion. The KPI sample and parquet exports expose these as `risk_posture` and subtype-specific counts (for example, `safety_minimization`).
- Every labeled sentence also carries a lightweight `claim_strength` tier (`exploratory`, `suggestive`, `confirmatory`) derived from lexical/statistical cues and study context. This allows dashboards to compare raw narrative volume versus evidence-weighted or claim-strength-filtered views without changing the deterministic pipeline.
- Aggregations now ship three additional exports per run: `narratives_weighted_{w|m}.parquet` (study-type weighted counts), `narratives_dimensions_{w|m}.parquet` (narrative × posture × claim strength grids), and `risk_signals_{w|m}.parquet` (per-pair safety/concern ratios) so omission or dilution trends can be monitored alongside raw change status.

## Explicitly out of scope
- Clinical decision support, outcome prediction, or causal claims.
- Generative summaries without direct sentence references.
- Black-box NLP; everything stays reproducible and explainable.
- BERT (or other models) can refine labels only; no automatic sentence selection, summarization, or interpretation.

## Acceptance criteria (“Phase 2 done”)
1. At least three narrative types are auto-labeled for one product (and are auditable).
2. Each narrative shows sentence-level evidence plus confidence breakdown.
3. The change view marks new, disappearing, and significantly shifting narratives.
4. Competitive narratives are directional and explainable with evidence sentences.
5. The pipeline remains fully deterministic with manifest + artifacts + tests aligned to Phase 1 guardrails.

## Strategic rationale
- Raises decision relevance by surfacing *what* changed without eroding trust.
- Maintains compliance by sticking to citable sentences and reproducible logic.
- Increases defensibility: rivals may have metrics, but not verified narratives with change attribution.
- Fits the Phase 1 infrastructure (manifest, dashboards, CLI), keeping implementation risk low.

## Next steps for the backlog and guardrails
1. Reflect this definition in backlog epics and technical spikes (narrative labels, BERT checkpoints), plus UI stories for narrative cards.
2. Extend `functional_checks`/pipeline coverage to include narrative labeling + change attribution so Phase 2 has the same one-command readiness as Phase 1; the `narratives_change_{freq}.parquet` export and dashboard change view are now live, so wire them into the checks.
3. Define measurable KPIs per capability (label precision, minimum delta for “significant shift,” etc.) to make the acceptance criteria objective.
4. The canonical list of Phase 2 epics, KPI governance actions, and the release checklist now lives in `docs/phase2-release-checklist.md`; consult it before approving a deploy tag.

## KPI definitions & thresholds
`config/narratives_kpis.json` is now the single source of truth for the Phase 2 KPI thresholds. The file keeps the values deterministic (and testable) while letting the CLI/dashboard checks consume the same spec.

1. **Narrative label precision** — Sample at least 30 labeled sentences per run (more for high-risk types). Require ≥0.90 precision overall and ≥0.95 for `safety` (additional high-risk types can be listed in the config). The sampled review log is exported to `artifacts/kpi/narratives_label_kpi.csv` so the manifest can link to the audit trail.
2. **Confidence breakdown sanity** — Sentence-level narrative cards must have a ≥0.20 delta between the top confidence sentence and the second tier unless they are explicitly flagged as `low_confidence`. Functional checks should fail when the top confidence drops below 0.60 or when the weight columns stop summing to ~1.0 (tolerance stays within the deterministic pipeline).
3. **Change significance threshold** — Change exports only emit ↑/↓/new statuses in `narratives_change_{freq}.parquet` when both a ≥20% relative shift and ≥10 supporting sentences exist for that narrative. Exports without enough sample count keep the `steady` status to avoid false positives.
4. **Competitive direction accuracy** — At least 40 curated co-mention comparisons are reviewed per release via `tests/data/competitive_kpi.json`, and directional heuristics (`favored`, `disfavored`, `add_on`, etc.) must match the reference ≥85% of the time before a deploy tag is blessed.

These KPIs map directly to the acceptance criteria: label/evidence quality, confidence transparency, change attribution rigor, and competitive explainability. When CI wires the config into `functional_checks`, Phase 2 moves from “implemented” to “validated.”

`powershell/functional_checks.ps1` now runs `scripts/check_narrative_kpis.py` so every one-command run enforces the thresholds above (label sample size, minimum confidence, change export presence, and curated competitive references) using the single source of truth in `config/narratives_kpis.json`.
