# Phase 2 – Narrative Structure & Change Attribution

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
4. **Reviewer-grade evidence views**
   - “Narrative cards” show the label, confidence breakdown, top evidence sentences, and change indicator (↑/↓/new).
   - Cards integrate with existing dashboard/CLI and are exportable as JSON/CSV with the manifest.

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
2. Extend `functional_checks`/pipeline coverage to include narrative labeling + change attribution so Phase 2 has the same one-command readiness as Phase 1.
3. Define measurable KPIs per capability (label precision, minimum delta for “significant shift,” etc.) to make the acceptance criteria objective.
