"""Annotate sentence-level co-mentions with lightweight context labels."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Tuple

from src.analytics.context_labels import classify_sentence_context, labels_to_columns
from src.analytics.narratives import (
    NarrativeClassification,
    NarrativeValidation,
    ProductRoleContext,
    classify_directional_roles,
    classify_narrative,
    validate_narrative_event,
)
from src.analytics.sections import normalize_section
from src.storage import init_db, insert_sentence_events

DEFAULT_DB = Path("data/europepmc.sqlite")


Row = Tuple[str, str, str, str, str, str | None, str | None, str | None]


PAREN_PATTERN = re.compile(r"\([^()]{0,500}\)|\[[^\[\]]{0,500}\]")
BRACKET_CHUNK_PATTERN = re.compile(r"\([^()]*\)|\[[^\[\]]*\]|\{[^{}]*\}")
HEADING_SECTION_TERMS = {"introduction", "background", "objective", "objectives", "methods"}
OBJECTIVE_CUES = (
    "objective",
    "objectives",
    "aim",
    "aimed",
    "purpose",
    "this study evaluated",
    "this study investigated",
    "we sought",
    "we aimed",
)
DEFAULT_ALLOWED_SECTIONS = {
    "results",
    "result",
    "conclusion",
    "conclusions",
    "discussion",
}
SECTION_ALLOWLIST = {
    "comparative": {"results", "result", "conclusion", "conclusions", "discussion"},
    "safety": {"results", "result", "conclusion", "conclusions", "discussion"},
    "efficacy": {"results", "result", "conclusion", "conclusions", "discussion"},
    "concern": {"results", "result", "conclusion", "conclusions", "discussion"},
    "evidence": {"results", "result", "conclusion", "conclusions", "discussion"},
    "positioning": {"results", "result", "conclusion", "conclusions", "discussion", "introduction"},
    "access": {"results", "result", "conclusion", "conclusions", "discussion", "introduction"},
}
LIST_KEYWORDS = (
    "including",
    "consisting of",
    "comprised of",
    "such as",
    "like",
)
TABLE_PREFIXES = ("table", "figure", "supplementary table", "supplementary figure", "appendix")
COVARIATE_KEYWORDS = (
    "covariate",
    "covariates",
    "predictor",
    "predictors",
    "adjusted for",
    "baseline characteristics",
    "variables included",
    "controlled for",
    "model included",
    "eligibility criteria",
    "inclusion criteria",
    "exclusion criteria",
)
NUMERIC_ANCHOR_PATTERNS = [
    re.compile(r"\b\d+(?:\.\d+)?\s*(?:%|percent|patients|subjects|mmhg|g/l|ml|mg|events|cases)\b", flags=re.IGNORECASE),
    re.compile(r"\bp\s*[<=>]\s*0\.\d+", flags=re.IGNORECASE),
    re.compile(r"\b(?:hr|rr|or|ci)\b", flags=re.IGNORECASE),
]
STUDY_DESCRIPTION_CUES = (
    "we examined",
    "we evaluated",
    "we investigated",
    "we assessed",
    "this analysis examined",
    "this analysis evaluated",
    "this analysis investigated",
)
STUDY_RESULT_KEYWORDS = ("found", "observed", "showed", "demonstrated", "revealed")
ANALYSIS_CUES = (
    "were analysed",
    "was analysed",
    "were analyzed",
    "was analyzed",
    "analysis examined",
    "analysis assessed",
    "analysis evaluated",
    "analysis was performed",
    "analyses were performed",
    "were assessed",
    "was assessed",
    "were evaluated",
    "was evaluated",
    "logistic regression",
    "cox regression",
    "regression models",
    "multivariate analysis",
    "ordinal logistic regression",
    "post hoc analysis",
    "association between",
    "associations between",
    "association of",
    "associations of",
)
ANALYSIS_DIRECTION_HINTS = (
    "increase",
    "increased",
    "decrease",
    "decreased",
    "reduction",
    "reduced",
    "improved",
    "improvement",
    "higher",
    "lower",
    "greater",
    "less",
    "worse",
    "better",
)
METHOD_INTRO_CUES = (
    "in the ",
    "in this ",
    "in our ",
    "from randomized",
    "from randomised",
    "data from",
    "participants were randomized",
    "participants were randomised",
    "patients were randomized",
    "patients were randomised",
    "subjects were randomized",
    "subjects were randomised",
    "this trial",
    "this study",
    "pooled data",
    "we pooled",
)
RESULT_ASSERTION_TERMS = (
    "significant",
    "significantly",
    "difference",
    "reduction",
    "increase",
    "improvement",
    "reduced",
    "improved",
    "decreased",
    "greater",
    "lower",
    "higher",
    "hazard ratio",
    "relative risk",
    "odds ratio",
    "p<",
    "p=",
    "p >",
    "no difference",
)
ASSOCIATION_PREFIXES = (
    "associations between",
    "association between",
    "associations of",
    "association of",
)
UTILIZATION_KEYWORDS = (
    "prescription",
    "prescribed",
    "prescribing",
    "discharge",
    "uptake",
    "utilization",
    "utilisation",
    "use ",
    " use",
    "using",
    "usage",
)
UTILIZATION_EXEMPT_TERMS = ("discontinuation", "discontinued", "discontinue")
PATIENT_OUTCOME_KEYWORDS = (
    "mortality",
    "death",
    "hospitalization",
    "hospitalisation",
    "readmission",
    "event",
    "events",
    "adverse",
    "ae",
    "risk",
    "incidence",
    "arrhythmia",
    "outcome",
    "outcomes",
)
BASELINE_KEYWORDS = (
    "baseline",
    "characteristics",
    "demographics",
    "at entry",
    "at enrollment",
    "prior to treatment",
    "before treatment",
    "pre-treatment",
    "pre treatment",
    "at baseline",
)
PROTOCOL_CUES = (
    "randomized",
    "randomised",
    "assigned to",
    "allocated to",
    "allocation",
    "double-blind",
    "double blind",
    "single-blind",
    "open-label",
    "open label",
    "placebo-controlled",
    "parallel-group",
    "parallel group",
    "stratified",
    "titration phase",
)
RESULT_VERBS = (
    "reduced",
    "reduction",
    "lower",
    "higher",
    "improved",
    "worsened",
    "associated",
    "difference",
    "benefit",
    "decrease",
    "increase",
    "gained",
    "hazard",
    "risk",
    "odds",
    "rate",
)
UTILIZATION_OUTCOME_KEYWORDS = PATIENT_OUTCOME_KEYWORDS + (
    "adherence",
    "hospital days",
    "length of stay",
    "cost",
    "readmission",
    "mortality",
)
LIST_SAFETY_PHRASES = (
    "similar rates",
    "well tolerated",
    "no difference in adverse",
    "no significant difference",
    "safety profile",
    "adverse events included",
)


def _co_mentions_only_in_parentheses(
    text: str | None,
    product_a_alias: str | None,
    product_b_alias: str | None,
    product_a: str,
    product_b: str,
) -> bool:
    if not text:
        return False
    alias_a = (product_a_alias or product_a or "").strip().lower()
    alias_b = (product_b_alias or product_b or "").strip().lower()
    if not alias_a or not alias_b:
        return False
    normalized = text.lower()
    if alias_a not in normalized or alias_b not in normalized:
        return False
    stripped = PAREN_PATTERN.sub(" ", normalized)
    a_outside = alias_a in stripped
    b_outside = alias_b in stripped
    return not a_outside and not b_outside


def _bracket_ratio(text: str | None) -> float:
    if not text:
        return 0.0
    total = len(text)
    if not total:
        return 0.0
    bracket_chars = 0
    for match in BRACKET_CHUNK_PATTERN.finditer(text):
        bracket_chars += len(match.group(0))
    return min(1.0, bracket_chars / total) if bracket_chars else 0.0


def _has_numeric_anchor(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    for pattern in NUMERIC_ANCHOR_PATTERNS:
        if pattern.search(lowered):
            return True
    if " vs " in lowered or " versus " in lowered:
        return True
    return False


def _is_citation_only(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    if not lowered:
        return False
    stripped = re.sub(BRACKET_CHUNK_PATTERN, " ", lowered).strip()
    word_count = len(stripped.split())
    if "et al" in lowered and word_count <= 5:
        return True
    if re.search(r"\(\d{4}(?:[,;]\s*\d{4})*\)", lowered) and word_count <= 5:
        return True
    if re.fullmatch(r"\(?\d{4}(?:[,;]\s*\d{4})*\)?", lowered):
        return True
    if lowered.startswith("(") and lowered.endswith(")") and len(lowered.split()) <= 6:
        return True
    return False


def _is_definition_sentence(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    definition_cues = (
        " was defined as ",
        " were defined as ",
        " is defined as ",
        " refers to ",
        " defined as ",
        " was considered ",
        " were considered ",
    )
    if any(cue in lowered for cue in definition_cues):
        if not _has_numeric_anchor(text):
            return True
    if re.match(r"^[a-z0-9\s-]+\([a-z0-9\s-]+\)$", lowered.strip()):
        return True
    return False


def _is_table_figure_reference(text: str | None) -> bool:
    if not text:
        return False
    stripped = text.strip()
    lowered = stripped.lower()
    for prefix in TABLE_PREFIXES:
        if lowered.startswith(prefix):
            return True
    if "see table" in lowered or "see figure" in lowered or "shown in table" in lowered:
        return True
    return False


def _listiness_guard(text: str | None) -> tuple[bool, float]:
    """Return (drop_flag, penalty)."""
    if not text:
        return (False, 0.0)
    lowered = text.lower()
    comma_count = text.count(",")
    semicolon_count = text.count(";")
    colon_present = ":" in text
    keyword_hit = any(keyword in lowered for keyword in LIST_KEYWORDS)
    if (semicolon_count >= 2) or (keyword_hit and comma_count >= 2) or (colon_present and comma_count >= 2):
        if _has_numeric_anchor(text):
            return (False, 0.2)
        should_drop = True
        if any(phrase in lowered for phrase in LIST_SAFETY_PHRASES):
            should_drop = False
        return (should_drop, 0.0)
    if comma_count >= 5 and not _has_numeric_anchor(text):
        return (True, 0.0)
    return (False, 0.0)


def _is_covariate_statement(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if any(keyword in lowered for keyword in COVARIATE_KEYWORDS):
        if not _has_numeric_anchor(text):
            return True
    return False


def _looks_like_heading(text: str | None) -> bool:
    if not text:
        return False
    stripped = text.strip()
    if not stripped:
        return False
    tokens = stripped.split()
    alpha_chars = sum(1 for c in stripped if c.isalpha())
    uppercase_chars = sum(1 for c in stripped if c.isupper())
    if len(tokens) <= 6 and stripped.endswith(":"):
        return True
    if len(tokens) <= 6 and alpha_chars and uppercase_chars / alpha_chars > 0.7:
        return True
    if re.match(r"^(figure|table)\s+\d+", stripped, flags=re.IGNORECASE):
        return True
    if re.match(r"^\d+(?:\.\d+)*\s", stripped):
        return True
    if stripped.lower() in HEADING_SECTION_TERMS:
        return True
    return False


def _looks_like_objective(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    if not lowered:
        return False
    if not any(cue in lowered for cue in OBJECTIVE_CUES):
        return False
    if re.search(r"\d", lowered) or " vs " in lowered or " compared " in lowered:
        return False
    return True


def _looks_like_study_description(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    if not lowered:
        return False
    for cue in STUDY_DESCRIPTION_CUES:
        if lowered.startswith(cue):
            if any(marker in lowered for marker in STUDY_RESULT_KEYWORDS):
                return False
            if _has_numeric_anchor(text):
                return False
            return True
    return False


def _looks_like_analysis_sentence(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if not any(cue in lowered for cue in ANALYSIS_CUES):
        return False
    if _has_numeric_anchor(text):
        return False
    if any(term in lowered for term in ANALYSIS_DIRECTION_HINTS):
        return False
    return True


def _is_utilization_statement(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if not any(keyword in lowered for keyword in UTILIZATION_KEYWORDS):
        return False
    if any(term in lowered for term in UTILIZATION_EXEMPT_TERMS):
        return False
    if any(keyword in lowered for keyword in PATIENT_OUTCOME_KEYWORDS):
        return False
    if any(keyword in lowered for keyword in UTILIZATION_OUTCOME_KEYWORDS):
        return False
    return True


def _starts_with_method_clause(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    for cue in METHOD_INTRO_CUES:
        if lowered.startswith(cue):
            return True
    return False


def _has_result_clause_after_intro(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    break_index = -1
    for delimiter in (";", "."):
        idx = lowered.find(delimiter)
        if idx != -1:
            break_index = idx
            break
    if break_index == -1:
        for pattern in (", however", ", but", ", and"):
            idx = lowered.find(pattern)
            if idx != -1:
                break_index = idx + len(pattern)
                break
    if break_index == -1:
        colon_idx = lowered.find(":")
        if 0 < colon_idx <= 40:
            break_index = colon_idx
    if break_index == -1 or break_index >= len(lowered) - 1:
        return False
    trailing = lowered[break_index + 1 :].strip()
    if not trailing:
        return False
    if any(term in trailing for term in RESULT_ASSERTION_TERMS):
        return True
    if _has_numeric_anchor(trailing) and any(verb in trailing for verb in RESULT_VERBS):
        return True
    return False


def _is_baseline_descriptor(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if not any(keyword in lowered for keyword in BASELINE_KEYWORDS):
        pattern_hit = False
        if re.search(r"patients\s+on\s+[a-z0-9/\-+ ]+\s+had", lowered):
            pattern_hit = True
        if re.search(r"group[s]?\s+[a-z0-9/\-+ ]+\s+had", lowered):
            pattern_hit = True
        if not pattern_hit:
            return False
    if any(verb in lowered for verb in RESULT_VERBS):
        return False
    if any(keyword in lowered for keyword in PATIENT_OUTCOME_KEYWORDS):
        return False
    return True


def _is_association_only_sentence(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    for prefix in ASSOCIATION_PREFIXES:
        if lowered.startswith(prefix):
            if _has_result_clause_after_intro(text):
                return False
            return True
    return False


def _looks_like_protocol_sentence(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if not any(cue in lowered for cue in PROTOCOL_CUES):
        return False
    if _has_numeric_anchor(text):
        return False
    if any(verb in lowered for verb in RESULT_VERBS):
        return False
    return True


def _is_incidence_listing(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if "highest incidence" in lowered or "highest rate" in lowered:
        if "observed" in lowered:
            return True
    return False


def _section_allowed_for_narrative(section: str | None, narrative_type: str | None) -> bool:
    if not narrative_type:
        return True
    if not section:
        return False
    normalized = section.strip().lower()
    if not normalized:
        return False
    allowed = SECTION_ALLOWLIST.get(narrative_type.strip().lower(), DEFAULT_ALLOWED_SECTIONS)
    return normalized in allowed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the SQLite database populated by ingestion.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum number of sentence pairs to label (default: 5000).",
    )
    parser.add_argument(
        "--since-publication",
        type=str,
        help="Only process documents with publication_date on or after this ISO date.",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Skip pairs that already have sentence_events records.",
    )
    return parser.parse_args()


def fetch_pairs(conn, *, limit: int, since_publication: str | None, only_missing: bool) -> List[Row]:
    query = [
        """
        SELECT
            cms.doc_id,
            cms.sentence_id,
            cms.product_a,
            cms.product_b,
            s.text,
            (
                SELECT alias_matched
                FROM product_mentions pm
                WHERE pm.doc_id = cms.doc_id
                  AND pm.sentence_id = cms.sentence_id
                  AND lower(pm.product_canonical) = lower(cms.product_a)
                ORDER BY pm.start_char
                LIMIT 1
            ) AS product_a_alias,
            (
                SELECT alias_matched
                FROM product_mentions pm
                WHERE pm.doc_id = cms.doc_id
                  AND pm.sentence_id = cms.sentence_id
                  AND lower(pm.product_canonical) = lower(cms.product_b)
                ORDER BY pm.start_char
                LIMIT 1
            ) AS product_b_alias,
            s.section AS sentence_section
        FROM co_mentions_sentences cms
        JOIN sentences s ON cms.sentence_id = s.sentence_id
        JOIN documents d ON cms.doc_id = d.doc_id
        LEFT JOIN sentence_events se
          ON cms.doc_id = se.doc_id
         AND cms.sentence_id = se.sentence_id
         AND cms.product_a = se.product_a
         AND cms.product_b = se.product_b
        WHERE 1=1
        """
    ]
    params: list[object] = []

    if since_publication:
        query.append("AND d.publication_date >= ?")
        params.append(since_publication)

    if only_missing:
        query.append("AND se.doc_id IS NULL")

    query.append("ORDER BY d.publication_date DESC, cms.doc_id, cms.sentence_id LIMIT ?")
    params.append(limit)

    cur = conn.execute("\n".join(query), params)
    return [
        (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
        )
        for row in cur.fetchall()
    ]


def main() -> None:
    args = parse_args()

    conn = init_db(args.db)
    rows = fetch_pairs(
        conn,
        limit=args.limit,
        since_publication=args.since_publication,
        only_missing=args.only_missing,
    )

    if not rows:
        print("No sentence pairs found to label.")
        return

    events = []
    for (
        doc_id,
        sentence_id,
        product_a,
        product_b,
        text,
        product_a_alias,
        product_b_alias,
        raw_section,
    ) in rows:
        if _looks_like_heading(text):
            continue
        if _looks_like_objective(text):
            continue
        if _looks_like_study_description(text):
            continue
        canonical_section, processed_text, derived = normalize_section(raw_section, text)
        target_text = processed_text or text
        confidence_penalty = 0.0
        if _co_mentions_only_in_parentheses(
            target_text,
            product_a_alias,
            product_b_alias,
            product_a,
            product_b,
        ):
            continue
        bracket_ratio = _bracket_ratio(target_text)
        if bracket_ratio >= 0.6:
            continue
        if bracket_ratio >= 0.4:
            confidence_penalty = max(confidence_penalty, 0.2)
        if _is_citation_only(target_text):
            continue
        if _is_definition_sentence(target_text):
            continue
        if _is_table_figure_reference(target_text) and not _has_numeric_anchor(target_text):
            continue
        if _looks_like_analysis_sentence(target_text):
            continue
        utilization_only = False
        incidence_only = False
        if _is_utilization_statement(target_text):
            utilization_only = True
        if _is_baseline_descriptor(target_text):
            continue
        if _is_association_only_sentence(target_text):
            continue
        if _is_incidence_listing(target_text):
            incidence_only = True
        if _looks_like_protocol_sentence(target_text):
            continue
        if _starts_with_method_clause(target_text) and not _has_result_clause_after_intro(target_text):
            continue
        list_should_drop, list_penalty = _listiness_guard(target_text)
        if list_should_drop:
            continue
        if list_penalty:
            confidence_penalty = max(confidence_penalty, list_penalty)
        if _is_covariate_statement(target_text):
            continue
        labels = classify_sentence_context(target_text)
        direction = classify_directional_roles(
            target_text,
            ProductRoleContext(canonical=product_a, alias=product_a_alias),
            ProductRoleContext(canonical=product_b, alias=product_b_alias),
        )
        labels.direction_type = direction.direction_type
        labels.product_a_role = direction.product_a_role
        labels.product_b_role = direction.product_b_role
        labels.direction_triggers = list(direction.triggers or [])
        (
            comparative_terms,
            relationship_types,
            risk_terms,
            study_context,
            matched_terms,
            context_rule_hits,
            direction_type,
            product_a_role,
            product_b_role,
            direction_triggers,
        ) = labels_to_columns(labels)
        normalized_section = (canonical_section or "").lower()
        if utilization_only or incidence_only:
            narrative = NarrativeClassification("evidence", "real_world", 0.7)
        elif normalized_section and normalized_section in DEFAULT_ALLOWED_SECTIONS:
            narrative = classify_narrative(labels, section=canonical_section, text=target_text)
        else:
            narrative = NarrativeClassification(None, None, None)
        validation: NarrativeValidation = NarrativeValidation(ok=True, reason=None)
        invariant_ok: int | None = None
        invariant_reason: str | None = None
        if narrative.narrative_type:
            if _section_allowed_for_narrative(canonical_section, narrative.narrative_type):
                validation = validate_narrative_event(
                    narrative,
                    labels,
                    text=target_text,
                    section=canonical_section,
                )
                invariant_ok = 1 if validation.ok else 0
                invariant_reason = validation.reason
            else:
                narrative = NarrativeClassification(None, None, None)
        claim_strength = narrative.claim_strength
        risk_posture = narrative.risk_posture
        narrative_confidence = narrative.confidence
        if narrative_confidence is not None and confidence_penalty:
            narrative_confidence = max(0.0, narrative_confidence - confidence_penalty)
        events.append(
            (
                doc_id,
                sentence_id,
                product_a,
                product_b,
                comparative_terms,
                relationship_types,
                risk_terms,
                study_context,
                matched_terms,
                context_rule_hits,
                direction_type,
                product_a_role,
                product_b_role,
                direction_triggers,
                narrative.narrative_type,
                narrative.narrative_subtype,
                narrative_confidence,
                claim_strength,
                risk_posture,
                canonical_section,
                invariant_ok,
                invariant_reason,
            )
        )

    insert_sentence_events(conn, events)
    conn.commit()

    print(f"Labeled {len(events)} sentence co-mention pairs into sentence_events.")


if __name__ == "__main__":
    main()
