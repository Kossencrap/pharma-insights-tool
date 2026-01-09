from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Set, Tuple

from .context_labels import SentenceContextLabels
from .narrative_config import (
    DirectionalPattern,
    NarrativeRule,
    load_directional_patterns,
    load_narrative_rules,
)
from .sentiment import SentimentLabel


@dataclass(frozen=True)
class NarrativeClassification:
    narrative_type: str | None
    narrative_subtype: str | None
    confidence: float | None
    risk_posture: Optional[str] = None
    claim_strength: Optional[str] = None


@dataclass(frozen=True)
class ProductRoleContext:
    canonical: str
    alias: Optional[str] = None


@dataclass(frozen=True)
class DirectionalClassification:
    direction_type: Optional[str]
    product_a_role: Optional[str]
    product_b_role: Optional[str]
    triggers: Tuple[str, ...] = ()


@dataclass(frozen=True)
class NarrativeValidation:
    ok: bool
    reason: Optional[str] = None


SAFETY_INDICATORS = {"safety", "tolerability", "adverse"}
COMBINATION_TYPES = {"combination", "add-on therapy"}
DELIVERY_TYPES = {"delivery"}
SWITCHING_TYPES = {"switching"}
SAFETY_POSTURE_SUBTYPES = {
    "reassurance": "safety_reassurance",
    "minimization": "safety_minimization",
    "acknowledgment": "safety_acknowledgment",
}
CLAIM_STRENGTH_PRIORITY = ("confirmatory", "suggestive", "exploratory")
COMPARATIVE_LEXICAL_ANCHORS = {
    " vs ",
    " versus ",
    " compared with ",
    " compared to ",
    " relative to ",
    " better than ",
    " worse than ",
    " non-inferior",
    " noninferior",
    " non inferior",
    " superior to ",
    " inferior to ",
    " head-to-head ",
    " head to head ",
    " less often than ",
    " more often than ",
}
COMPARATIVE_REGEXES = [
    re.compile(r"\b(?:rr|or|hr|arr|nnt|nnh)\b\s*[:=]?\s*\d", flags=re.IGNORECASE),
    re.compile(r"\bp\s*[<=>]\s*0\.\d+", flags=re.IGNORECASE),
    re.compile(r"\bconfidence interval\b", flags=re.IGNORECASE),
    re.compile(r"\b(?:less|more|higher|lower)\s+[a-z0-9\- ]{1,40}\s+than\b", flags=re.IGNORECASE),
    re.compile(r"\bnot\s+significantly\s+[a-z0-9\- ]{1,40}\s+than\b", flags=re.IGNORECASE),
]
QUALITATIVE_COMPARATIVE_PATTERNS = [
    re.compile(
        r"\bwas\s+(?:more|less|equally|as|similarly)\s+(?:effective|efficacious|beneficial|safe|tolerated|advantageous)[^.;]{0,60}\bthan\b",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\bwas\s+(?:superior|inferior)\s+to\b", flags=re.IGNORECASE),
    re.compile(r"\bwas\s+(?:similar|comparable)\s+to\b", flags=re.IGNORECASE),
    re.compile(r"\bwas\s+non[-\s]?inferior\s+to\b", flags=re.IGNORECASE),
    re.compile(r"\bwas\s+as\s+(?:effective|beneficial|safe)\s+as\b", flags=re.IGNORECASE),
]
GROUP_CONTRAST_TERMS = (" group", " arm", " cohort", " patients receiving ", " patients treated with ")
CONTRAST_OPERATORS = {
    " than ",
    " compared ",
    " versus ",
    " vs ",
    " relative ",
    " difference",
    " higher ",
    " lower ",
    " similar ",
    " non-inferior",
    " noninferior",
    " non inferior",
    " parity",
    " equal ",
}
EFFICACY_ENDPOINT_HINTS = {
    "mortality",
    "survival",
    "progression",
    "response",
    "remission",
    "hospitalization",
    "nt-probnp",
    "lvef",
    "blood pressure",
    "hba1c",
    "readmission",
    "symptom",
    "qaly",
    "endpoint",
    "primary outcome",
    "win ratio",
    "end-diastolic diameter",
    "left ventricular end-diastolic diameter",
    "relative wall thickness",
    "myocardial energy expenditure",
    "mee",
    "global wasted work",
    "gww",
    "global work efficiency",
    "gwe",
    "left atrial volume",
    "lv-indexed volume",
    "cardiopulmonary function",
    "myocardial injury",
    "quality of life",
    "systolic blood pressure",
    "sbp",
    "hemoglobin",
    "haemoglobin",
    "ucgmp",
    "bnp",
    "atrial fibrillation",
    "af risk",
    "af occurrence",
    "egfr",
    "composite end point",
}
SAFETY_ASSERTION_PATTERNS = [
    re.compile(r"well[-\s]?tolerated", flags=re.IGNORECASE),
    re.compile(r"no (?:significant )?(?:difference|increase) in (?:adverse|ae)", flags=re.IGNORECASE),
    re.compile(r"(?:incidence|rate|risk) of [^.;]+adverse", flags=re.IGNORECASE),
    re.compile(r"(?:serious )?adverse events? (?:were|was|remained)", flags=re.IGNORECASE),
    re.compile(r"tolerability (?:was|remained)", flags=re.IGNORECASE),
    re.compile(r"discontinuation due to", flags=re.IGNORECASE),
    re.compile(
        r"(?:risk|incidence|rate|probability)\s+of\s+[^.;]{0,80}\b(?:was|were|is|are|remained|reduced|decreased|increased|higher|lower|unchanged)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"(?:risk|incidence|rate|hazard)\s+(?:remained|was|were|stayed|became)\s+(?:similar|unchanged|lower|higher|reduced|increased)",
        flags=re.IGNORECASE,
    ),
    re.compile(r"was associated with (?:reduced|increased) [^.;]{0,60}risk", flags=re.IGNORECASE),
    re.compile(r"(?:hazard ratio|odds ratio|relative risk)\s*[:=]?\s*\d", flags=re.IGNORECASE),
    re.compile(r"(?:safety|adverse|tolerability) outcomes? (?:did|did not)? differ", flags=re.IGNORECASE),
    re.compile(r"no (?:new|unexpected) safety signals?", flags=re.IGNORECASE),
    re.compile(r"no (?:safety )?concerns?(?: were)?(?: identified| observed| reported)", flags=re.IGNORECASE),
    re.compile(r"adverse events? leading to discontinuation", flags=re.IGNORECASE),
    re.compile(r"treatment[-\s]?emergent adverse events?", flags=re.IGNORECASE),
    re.compile(r"grade\s*(?:3|4|iii|iv)[^.;]{0,20}(?:toxicit|adverse)", flags=re.IGNORECASE),
    re.compile(r"serious adverse events? (?:occurred|occur|occurring)", flags=re.IGNORECASE),
]
NON_CLAIM_KEYWORDS = {
    "registry",
    "baseline characteristics",
    "predictor",
    "predictors",
    "covariate",
    "covariates",
    "variable",
    "variables",
    "adjusted for",
    "model included",
    "nct",
    "clinicaltrials.gov",
    "icd-",
    "icd10",
    "icd-10",
    "cohort description",
    "eligibility criteria",
    "protocol",
    "enrolled",
    "prospective registry",
}
LIST_STRUCTURE_PATTERN = re.compile(r":\s*[^:]{0,80},\s*[^:]{0,80},\s*[^:]{0,80}")
POSITIVE_DIRECTION_TERMS = {
    "superior",
    "better",
    "reduced",
    "reduction",
    "improved",
    "greater",
    "higher",
    "lower",
    "decreased",
    "improvement",
    "gain",
}
NEGATIVE_DIRECTION_TERMS = {
    "worse",
    "inferior",
    "increased",
    "increase",
    "higher risk",
    "elevated",
    "deteriorated",
    "decline",
}
EQUIVALENCE_TERMS = {
    "non-inferior",
    "noninferior",
    "similar",
    "no significant difference",
    "comparable",
    "equivalent",
}
OUTCOME_KEYWORDS = {
    "mortality",
    "death",
    "outcome",
    "outcomes",
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
    "quality of life",
    "qol",
    "nt-probnp",
    "lvef",
    "symptom",
    "progression",
    "remission",
    "survival",
}
BASELINE_KEYWORDS = {
    "baseline",
    "characteristics",
    "demographics",
    "at entry",
    "at enrollment",
    "at enrolment",
    "prior to treatment",
    "before treatment",
    "pre-treatment",
    "pre treatment",
    "at baseline",
}
OUTCOME_VERBS = {
    "reduced",
    "reduction",
    "lower",
    "higher",
    "improved",
    "worsened",
    "associated",
    "difference",
    "benefit",
    "increase",
    "decrease",
    "gain",
    "occurred",
    "occur",
    "led to",
    "resulted",
    "hazard",
    "risk",
}


def _pick_subtype(options: Iterable[str]) -> str | None:
    for option in options:
        return option
    return None


def _normalize_terms(items: Iterable[str]) -> Set[str]:
    return {item.strip().lower() for item in items if item}


def _sentiment_aliases(label: Optional[str]) -> Set[str]:
    if not label:
        return set()
    normalized = label.strip().lower()
    mapping = {
        SentimentLabel.POSITIVE.value.lower(): {"pos", "positive"},
        SentimentLabel.NEGATIVE.value.lower(): {"neg", "negative"},
        SentimentLabel.NEUTRAL.value.lower(): {"neu", "neutral"},
    }
    return mapping.get(normalized, {normalized})


def _candidate_tokens(context: ProductRoleContext) -> Tuple[str, ...]:
    tokens = []
    if context.alias:
        tokens.append(context.alias)
    tokens.append(context.canonical)
    normalized = []
    seen = set()
    for token in tokens:
        key = token.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return tuple(normalized)


def _collect_spans(text_lower: str, tokens: Tuple[str, ...]) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    for token in tokens:
        pattern = re.compile(re.escape(token), flags=re.IGNORECASE)
        for match in pattern.finditer(text_lower):
            spans.append(match.span())
    spans.sort()
    return spans


def _nearest_before(spans: dict[str, list[tuple[int, int]]], index: int) -> Optional[str]:
    best_id: Optional[str] = None
    best_pos: Optional[int] = None
    for product_id, product_spans in spans.items():
        for _, end in product_spans:
            if end <= index and (best_pos is None or end > best_pos):
                best_pos = end
                best_id = product_id
    return best_id


def _nearest_after(spans: dict[str, list[tuple[int, int]]], index: int) -> Optional[str]:
    best_id: Optional[str] = None
    best_pos: Optional[int] = None
    for product_id, product_spans in spans.items():
        for start, _ in product_spans:
            if start >= index and (best_pos is None or start < best_pos):
                best_pos = start
                best_id = product_id
    return best_id


def _phrase_regex(phrase: str) -> re.Pattern[str]:
    escaped = re.escape(phrase.strip())
    pattern_text = re.sub(r"\\\s+", r"\\s+", escaped)
    return re.compile(pattern_text, flags=re.IGNORECASE)


def _resolve_roles(
    pattern: DirectionalPattern,
    subject_product: Optional[str],
    object_product: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    a_role: Optional[str] = None
    b_role: Optional[str] = None
    if subject_product == "A":
        a_role = pattern.subject_role
    elif subject_product == "B":
        b_role = pattern.subject_role

    if object_product == "A":
        a_role = pattern.object_role
    elif object_product == "B":
        b_role = pattern.object_role

    # If only one side is set but we have two products, infer the opposite when roles differ.
    if a_role is None and b_role is not None and pattern.object_role and pattern.subject_role:
        a_role = pattern.subject_role if object_product == "B" else pattern.object_role
    if b_role is None and a_role is not None and pattern.object_role and pattern.subject_role:
        b_role = pattern.subject_role if object_product == "A" else pattern.object_role

    return a_role, b_role


def _has_group_contrast(text_lower: str) -> bool:
    count = 0
    for term in GROUP_CONTRAST_TERMS:
        if term in text_lower:
            count += 1
    if count >= 2 and _has_contrast_operator(text_lower):
        return True
    return False


def _has_contrast_operator(text_lower: str) -> bool:
    if not text_lower:
        return False
    return any(op in text_lower for op in CONTRAST_OPERATORS)


def _has_comparative_anchor(
    text_lower: str, labels: Optional[SentenceContextLabels]
) -> bool:
    if labels:
        if labels.comparative_terms:
            return True
        if getattr(labels, "direction_type", None):
            return True
    if not text_lower:
        return False
    for phrase in COMPARATIVE_LEXICAL_ANCHORS:
        if phrase in text_lower:
            return True
    for pattern in COMPARATIVE_REGEXES:
        if pattern.search(text_lower):
            return True
    for pattern in QUALITATIVE_COMPARATIVE_PATTERNS:
        if pattern.search(text_lower):
            return True
    return _has_group_contrast(text_lower)


def _has_efficacy_signal(
    labels: SentenceContextLabels, text_lower: str
) -> bool:
    if labels.endpoint_terms:
        return True
    return any(term in text_lower for term in EFFICACY_ENDPOINT_HINTS)


def _has_safety_signal(labels: SentenceContextLabels) -> bool:
    return bool(labels.risk_terms)


def _has_safety_assertion(text_lower: str) -> bool:
    if not text_lower:
        return False
    for pattern in SAFETY_ASSERTION_PATTERNS:
        if pattern.search(text_lower):
            return True
    return False


def _is_non_claim_context(text_lower: str) -> bool:
    if not text_lower:
        return False
    clauses = re.split(r"[.;]", text_lower)
    leading_clause = clauses[0]
    trailing_clause = clauses[1] if len(clauses) > 1 else ""
    def _has_non_claim(clause: str) -> bool:
        return any(keyword in clause for keyword in NON_CLAIM_KEYWORDS)
    if _has_non_claim(leading_clause):
        trailing_window = " ".join(trailing_clause.strip().split()[:18])
        if trailing_window and _has_safety_assertion(trailing_window):
            return False
        if trailing_window and _has_non_claim(trailing_window):
            return True
        if not _has_safety_assertion(text_lower):
            return True
    if LIST_STRUCTURE_PATTERN.search(text_lower):
        return True
    return False


def _has_directional_positive(text_lower: str) -> bool:
    if not text_lower:
        return False
    return any(term in text_lower for term in POSITIVE_DIRECTION_TERMS)


def _has_directional_negative(text_lower: str) -> bool:
    if not text_lower:
        return False
    return any(term in text_lower for term in NEGATIVE_DIRECTION_TERMS)


def _has_equivalence_signal(text_lower: str) -> bool:
    if not text_lower:
        return False
    return any(term in text_lower for term in EQUIVALENCE_TERMS)


def _has_outcome_signal(labels: SentenceContextLabels, text_lower: str) -> bool:
    if labels.endpoint_terms or labels.risk_terms:
        return True
    if not text_lower:
        return False
    return any(term in text_lower for term in OUTCOME_KEYWORDS)


def _looks_like_baseline_context(text_lower: str) -> bool:
    if not text_lower:
        return False
    if not any(keyword in text_lower for keyword in BASELINE_KEYWORDS):
        return False
    if any(verb in text_lower for verb in OUTCOME_VERBS):
        return False
    if any(term in text_lower for term in OUTCOME_KEYWORDS):
        return False
    return True


def _contains_guideline_positioning_cue(text_lower: str) -> bool:
    if not text_lower:
        return False
    return any(cue in text_lower for cue in POSITIONING_CUES)


def _rule_matches(
    rule: NarrativeRule,
    labels: SentenceContextLabels,
    sentiment_label: Optional[str],
    section: Optional[str],
) -> bool:
    for field, expected in rule.requires.items():
        required_values = _normalize_terms(expected)
        label_values = getattr(labels, field, None)
        if not isinstance(label_values, Iterable):
            return False
        normalized = _normalize_terms(label_values)
        if not normalized:
            return False
        if not required_values:
            continue
        if "*" in required_values:
            continue
        if not any(
            requirement in value
            for requirement in required_values
            for value in normalized
        ):
            return False

    if rule.requires_sentiment:
        sentiment_aliases = {alias.lower() for alias in rule.requires_sentiment}
        provided = _sentiment_aliases(sentiment_label)
        if not provided or provided.isdisjoint(sentiment_aliases):
            return False

    normalized_section = section.strip().lower() if section else None
    if rule.include_sections:
        if not normalized_section or normalized_section not in rule.include_sections:
            return False
    if rule.exclude_sections and normalized_section in rule.exclude_sections:
        return False

    return True


def classify_directional_roles(
    text: str,
    product_a: ProductRoleContext,
    product_b: ProductRoleContext,
    *,
    patterns: Sequence[DirectionalPattern] | None = None,
) -> DirectionalClassification:
    compiled = tuple(patterns) if patterns is not None else load_directional_patterns()
    if not compiled:
        return DirectionalClassification(None, None, None)

    text_lower = text.lower()
    spans = {
        "A": _collect_spans(text_lower, _candidate_tokens(product_a)),
        "B": _collect_spans(text_lower, _candidate_tokens(product_b)),
    }

    for pattern in compiled:
        if not pattern.phrases:
            continue
        match_type = (pattern.match_type or "between").lower()
        for phrase in pattern.phrases:
            regex = _phrase_regex(phrase)
            for match in regex.finditer(text_lower):
                subject_id: Optional[str] = None
                object_id: Optional[str] = None

                if match_type == "between":
                    subject_id = _nearest_before(spans, match.start())
                    object_id = _nearest_after(spans, match.end())
                elif match_type == "after":
                    object_id = _nearest_after(spans, match.end())
                    if object_id:
                        subject_id = "A" if object_id == "B" else "B"
                elif match_type == "before":
                    subject_id = _nearest_before(spans, match.start())
                    if subject_id:
                        object_id = "A" if subject_id == "B" else "B"
                else:
                    subject_id = _nearest_before(spans, match.start())
                    object_id = _nearest_after(spans, match.end())

                if not subject_id and not object_id:
                    continue

                a_role, b_role = _resolve_roles(pattern, subject_id, object_id)
                if a_role or b_role:
                    return DirectionalClassification(
                        direction_type=pattern.direction_type,
                        product_a_role=a_role,
                        product_b_role=b_role,
                        triggers=(pattern.name,),
                    )

    return DirectionalClassification(None, None, None)


def _infer_risk_posture(
    labels: SentenceContextLabels, section: Optional[str]
) -> Optional[str]:
    normalized_section = section.strip().lower() if section else None
    if normalized_section == "methods":
        return None
    if not labels.risk_terms:
        return None
    for posture in ("reassurance", "minimization"):
        if posture in labels.risk_posture_labels:
            return posture
    return "acknowledgment"


def _infer_claim_strength(
    labels: SentenceContextLabels, sentiment_label: Optional[str]
) -> Optional[str]:
    for tier in CLAIM_STRENGTH_PRIORITY:
        if tier in labels.claim_strength_labels:
            return tier

    normalized_context = {term.lower() for term in labels.study_context}
    normalized_endpoints = {term.lower() for term in labels.endpoint_terms}

    if any("phase iii" in term or "randomized" in term for term in normalized_context):
        return "confirmatory"
    if any("primary" in term for term in normalized_endpoints):
        return "confirmatory"
    if any("phase ii" in term or "phase 2" in term for term in normalized_context):
        return "suggestive"
    if any("secondary" in term for term in normalized_endpoints):
        return "suggestive"
    if sentiment_label and (labels.comparative_terms or labels.relationship_types):
        return "exploratory"
    return None


def _legacy_classification(
    labels: SentenceContextLabels,
    sentiment_label: Optional[str],
    section: Optional[str],
    text: Optional[str] = None,
) -> NarrativeClassification:
    normalized_section = section.strip().lower() if section else None
    methods_section = normalized_section == "methods"
    claim_strength = _infer_claim_strength(labels, sentiment_label)
    if not claim_strength and sentiment_label and has_anchor:
        claim_strength = "exploratory"
    text_lower = text.lower() if text else ""
    has_anchor = _has_comparative_anchor(text_lower, labels) if text_lower else False

    if labels.risk_terms and not methods_section and not has_anchor:
        posture = _infer_risk_posture(labels, section)
        subtype = SAFETY_POSTURE_SUBTYPES.get(posture, "safety_reassurance")
        if not text_lower or (
            not _is_non_claim_context(text_lower) and _has_safety_assertion(text_lower)
        ):
            return NarrativeClassification(
                "safety", subtype, 0.9, risk_posture=posture, claim_strength=claim_strength
            )

    can_be_comparative = bool(labels.comparative_terms) or has_anchor
    if can_be_comparative and not methods_section:
        subtype = "comparative_efficacy"
        sentiment_map = {
            SentimentLabel.POSITIVE.value: "comparative_efficacy_advantage",
            SentimentLabel.NEGATIVE.value: "comparative_efficacy_disadvantage",
        }
        if sentiment_label in sentiment_map:
            subtype = sentiment_map[sentiment_label]
        if not text_lower or (
            _has_comparative_anchor(text_lower, labels)
            and _has_efficacy_signal(labels, text_lower)
        ):
            return NarrativeClassification("comparative", subtype, 0.8, claim_strength=claim_strength)

    if labels.relationship_types and not methods_section and not has_anchor:
        normalized = {item.lower() for item in labels.relationship_types}
        if normalized & COMBINATION_TYPES:
            if text_lower and not (_has_outcome_signal(labels, text_lower) or _contains_guideline_positioning_cue(text_lower)):
                pass
            else:
                return NarrativeClassification("positioning", "combination", 0.85, claim_strength=claim_strength)
        if normalized & SWITCHING_TYPES:
            return NarrativeClassification("positioning", "switching", 0.8, claim_strength=claim_strength)
        if normalized & DELIVERY_TYPES:
            return NarrativeClassification("positioning", "delivery", 0.75, claim_strength=claim_strength)

    if labels.study_context:
        subtype = "clinical_context"
        phase_hits = [
            term for term in labels.study_context if term.lower().startswith("phase")
        ]
        if phase_hits:
            subtype = "clinical_trial"
        elif any("review" in term.lower() for term in labels.study_context):
            subtype = "evidence_review"
        return NarrativeClassification("evidence", subtype, 0.7, claim_strength=claim_strength)

    if sentiment_label and not methods_section:
        sentiment_map = {
            SentimentLabel.POSITIVE.value: ("efficacy", "positive_signal", 0.6),
            SentimentLabel.NEGATIVE.value: ("concern", "negative_signal", 0.6),
        }
        resolved = sentiment_map.get(sentiment_label)
        if resolved:
            return NarrativeClassification(*resolved, claim_strength=claim_strength)

    return NarrativeClassification(None, None, None, claim_strength=claim_strength)


def classify_narrative(
    labels: SentenceContextLabels,
    sentiment_label: Optional[str] = None,
    *,
    section: Optional[str] = None,
    rules: Sequence[NarrativeRule] | None = None,
    text: Optional[str] = None,
) -> NarrativeClassification:
    """
    Deterministically map context labels (plus optional sentiment) into a narrative bucket.
    """
    compiled_rules = tuple(rules) if rules is not None else load_narrative_rules()
    text_lower = text.lower() if text else ""

    def _looks_like_method_sentence() -> bool:
        method_cues = (
            "was analysed",
            "were analysed",
            "analysis examined",
            "were assessed",
            "was assessed",
            "patients were randomized",
            "post hoc analysis",
            "shap analysis",
            "lasso regression",
            "multivariate logistic regression",
            "was defined as",
            "were defined",
            "study was",
            "this study",
        )
        return any(cue in text_lower for cue in method_cues)

    for rule in compiled_rules:
        if _rule_matches(rule, labels, sentiment_label, section):
            if text and not labels.comparative_terms and rule.narrative_type == "comparative" and _looks_like_method_sentence():
                continue
            if text_lower:
                subtype = (rule.narrative_subtype or "").lower()
                if rule.narrative_type == "comparative":
                    if not _has_comparative_anchor(text_lower, labels):
                        continue
                    if "efficacy" in subtype and not _has_efficacy_signal(labels, text_lower):
                        continue
                    if "safety" in subtype:
                        if not _has_safety_signal(labels):
                            continue
                        if not _has_safety_assertion(text_lower):
                            continue
                    if "advantage" in subtype and not _has_directional_positive(text_lower):
                        continue
                    if "disadvantage" in subtype and not _has_directional_negative(text_lower):
                        continue
                    if ("neutral" in subtype or "parity" in subtype) and not _has_equivalence_signal(text_lower):
                        continue
                if rule.narrative_type == "safety":
                    if _has_comparative_anchor(text_lower, labels):
                        continue
                    if _is_non_claim_context(text_lower):
                        continue
                    if not _has_safety_signal(labels):
                        continue
                    if not _has_safety_assertion(text_lower):
                        continue
                if rule.narrative_type == "positioning":
                    if _has_comparative_anchor(text_lower, labels):
                        continue
                    subtype = (rule.narrative_subtype or "").lower()
                    if "combination" in subtype:
                        if not (_has_outcome_signal(labels, text_lower) or _contains_guideline_positioning_cue(text_lower)):
                            continue
            claim_strength = _infer_claim_strength(labels, sentiment_label)
            risk_posture = (
                _infer_risk_posture(labels, section)
                if rule.narrative_type == "safety"
                else None
            )
            subtype = rule.narrative_subtype
            if rule.narrative_type == "safety" and risk_posture:
                subtype = SAFETY_POSTURE_SUBTYPES.get(risk_posture, subtype)
            return NarrativeClassification(
                rule.narrative_type,
                subtype,
                rule.confidence,
                risk_posture=risk_posture,
                claim_strength=claim_strength,
            )

    return _legacy_classification(labels, sentiment_label, section, text)


def validate_narrative_event(
    classification: NarrativeClassification,
    labels: SentenceContextLabels,
    *,
    text: Optional[str],
    section: Optional[str] = None,
) -> NarrativeValidation:
    if not classification.narrative_type:
        return NarrativeValidation(ok=True, reason=None)

    text_lower = text.lower() if text else ""
    token_count = len(text.split()) if text else 0
    if classification.narrative_type in {"comparative", "safety"}:
        if token_count and token_count < 8:
            return NarrativeValidation(False, "text_too_short")
        if (
            text_lower
            and ("table" in text_lower or "figure" in text_lower or "supplementary" in text_lower)
            and not any(char.isdigit() for char in text_lower)
        ):
            return NarrativeValidation(False, "table_reference_without_data")
    if classification.narrative_type in {"comparative", "safety"}:
        if text_lower and _looks_like_baseline_context(text_lower):
            return NarrativeValidation(False, "baseline_context")
    subtype = (classification.narrative_subtype or "").lower()
    if classification.narrative_type == "comparative":
        if not _has_comparative_anchor(text_lower, labels):
            return NarrativeValidation(False, "missing_comparative_anchor")
        if "efficacy" in subtype and not _has_efficacy_signal(labels, text_lower):
            return NarrativeValidation(False, "missing_efficacy_endpoint")
        if "safety" in subtype and not _has_safety_signal(labels):
            return NarrativeValidation(False, "missing_safety_anchor")
        if "safety" in subtype and not _has_safety_assertion(text_lower):
            return NarrativeValidation(False, "missing_safety_assertion")
        if "advantage" in subtype and not _has_directional_positive(text_lower):
            return NarrativeValidation(False, "missing_directional_positive")
        if "disadvantage" in subtype and not _has_directional_negative(text_lower):
            return NarrativeValidation(False, "missing_directional_negative")
        if ("neutral" in subtype or "parity" in subtype) and not _has_equivalence_signal(text_lower):
            return NarrativeValidation(False, "missing_equivalence_signal")
    elif classification.narrative_type == "safety":
        if _is_non_claim_context(text_lower):
            return NarrativeValidation(False, "safety_non_claim_context")
        if not _has_safety_signal(labels):
            return NarrativeValidation(False, "missing_safety_anchor")
        if not _has_safety_assertion(text_lower):
            return NarrativeValidation(False, "missing_safety_assertion")
    return NarrativeValidation(True, None)
POSITIONING_CUES = {
    "recommended",
    "recommend",
    "first-line",
    "first line",
    "frontline",
    "foundation",
    "foundational",
    "guideline",
    "standard of care",
    "preferred",
    "indicated",
    "cornerstone",
}
