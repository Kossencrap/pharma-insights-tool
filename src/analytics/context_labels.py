from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Set, Tuple


COMPARATIVE_TERMS: Tuple[str, ...] = (
    "vs",
    "versus",
    "compared with",
    "compared to",
    "superior",
    "non-inferior",
    "noninferior",
    "better than",
    "worse than",
    "similar to",
    "equivalent to",
    "outperformed",
    "inferior to",
    "non inferior",
)

RELATIONSHIP_PATTERNS: Mapping[str, Tuple[str, ...]] = {
    "combination": (
        "and",
        "plus",
        "combined with",
        "co-administered",
        "coadministered",
        "co-administration",
        "coadministration",
        "in combination with",
    ),
    "delivery": (
        "delivered via",
        "administered via",
        "delivery",
        "device",
        "pen",
        "formulation",
    ),
    "switching": (
        "switched to",
        "switching to",
        "converted to",
        "transitioned to",
    ),
    "add-on therapy": (
        "add-on",
        "add on",
        "added to",
        "add-on therapy",
    ),
}

RISK_TERMS: Tuple[str, ...] = (
    "risk",
    "adverse",
    "adverse event",
    "side effect",
    "side effects",
    "safety",
    "tolerability",
    "hypoglycemia",
    "toxicity",
    "serious",
    "fatal",
)

STUDY_CONTEXT_TERMS: Tuple[str, ...] = (
    "trial",
    "randomized",
    "randomised",
    "meta-analysis",
    "systematic review",
    "review",
    "mouse",
    "mice",
    "in vitro",
    "phase",
    "cohort",
    "enrollment",
    "recruitment",
)

TRIAL_PHASE_PATTERNS: Tuple[str, ...] = (
    r"phase\s*[1-4](?:/[1-4])?[ab]?",
    r"phase\s*i{1,3}v?[ab]?",
    r"phase-?[1-4][ab]?",
)

ENDPOINT_TERMS: Tuple[str, ...] = (
    "primary endpoint",
    "secondary endpoint",
    "overall survival",
    "progression-free survival",
    "pfs",
    "os",
    "response rate",
    "objective response",
    "hba1c",
    "a1c",
    "body weight",
    "weight loss",
    "blood pressure",
)

ADVERSE_EVENT_TERMS: Tuple[str, ...] = (
    "adverse event",
    "adverse events",
    "serious adverse event",
    "serious adverse events",
    "grade 3",
    "grade 4",
    "ae",
    "aes",
)


def _match_terms(text: str, terms: Iterable[str]) -> Set[str]:
    matches: Set[str] = set()
    for term in terms:
        pattern = re.compile(rf"\b{re.escape(term)}\b", flags=re.IGNORECASE)
        if pattern.search(text):
            matches.add(term)
    return matches


def _match_labeled_terms(
    text: str, patterns: Mapping[str, Iterable[str]]
) -> tuple[Set[str], Dict[str, List[str]]]:
    labels: Set[str] = set()
    matched: Dict[str, List[str]] = {}
    for label, terms in patterns.items():
        hits = sorted(_match_terms(text, terms))
        if hits:
            labels.add(label)
            matched[label] = hits
    return labels, matched


@dataclass
class SentenceContextLabels:
    comparative_terms: Set[str] = field(default_factory=set)
    relationship_types: Set[str] = field(default_factory=set)
    risk_terms: Set[str] = field(default_factory=set)
    study_context: Set[str] = field(default_factory=set)
    trial_phase_terms: Set[str] = field(default_factory=set)
    endpoint_terms: Set[str] = field(default_factory=set)
    matched_terms: Dict[str, List[str]] = field(default_factory=dict)


def classify_sentence_context(text: str) -> SentenceContextLabels:
    lower_text = text.lower()

    comparative_terms = _match_terms(lower_text, COMPARATIVE_TERMS)
    relationship_types, relationship_matches = _match_labeled_terms(
        lower_text, RELATIONSHIP_PATTERNS
    )
    risk_terms = _match_terms(lower_text, RISK_TERMS + ADVERSE_EVENT_TERMS)
    study_context = _match_terms(lower_text, STUDY_CONTEXT_TERMS)
    endpoint_terms = _match_terms(lower_text, ENDPOINT_TERMS)

    trial_phase_terms: Set[str] = set()
    for pattern in TRIAL_PHASE_PATTERNS:
        compiled = re.compile(pattern, flags=re.IGNORECASE)
        for match in compiled.finditer(lower_text):
            trial_phase_terms.add(match.group(0))
    if trial_phase_terms:
        study_context |= trial_phase_terms

    matched_terms: Dict[str, List[str]] = {}
    if comparative_terms:
        matched_terms["comparative_terms"] = sorted(comparative_terms)
    matched_terms.update(relationship_matches)
    if risk_terms:
        matched_terms["risk_terms"] = sorted(risk_terms)
    if study_context:
        matched_terms["study_context"] = sorted(study_context)
    if endpoint_terms:
        matched_terms["endpoint_terms"] = sorted(endpoint_terms)
    if trial_phase_terms:
        matched_terms["trial_phase_terms"] = sorted(trial_phase_terms)

    return SentenceContextLabels(
        comparative_terms=comparative_terms,
        relationship_types=relationship_types,
        risk_terms=risk_terms,
        study_context=study_context,
        trial_phase_terms=trial_phase_terms,
        endpoint_terms=endpoint_terms,
        matched_terms=matched_terms,
    )


def labels_to_columns(labels: SentenceContextLabels) -> tuple[str | None, ...]:
    def _join(items: Set[str]) -> str | None:
        return ", ".join(sorted(items)) if items else None

    matched = json.dumps(labels.matched_terms) if labels.matched_terms else None
    return (
        _join(labels.comparative_terms),
        _join(labels.relationship_types),
        _join(labels.risk_terms),
        _join(labels.study_context),
        matched,
    )
