from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Set, Tuple

from .narrative_config import load_narrative_terms


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
    triggered_rules: List[str] = field(default_factory=list)


def classify_sentence_context(text: str) -> SentenceContextLabels:
    lower_text = text.lower()

    terms = load_narrative_terms()

    comparative_terms = _match_terms(lower_text, terms.comparative_terms)
    relationship_types, relationship_matches = _match_labeled_terms(
        lower_text, terms.relationship_patterns
    )
    risk_terms = _match_terms(lower_text, terms.risk_terms)
    study_context = _match_terms(lower_text, terms.study_context_terms)
    endpoint_terms = _match_terms(lower_text, terms.endpoint_terms)

    trial_phase_terms: Set[str] = set()
    for pattern in terms.trial_phase_patterns:
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

    triggered_rules: List[str] = []
    if comparative_terms:
        triggered_rules.append("comparative_terms")
    if relationship_types:
        triggered_rules.extend(f"relationship:{label}" for label in sorted(relationship_types))
    if risk_terms:
        triggered_rules.append("risk_terms")
    if study_context:
        triggered_rules.append("study_context")
    if endpoint_terms:
        triggered_rules.append("endpoint_terms")
    if trial_phase_terms:
        triggered_rules.append("trial_phase_terms")

    return SentenceContextLabels(
        comparative_terms=comparative_terms,
        relationship_types=relationship_types,
        risk_terms=risk_terms,
        study_context=study_context,
        trial_phase_terms=trial_phase_terms,
        endpoint_terms=endpoint_terms,
        matched_terms=matched_terms,
        triggered_rules=triggered_rules,
    )


def labels_to_columns(labels: SentenceContextLabels) -> tuple[str | None, ...]:
    def _join(items: Set[str]) -> str | None:
        return ", ".join(sorted(items)) if items else None

    matched = json.dumps(labels.matched_terms) if labels.matched_terms else None
    triggered = json.dumps(labels.triggered_rules) if labels.triggered_rules else None
    return (
        _join(labels.comparative_terms),
        _join(labels.relationship_types),
        _join(labels.risk_terms),
        _join(labels.study_context),
        matched,
        triggered,
    )
