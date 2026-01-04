from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from typing import Iterable, Optional, Sequence, Set

from .context_labels import SentenceContextLabels
from .narrative_config import NarrativeRule, load_narrative_rules
from .sentiment import SentimentLabel


@dataclass(frozen=True)
class NarrativeClassification:
    narrative_type: str | None
    narrative_subtype: str | None
    confidence: float | None


SAFETY_INDICATORS = {"safety", "tolerability", "adverse"}
COMBINATION_TYPES = {"combination", "add-on therapy"}
DELIVERY_TYPES = {"delivery"}
SWITCHING_TYPES = {"switching"}


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


def _rule_matches(
    rule: NarrativeRule,
    labels: SentenceContextLabels,
    sentiment_label: Optional[str],
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

    return True


def _legacy_classification(
    labels: SentenceContextLabels, sentiment_label: Optional[str]
) -> NarrativeClassification:
    if labels.risk_terms:
        subtype = "risk_signal"
        normalized_terms = {term.lower() for term in labels.risk_terms}
        if any(
            indicator in term for term in normalized_terms for indicator in SAFETY_INDICATORS
        ):
            subtype = "safety_reassurance"
        return NarrativeClassification("safety", subtype, 0.9)

    if labels.relationship_types:
        normalized = {item.lower() for item in labels.relationship_types}
        if normalized & COMBINATION_TYPES:
            return NarrativeClassification("positioning", "combination", 0.85)
        if normalized & SWITCHING_TYPES:
            return NarrativeClassification("positioning", "switching", 0.8)
        if normalized & DELIVERY_TYPES:
            return NarrativeClassification("positioning", "delivery", 0.75)

    if labels.comparative_terms:
        subtype = "comparative"
        sentiment_map = {
            SentimentLabel.POSITIVE.value: "comparative_advantage",
            SentimentLabel.NEGATIVE.value: "comparative_disadvantage",
        }
        if sentiment_label in sentiment_map:
            subtype = sentiment_map[sentiment_label]
        return NarrativeClassification("comparative", subtype, 0.8)

    if labels.study_context:
        subtype = "clinical_context"
        phase_hits = [
            term for term in labels.study_context if term.lower().startswith("phase")
        ]
        if phase_hits:
            subtype = "clinical_trial"
        elif any("review" in term.lower() for term in labels.study_context):
            subtype = "evidence_review"
        return NarrativeClassification("evidence", subtype, 0.7)

    if sentiment_label:
        sentiment_map = {
            SentimentLabel.POSITIVE.value: ("efficacy", "positive_signal", 0.6),
            SentimentLabel.NEGATIVE.value: ("concern", "negative_signal", 0.6),
        }
        resolved = sentiment_map.get(sentiment_label)
        if resolved:
            return NarrativeClassification(*resolved)

    return NarrativeClassification(None, None, None)


def classify_narrative(
    labels: SentenceContextLabels,
    sentiment_label: Optional[str] = None,
    rules: Sequence[NarrativeRule] | None = None,
) -> NarrativeClassification:
    """
    Deterministically map context labels (plus optional sentiment) into a narrative bucket.
    """
    compiled_rules = tuple(rules) if rules is not None else load_narrative_rules()
    for rule in compiled_rules:
        if _rule_matches(rule, labels, sentiment_label):
            return NarrativeClassification(
                rule.narrative_type, rule.narrative_subtype, rule.confidence
            )

    return _legacy_classification(labels, sentiment_label)
