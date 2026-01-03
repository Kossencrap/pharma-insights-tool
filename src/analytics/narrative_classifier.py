from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from src.analytics.context_labels import SentenceContextLabels


@dataclass(frozen=True)
class NarrativePrediction:
    narrative_type: Optional[str]
    narrative_subtype: Optional[str]
    confidence: Optional[float]

    def to_tuple(self) -> Tuple[Optional[str], Optional[str], Optional[float]]:
        return self.narrative_type, self.narrative_subtype, self.confidence


def _confidence_from_signals(*signals: int) -> float:
    strength = sum(signals)
    return min(1.0, 0.6 + 0.1 * strength)


def classify_narrative(
    text: str, labels: SentenceContextLabels
) -> NarrativePrediction:
    """Assign a deterministic narrative based on simple keyword groupings.

    The classifier intentionally relies on interpretable cues so that
    downstream exports can point back to specific terms and sections.
    """

    risk_signal = 1 if labels.risk_terms else 0
    comparative_signal = 1 if labels.comparative_terms or labels.relationship_types else 0
    trial_signal = 1 if labels.trial_phase_terms or labels.study_context else 0
    endpoint_signal = 1 if labels.endpoint_terms else 0

    if risk_signal:
        return NarrativePrediction(
            narrative_type="safety",
            narrative_subtype="risk_signal",
            confidence=_confidence_from_signals(risk_signal, trial_signal),
        )

    if comparative_signal:
        subtype = "head_to_head" if labels.comparative_terms else "treatment_strategy"
        return NarrativePrediction(
            narrative_type="comparative_positioning",
            narrative_subtype=subtype,
            confidence=_confidence_from_signals(comparative_signal, endpoint_signal),
        )

    if endpoint_signal:
        return NarrativePrediction(
            narrative_type="efficacy",
            narrative_subtype="clinical_endpoint",
            confidence=_confidence_from_signals(endpoint_signal, trial_signal),
        )

    if trial_signal:
        subtype = "trial_design" if labels.trial_phase_terms else "study_setting"
        return NarrativePrediction(
            narrative_type="study_context",
            narrative_subtype=subtype,
            confidence=_confidence_from_signals(trial_signal),
        )

    return NarrativePrediction(None, None, None)
