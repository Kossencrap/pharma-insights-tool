from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from math import exp, log
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

DEFAULT_HALF_LIFE_DAYS = 365

# Maps common publication type strings to canonical study types.
STUDY_TYPE_ALIASES: dict[str, str] = {
    "randomised controlled trial": "randomized controlled trial",
    "controlled clinical trial": "clinical trial",
    "clinical trial": "clinical trial",
    "case report": "case report",
    "case reports": "case report",
    "review": "review",
    "systematic review": "review",
    "meta-analysis": "review",
    "observational study": "observational study",
    "cohort study": "observational study",
    "case-control study": "observational study",
}


@dataclass(frozen=True)
class DocumentWeight:
    doc_id: str
    recency_weight: float
    study_type: str | None
    study_type_weight: float | None

    @property
    def combined_weight(self) -> float:
        base = self.recency_weight
        if self.study_type_weight is None:
            return base
        return base * self.study_type_weight


def compute_recency_weight(
    publication_date: date | None,
    *,
    reference_date: date | None = None,
    half_life_days: int = DEFAULT_HALF_LIFE_DAYS,
) -> float:
    """Compute an exponential-decay recency score.

    A publication on the reference date yields a score of 1.0; a publication at
    the half-life yields roughly 0.5. Dates older than the reference degrade
    smoothly, while missing dates receive a score of 0.0.
    """

    if publication_date is None:
        return 0.0

    ref = reference_date or date.today()
    age_days = (ref - publication_date).days
    if age_days <= 0:
        return 1.0

    decay_constant = log(2) / half_life_days
    return exp(-decay_constant * age_days)


def _coerce_pub_types(values: Any) -> list[str]:
    if isinstance(values, str):
        return [values]
    if isinstance(values, Iterable):
        coerced: list[str] = []
        for value in values:
            if isinstance(value, str):
                coerced.append(value)
        return coerced
    return []


def extract_publication_types(raw_metadata: Mapping[str, Any]) -> list[str]:
    """Extract study design/publication type labels from raw metadata."""

    pub_types: list[str] = []
    pub_type_list = raw_metadata.get("pubTypeList")
    if isinstance(pub_type_list, Mapping):
        pub_types.extend(_coerce_pub_types(pub_type_list.get("pubType")))
    elif pub_type_list is not None:
        pub_types.extend(_coerce_pub_types(pub_type_list))

    for key in ("publicationType", "pubType", "studyDesign", "study_type"):
        if key in raw_metadata:
            pub_types.extend(_coerce_pub_types(raw_metadata[key]))

    return pub_types


def load_study_type_weights(path: Path | str) -> dict[str, float]:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return {k.strip().lower(): float(v) for k, v in raw.items()}


def map_study_type(
    pub_types: Sequence[str],
    weight_lookup: Mapping[str, float],
    *,
    fallback_label: str | None = "other",
) -> tuple[str | None, float | None]:
    """Map publication types to a weighted study type.

    Returns a tuple of (canonical_study_type, weight). If no match is found and a
    fallback is configured, that fallback label and weight will be returned
    instead.
    """

    normalized_weights = {k.strip().lower(): v for k, v in weight_lookup.items()}
    matches: list[tuple[str, float]] = []

    for pub_type in pub_types:
        normalized = pub_type.strip().lower()
        canonical = STUDY_TYPE_ALIASES.get(normalized, normalized)
        weight = normalized_weights.get(canonical)
        if weight is not None:
            matches.append((canonical, weight))

    if matches:
        return max(matches, key=lambda entry: entry[1])

    if fallback_label:
        fallback_key = fallback_label.strip().lower()
        fallback_weight = normalized_weights.get(fallback_key)
        if fallback_weight is not None:
            return fallback_key, fallback_weight

    return None, None


def compute_document_weight(
    doc_id: str,
    publication_date: date | None,
    raw_metadata: Mapping[str, Any],
    weight_lookup: Mapping[str, float],
    *,
    reference_date: date | None = None,
    half_life_days: int = DEFAULT_HALF_LIFE_DAYS,
) -> DocumentWeight:
    recency_weight = compute_recency_weight(
        publication_date, reference_date=reference_date, half_life_days=half_life_days
    )
    pub_types = extract_publication_types(raw_metadata)
    study_type, study_weight = map_study_type(pub_types, weight_lookup)

    return DocumentWeight(
        doc_id=doc_id,
        recency_weight=recency_weight,
        study_type=study_type,
        study_type_weight=study_weight,
    )
