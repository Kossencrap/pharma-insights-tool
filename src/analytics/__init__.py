"""Lightweight analytics helpers for structured documents."""

from __future__ import annotations

from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported only for typing
    from src.structuring.models import Document, Sentence
from .evidence import (
    SentenceEvidence,
    explain_confidence,
    fetch_sentence_evidence,
    build_narrative_card,
    NarrativeEvidenceCard,
    resolve_study_weight,
    serialize_sentence_evidence,
)
from .narratives import (
    DirectionalClassification,
    NarrativeClassification,
    ProductRoleContext,
    classify_directional_roles,
    classify_narrative,
)
from .indication_extractor import IndicationExtractor, load_indication_config
from .mention_extractor import (
    MentionExtractor,
    ProductMention,
    co_mentions_from_sentence,
    load_product_config,
)
from .sentiment import SentimentResult, SentimentLabel, classify_batch, classify_sentence
from .time_series import (
    TimeSeriesConfig,
    add_change_metrics,
    add_sentiment_ratios,
    bucket_counts,
    sentiment_bucket_counts,
)
from .weights import (
    DocumentWeight,
    compute_document_weight,
    compute_recency_weight,
    extract_publication_types,
    load_study_type_weights,
    map_study_type,
)


def sentence_counts_by_section(document: "Document") -> Dict[str, int]:
    return {section.name: len(section.sentences) for section in document.sections}


def flattened_sentences(document: "Document") -> List["Sentence"]:
    return list(document.iter_sentences())


def mean_sentence_length(document: "Document") -> float:
    sentences = flattened_sentences(document)
    if not sentences:
        return 0.0
    return sum(len(sentence.text) for sentence in sentences) / len(sentences)


__all__ = [
    "IndicationExtractor",
    "load_indication_config",
    "MentionExtractor",
    "ProductMention",
    "co_mentions_from_sentence",
    "flattened_sentences",
    "load_product_config",
    "mean_sentence_length",
    "SentenceEvidence",
    "NarrativeEvidenceCard",
    "explain_confidence",
    "DocumentWeight",
    "compute_document_weight",
    "compute_recency_weight",
    "extract_publication_types",
    "load_study_type_weights",
    "map_study_type",
    "sentence_counts_by_section",
    "fetch_sentence_evidence",
    "build_narrative_card",
    "serialize_sentence_evidence",
    "TimeSeriesConfig",
    "add_change_metrics",
    "bucket_counts",
    "add_sentiment_ratios",
    "sentiment_bucket_counts",
    "SentimentLabel",
    "SentimentResult",
    "classify_batch",
    "classify_sentence",
    "resolve_study_weight",
    "NarrativeClassification",
    "classify_narrative",
    "DirectionalClassification",
    "ProductRoleContext",
    "classify_directional_roles",
]
