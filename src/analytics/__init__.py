"""Lightweight analytics helpers for structured documents."""

from __future__ import annotations

from typing import Dict, List

from src.structuring.models import Document, Sentence
from .mention_extractor import (
    MentionExtractor,
    ProductMention,
    co_mentions_from_sentence,
    load_product_config,
)


def sentence_counts_by_section(document: Document) -> Dict[str, int]:
    return {section.name: len(section.sentences) for section in document.sections}


def flattened_sentences(document: Document) -> List[Sentence]:
    return list(document.iter_sentences())


def mean_sentence_length(document: Document) -> float:
    sentences = flattened_sentences(document)
    if not sentences:
        return 0.0
    return sum(len(sentence.text) for sentence in sentences) / len(sentences)


__all__ = [
    "MentionExtractor",
    "ProductMention",
    "co_mentions_from_sentence",
    "flattened_sentences",
    "load_product_config",
    "mean_sentence_length",
    "sentence_counts_by_section",
]
