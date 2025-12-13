"""Lightweight analytics helpers for structured documents."""

from __future__ import annotations

from typing import Dict, List

from src.structuring.models import Document, Sentence


def sentence_counts_by_section(document: Document) -> Dict[str, int]:
    return {section.name: len(section.sentences) for section in document.sections}


def flattened_sentences(document: Document) -> List[Sentence]:
    return list(document.iter_sentences())


def mean_sentence_length(document: Document) -> float:
    sentences = flattened_sentences(document)
    if not sentences:
        return 0.0
    return sum(len(sentence.text) for sentence in sentences) / len(sentences)
