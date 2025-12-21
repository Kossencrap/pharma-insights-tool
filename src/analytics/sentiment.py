from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, TypedDict


DEFAULT_SENTIMENT_CONFIG = Path("config/sentiment_lexicon.json")


class SentimentLabel(str, Enum):
    POSITIVE = "POS"
    NEUTRAL = "NEU"
    NEGATIVE = "NEG"


class SentenceRecord(TypedDict, total=False):
    doc_id: str
    sentence_id: str
    date: str
    text: str
    sentence_text: str
    product_mentions: List[dict]
    sentiment_label: str
    sentiment_score: float
    sentiment_model: str
    sentiment_inference_ts: str


@dataclass(frozen=True)
class SentimentConfig:
    version: str
    positive_terms: Tuple[str, ...]
    negative_terms: Tuple[str, ...]
    negations: Tuple[str, ...]
    hedges: Tuple[str, ...]
    contrast_terms: Tuple[str, ...]
    negation_window: int
    score_threshold: float


@dataclass(frozen=True)
class SentimentResult:
    label: str
    score: float
    model_version: str
    inference_ts: str


@dataclass(frozen=True)
class _TermMatch:
    term: str
    token_index: int


@lru_cache(maxsize=1)
def load_sentiment_config(path: Path | str = DEFAULT_SENTIMENT_CONFIG) -> SentimentConfig:
    config_path = Path(path)
    data = json.loads(config_path.read_text(encoding="utf-8"))

    return SentimentConfig(
        version=data.get("version", "lexicon_v1"),
        positive_terms=tuple(data.get("positive_terms", [])),
        negative_terms=tuple(data.get("negative_terms", [])),
        negations=tuple(data.get("negations", [])),
        hedges=tuple(data.get("hedges", [])),
        contrast_terms=tuple(data.get("contrast_terms", [])),
        negation_window=int(data.get("negation_window", 3)),
        score_threshold=float(data.get("score_threshold", 1)),
    )


def _token_spans(text: str) -> List[Tuple[int, int]]:
    return [(match.start(), match.end()) for match in re.finditer(r"\b\w+\b", text)]


def _token_index_for_char(spans: Sequence[Tuple[int, int]], char_pos: int) -> Optional[int]:
    for idx, (start, end) in enumerate(spans):
        if start <= char_pos < end:
            return idx
    return None


def _find_matches(text: str, terms: Iterable[str], spans: Sequence[Tuple[int, int]]) -> List[_TermMatch]:
    matches: List[_TermMatch] = []
    for term in terms:
        pattern = re.compile(rf"\b{re.escape(term)}\b", flags=re.IGNORECASE)
        for match in pattern.finditer(text):
            token_index = _token_index_for_char(spans, match.start())
            if token_index is None:
                continue
            matches.append(_TermMatch(term=term.lower(), token_index=token_index))
    return matches


def _has_term(text: str, terms: Iterable[str]) -> bool:
    for term in terms:
        pattern = re.compile(rf"\b{re.escape(term)}\b", flags=re.IGNORECASE)
        if pattern.search(text):
            return True
    return False


def _is_negated(token_index: int, negations: Sequence[int], window: int) -> bool:
    for neg_index in negations:
        if 0 <= token_index - neg_index <= window:
            return True
    return False


def classify_sentence(text: str) -> SentimentResult:
    config = load_sentiment_config()
    spans = _token_spans(text.lower())

    pos_matches = _find_matches(text, config.positive_terms, spans)
    neg_matches = _find_matches(text, config.negative_terms, spans)
    negation_matches = _find_matches(text, config.negations, spans)

    negation_indices = [match.token_index for match in negation_matches]

    pos_count = 0
    neg_count = 0

    for match in pos_matches:
        if _is_negated(match.token_index, negation_indices, config.negation_window):
            neg_count += 1
        else:
            pos_count += 1

    for match in neg_matches:
        if _is_negated(match.token_index, negation_indices, config.negation_window):
            pos_count += 1
        else:
            neg_count += 1

    if pos_count and neg_count:
        label = SentimentLabel.NEUTRAL.value
        score = 0.0
    else:
        score = float(pos_count - neg_count)
        if _has_term(text, config.hedges):
            score *= 0.5

        if score >= config.score_threshold:
            label = SentimentLabel.POSITIVE.value
        elif score <= -config.score_threshold:
            label = SentimentLabel.NEGATIVE.value
        else:
            label = SentimentLabel.NEUTRAL.value

    return SentimentResult(
        label=label,
        score=score,
        model_version=config.version,
        inference_ts=datetime.now(timezone.utc).isoformat(),
    )


def classify_batch(sentences: list[SentenceRecord]) -> list[SentenceRecord]:
    results: list[SentenceRecord] = []
    for record in sentences:
        text = record.get("sentence_text") or record.get("text") or ""
        sentiment = classify_sentence(text)
        enriched = dict(record)
        enriched.update(
            {
                "sentiment_label": sentiment.label,
                "sentiment_score": sentiment.score,
                "sentiment_model": sentiment.model_version,
                "sentiment_inference_ts": sentiment.inference_ts,
            }
        )
        results.append(enriched)
    return results
