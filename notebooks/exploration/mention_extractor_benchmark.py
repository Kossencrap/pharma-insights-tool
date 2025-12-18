"""Compare rule-based and model-assisted mention extraction on a toy corpus."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from src.analytics.mention_extractor import MentionExtractor


@dataclass
class BenchmarkResult:
    strategy: str
    precision: float
    recall: float
    f1: float
    matched: int
    expected: int
    predicted: int


CORPUS = [
    (
        "Semaglutide (Ozempic) outperformed insulin glargine in the trial.",
        {"semaglutide"},
    ),
    (
        "Patients switched from Ozempic to Mounjaro after adverse events.",
        {"semaglutide", "tirzepatide"},
    ),
    (
        "The formulation of tirzepatide was delivered via a weekly pen.",
        {"tirzepatide"},
    ),
]

PRODUCT_ALIASES: Dict[str, Sequence[str]] = {
    "semaglutide": ["Ozempic", "semaglutide"],
    "tirzepatide": ["Mounjaro", "tirzepatide"],
}


def _score(predicted: Iterable[str], expected: Iterable[str]) -> tuple[int, int, int]:
    predicted_set = set(predicted)
    expected_set = set(expected)
    tp = len(predicted_set & expected_set)
    fp = len(predicted_set - expected_set)
    fn = len(expected_set - predicted_set)
    return tp, fp, fn


def _evaluate(strategy: str, extractor: MentionExtractor) -> BenchmarkResult:
    tp_total = fp_total = fn_total = 0
    predicted_total = expected_total = 0

    for text, expected in CORPUS:
        mentions = extractor.extract(text)
        predicted = {m.product_canonical for m in mentions}
        tp, fp, fn = _score(predicted, expected)
        tp_total += tp
        fp_total += fp
        fn_total += fn
        predicted_total += len(predicted)
        expected_total += len(expected)

    precision = tp_total / (tp_total + fp_total) if (tp_total + fp_total) else 0.0
    recall = tp_total / (tp_total + fn_total) if (tp_total + fn_total) else 0.0
    f1 = (
        2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    )

    return BenchmarkResult(
        strategy=strategy,
        precision=precision,
        recall=recall,
        f1=f1,
        matched=tp_total,
        expected=expected_total,
        predicted=predicted_total,
    )


def run() -> List[BenchmarkResult]:
    regex_extractor = MentionExtractor(PRODUCT_ALIASES)
    model_extractor = MentionExtractor(PRODUCT_ALIASES, use_model_assisted=True)

    results = [
        _evaluate("regex", regex_extractor),
        _evaluate("model_assisted", model_extractor),
    ]
    return results


if __name__ == "__main__":
    for result in run():
        print(
            f"{result.strategy}: precision={result.precision:.2f}, "
            f"recall={result.recall:.2f}, f1={result.f1:.2f} "
            f"(matched {result.matched}/{result.expected}, predicted {result.predicted})"
        )
