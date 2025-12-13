from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence


@dataclass
class ProductMention:
    product_canonical: str
    alias_matched: str
    start_char: int
    end_char: int
    match_method: str = "regex"


def load_product_config(path: Path | str) -> Dict[str, List[str]]:
    """Load a product dictionary from JSON or YAML-like text."""
    config_path = Path(path)
    text = config_path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise ValueError(f"Product config at {path} must be valid JSON") from None

    normalized: Dict[str, List[str]] = {}
    for canonical, aliases in data.items():
        alias_list = list(aliases)
        if canonical not in alias_list:
            alias_list.append(canonical)
        normalized[canonical] = alias_list
    return normalized


class MentionExtractor:
    """Deterministic, dictionary-based product mention extraction."""

    def __init__(self, product_aliases: Mapping[str, Sequence[str]]):
        self.patterns: List[tuple[str, str, re.Pattern[str]]] = []
        for canonical, aliases in product_aliases.items():
            for alias in aliases:
                escaped = re.escape(alias)
                pattern = re.compile(rf"\b{escaped}\b", flags=re.IGNORECASE)
                self.patterns.append((canonical, alias, pattern))

    def extract(self, text: str) -> List[ProductMention]:
        mentions: List[ProductMention] = []
        for canonical, alias, pattern in self.patterns:
            for match in pattern.finditer(text):
                mentions.append(
                    ProductMention(
                        product_canonical=canonical,
                        alias_matched=match.group(0),
                        start_char=match.start(),
                        end_char=match.end(),
                        match_method="regex",
                    )
                )
        return mentions


def co_mentions_from_sentence(mentions: Iterable[ProductMention]) -> List[tuple[str, str, int]]:
    seen = sorted({m.product_canonical.lower() for m in mentions})
    pairs: List[tuple[str, str, int]] = []
    for i in range(len(seen)):
        for j in range(i + 1, len(seen)):
            pairs.append((seen[i], seen[j], 1))
    return pairs
