from __future__ import annotations

import json
import re
import unicodedata
import warnings
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
    seen_aliases: dict[str, str] = {}
    for canonical, aliases in data.items():
        alias_list = list(aliases)
        if canonical not in alias_list:
            alias_list.append(canonical)
        pruned_aliases: list[str] = []
        for alias in alias_list:
            cleaned = alias.strip()
            if len(cleaned) < 3:
                warnings.warn(
                    f"Skipping alias '{alias}' for canonical '{canonical}' because it is too short to match reliably.",
                    RuntimeWarning,
                )
                continue
            key = cleaned.lower()
            other = seen_aliases.get(key)
            if other and other != canonical:
                warnings.warn(
                    f"Alias '{alias}' appears for both '{other}' and '{canonical}'. Matching will favor first occurrence.",
                    RuntimeWarning,
                )
            else:
                seen_aliases[key] = canonical
            pruned_aliases.append(cleaned)
        normalized[canonical] = pruned_aliases
    return normalized


class MentionExtractor:
    """Product mention extraction with rule-based and optional NLP assistance."""

    def __init__(
        self,
        product_aliases: Mapping[str, Sequence[str]],
        *,
        use_model_assisted: bool = False,
        nlp=None,
    ):
        self.use_model_assisted = use_model_assisted
        self.patterns: List[tuple[str, str, re.Pattern[str]]] = []
        for canonical, aliases in product_aliases.items():
            for alias in aliases:
                escaped = re.escape(alias)
                plural_suffix = r"(?:['’]s|s|es)?"
                boundary_prefix = r"(?:\b|(?<=['’]))"
                boundary_suffix = r"(?:\b|(?=['’]))"
                pattern = re.compile(
                    rf"{boundary_prefix}{escaped}{plural_suffix}{boundary_suffix}",
                    flags=re.IGNORECASE,
                )
                self.patterns.append((canonical, alias, pattern))

        self.nlp = None
        if use_model_assisted:
            try:
                import spacy  # type: ignore
            except Exception as exc:  # pragma: no cover - dependency guard
                raise ImportError(
                    "Model-assisted mention extraction requires spaCy to be installed"
                ) from exc

            self.nlp = nlp or spacy.blank("en")
            ruler = self.nlp.add_pipe("entity_ruler")
            patterns = []
            for canonical, aliases in product_aliases.items():
                for alias in aliases:
                    patterns.append(
                        {
                            "label": "PRODUCT",
                            "id": canonical,
                            "pattern": alias,
                            "title": alias,
                        }
                    )
            ruler.add_patterns(patterns)

    def _extract_with_regex(self, text: str) -> List[ProductMention]:
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

    def _extract_with_model(self, text: str) -> List[ProductMention]:
        if not self.nlp:
            return []

        mentions: List[ProductMention] = []
        doc = self.nlp(text)
        for ent in doc.ents:
            if ent.label_ != "PRODUCT":
                continue
            canonical = ent.ent_id_ or ent.label_
            mentions.append(
                ProductMention(
                    product_canonical=canonical,
                    alias_matched=ent.text,
                    start_char=ent.start_char,
                    end_char=ent.end_char,
                    match_method="nlp",
                )
            )
        return mentions

    def extract(self, text: str) -> List[ProductMention]:
        normalized_text = unicodedata.normalize("NFC", text)

        mentions: List[ProductMention] = []
        if self.use_model_assisted:
            mentions.extend(self._extract_with_model(normalized_text))

        mentions.extend(self._extract_with_regex(normalized_text))

        seen = set()
        unique_mentions: List[ProductMention] = []
        for mention in sorted(mentions, key=lambda m: (m.start_char, m.end_char)):
            key = (
                mention.start_char,
                mention.end_char,
                mention.product_canonical.lower(),
                mention.match_method,
            )
            if key in seen:
                continue
            seen.add(key)
            unique_mentions.append(mention)

        return unique_mentions


def co_mentions_from_sentence(mentions: Iterable[ProductMention]) -> List[tuple[str, str, int]]:
    seen = sorted({m.product_canonical.lower() for m in mentions})
    pairs: List[tuple[str, str, int]] = []
    for i in range(len(seen)):
        for j in range(i + 1, len(seen)):
            pairs.append((seen[i], seen[j], 1))
    return pairs
