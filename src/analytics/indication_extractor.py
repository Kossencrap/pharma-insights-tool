from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence


@dataclass(frozen=True)
class IndicationMention:
    indication_canonical: str
    alias_matched: str
    start_char: int
    end_char: int


def load_indication_config(path: Path | str) -> Dict[str, List[str]]:
    config_path = Path(path)
    text = config_path.read_text(encoding="utf-8")
    data = json.loads(text)
    normalized: Dict[str, List[str]] = {}
    for canonical, aliases in data.items():
        if not isinstance(aliases, Iterable):
            continue
        alias_list = list(aliases)
        if canonical not in alias_list:
            alias_list.append(canonical)
        cleaned = sorted({alias.strip() for alias in alias_list if alias.strip()})
        if cleaned:
            normalized[canonical] = cleaned
    return normalized


class IndicationExtractor:
    def __init__(self, indication_aliases: Mapping[str, Sequence[str]]) -> None:
        self.patterns: List[tuple[str, re.Pattern[str]]] = []
        for canonical, aliases in indication_aliases.items():
            for alias in aliases:
                escaped = re.escape(alias)
                pattern = re.compile(rf"(?<!\w){escaped}(?!\w)", flags=re.IGNORECASE)
                self.patterns.append((canonical, pattern))

    def extract(self, text: str) -> List[IndicationMention]:
        mentions: List[IndicationMention] = []
        seen_spans: set[tuple[int, int, str]] = set()
        used_canonicals: set[str] = set()
        for canonical, pattern in self.patterns:
            canonical_key = canonical.lower()
            for match in pattern.finditer(text):
                if canonical_key in used_canonicals:
                    continue
                key = (match.start(), match.end(), canonical_key)
                if key in seen_spans:
                    continue
                seen_spans.add(key)
                used_canonicals.add(canonical_key)
                mentions.append(
                    IndicationMention(
                        indication_canonical=canonical,
                        alias_matched=match.group(0),
                        start_char=match.start(),
                        end_char=match.end(),
                    )
                )
        mentions.sort(key=lambda m: (m.start_char, m.end_char))
        return mentions
