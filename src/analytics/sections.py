from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, Tuple

DEFAULT_SECTION_CONFIG = Path("config/section_aliases.json")


@dataclass(frozen=True)
class SectionAliasSpec:
    canonical: str
    aliases: Tuple[str, ...]


def _resolve_config_path(path: Path | str | None) -> Path:
    config_path = Path(path) if path is not None else DEFAULT_SECTION_CONFIG
    if not config_path.exists():
        raise FileNotFoundError(f"Section alias config not found at {config_path}")
    return config_path.resolve()


def _normalize_token(value: str) -> str:
    cleaned = value.strip().casefold()
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"[^\w\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _load_aliases_from_disk(path: Path) -> Dict[str, Tuple[str, ...]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    aliases: Dict[str, Tuple[str, ...]] = {}
    for canonical, values in data.items():
        canonical_norm = _normalize_token(canonical)
        if not canonical_norm:
            continue
        tokens = {_normalize_token(value) for value in values or []}
        tokens.add(canonical_norm)
        aliases[canonical_norm] = tuple(sorted(token for token in tokens if token))
    return aliases


@lru_cache(maxsize=4)
def _load_aliases_cached(resolved_path: str) -> Dict[str, Tuple[str, ...]]:
    return _load_aliases_from_disk(Path(resolved_path))


def load_section_aliases(path: Path | str | None = None) -> Dict[str, Tuple[str, ...]]:
    resolved = _resolve_config_path(path)
    return _load_aliases_cached(str(resolved))


def _match_alias(token: str | None, aliases: Dict[str, Tuple[str, ...]]) -> str | None:
    if not token:
        return None

    def _lookup(normalized: str) -> str | None:
        if not normalized:
            return None
        for canonical, candidates in aliases.items():
            if normalized == canonical or normalized in candidates:
                return canonical
        return None

    normalized = _normalize_token(token)
    canonical = _lookup(normalized)
    if canonical:
        return canonical

    parts = re.split(r"[\\/&|]+", token)
    for part in parts:
        if not part or part == token:
            continue
        part_normalized = _normalize_token(part)
        canonical = _lookup(part_normalized)
        if canonical:
            return canonical
    return None


def _match_leading_heading(
    text: str, aliases: Dict[str, Tuple[str, ...]]
) -> tuple[int, int, str] | None:
    if not text:
        return None
    leading_whitespace = re.match(r"\s*", text)
    offset = leading_whitespace.end() if leading_whitespace else 0
    stripped = text[offset:]
    if not stripped:
        return None
    max_length = min(len(stripped), 80)
    for length in range(1, max_length + 1):
        segment = stripped[:length]
        normalized = _normalize_token(segment)
        if not normalized:
            continue
        canonical = None
        for key, candidates in aliases.items():
            if normalized == key or normalized in candidates:
                canonical = key
                break
        if canonical:
            next_char = stripped[length:length + 1]
            if not next_char or not next_char.isalpha() or next_char.isupper():
                return offset, offset + length, stripped[:length]
    return None


def normalize_section(
    raw_section: str | None,
    text: str | None = None,
    *,
    config_path: Path | str | None = None,
) -> tuple[str | None, str | None, bool]:
    """
    Normalize a sentence section into the canonical taxonomy.

    Returns a tuple of (canonical_section, cleaned_text, derived_from_heading).
    cleaned_text removes any inline heading (e.g., "Methods:") when that heading
    determines the section.
    """

    aliases = load_section_aliases(config_path)
    candidates: list[str] = []

    if raw_section:
        candidates.append(raw_section)
        if ":" in raw_section:
            candidates.extend(part for part in raw_section.split(":") if part)

    heading_prefix = None
    cleaned_text = text
    derived = False
    if text:
        prefix, sep, remainder = text.partition(":")
        if sep and len(prefix) <= 40:
            heading_prefix = prefix
            candidates.insert(0, prefix)
            cleaned_text = remainder.lstrip(" -–—")

        tag_match = re.search(r"<h\d[^>]*>([^<]{1,80})</h\d>", text, flags=re.IGNORECASE)
        if tag_match:
            heading_prefix = tag_match.group(1)
            candidates.insert(0, heading_prefix)
            if tag_match.end() < len(text):
                cleaned_text = (text[:tag_match.start()] + " " + text[tag_match.end():]).strip()
            else:
                cleaned_text = text[:tag_match.start()].strip()

        if heading_prefix is None:
            heading_match = _match_leading_heading(text, aliases)
            if heading_match:
                _, end_idx, fragment = heading_match
                heading_prefix = fragment
                candidates.insert(0, fragment)
                cleaned_text = text[end_idx:].lstrip(" -–—")

    for candidate in candidates:
        canonical = _match_alias(candidate, aliases)
        if canonical:
            if heading_prefix and candidate == heading_prefix:
                return canonical, cleaned_text, True
            return canonical, text, derived

    fallback = raw_section.strip().lower() if raw_section else None
    return fallback or None, text, derived
