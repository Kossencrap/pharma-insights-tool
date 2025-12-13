from __future__ import annotations

import hashlib
import re
from typing import Optional


def build_document_id(
    *,
    source: Optional[str],
    pmid: Optional[str],
    pmcid: Optional[str],
    doi: Optional[str],
    fallback_text: str = "",
) -> str:
    """
    Create a stable document identifier prioritizing authoritative IDs.

    Preference order: PMID > PMCID > DOI. If none are present, a hash of the
    fallback_text is used to ensure deterministic IDs.
    """

    source_prefix = (source or "unknown").lower()
    if pmid:
        return f"{source_prefix}:pmid:{pmid}"
    if pmcid:
        return f"{source_prefix}:pmcid:{pmcid}"
    if doi:
        return f"{source_prefix}:doi:{doi}"

    digest = hashlib.sha256(fallback_text.encode("utf-8")).hexdigest()[:12]
    return f"{source_prefix}:hash:{digest}"


def build_sentence_id(doc_id: str, section: str, sent_index: int) -> str:
    """Generate a deterministic sentence identifier within a document."""
    section_slug = re.sub(r"\W+", "_", section.lower()).strip("_") or "section"
    return f"{doc_id}:sec:{section_slug}:sent:{sent_index}"
