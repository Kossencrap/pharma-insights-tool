from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

from src.ingestion.models import EuropePMCSearchResult
from src.utils.identifiers import build_document_id


@dataclass
class Sentence:
    text: str
    index: int
    start_char: int
    end_char: int
    section: str


@dataclass
class Section:
    name: str
    text: str
    sentences: List[Sentence] = field(default_factory=list)

    def iter_sentences(self) -> Iterable[Sentence]:
        yield from self.sentences


@dataclass
class Document:
    doc_id: str
    source: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    doi: Optional[str] = None
    title: Optional[str] = None
    abstract: Optional[str] = None
    publication_date: Optional[date] = None
    pub_year: Optional[int] = None
    journal: Optional[str] = None
    study_design: Optional[str] = None
    study_phase: Optional[str] = None
    sample_size: Optional[int] = None
    sections: List[Section] = field(default_factory=list)

    @classmethod
    def from_europe_pmc(cls, record: EuropePMCSearchResult) -> "Document":
        doc_id = build_document_id(
            source=record.source,
            pmid=record.pmid,
            pmcid=record.pmcid,
            doi=record.doi,
            fallback_text=f"{record.title} {record.abstract or ''}",
        )

        return cls(
            doc_id=doc_id,
            source=record.source,
            pmid=record.pmid,
            pmcid=record.pmcid,
            doi=record.doi,
            title=record.title,
            abstract=record.abstract,
            publication_date=record.publication_date,
            pub_year=record.pub_year,
            journal=record.journal,
            study_design=record.study_design,
            study_phase=record.study_phase,
            sample_size=record.sample_size,
        )

    def iter_sentences(self) -> Iterable[Sentence]:
        for section in self.sections:
            yield from section.iter_sentences()

    def add_section(self, section: Section) -> None:
        self.sections.append(section)

    def to_dict(self) -> Dict[str, object]:
        """Serialize document (and nested sections) to JSON-friendly dict."""

        def _sentence_to_dict(sentence: Sentence) -> Dict[str, object]:
            return {
                "text": sentence.text,
                "index": sentence.index,
                "start_char": sentence.start_char,
                "end_char": sentence.end_char,
                "section": sentence.section,
            }

        def _section_to_dict(section: Section) -> Dict[str, object]:
            return {
                "name": section.name,
                "text": section.text,
                "sentences": [_sentence_to_dict(s) for s in section.sentences],
            }

        return {
            "doc_id": self.doc_id,
            "source": self.source,
            "pmid": self.pmid,
            "pmcid": self.pmcid,
            "doi": self.doi,
            "title": self.title,
            "abstract": self.abstract,
            "publication_date": self.publication_date.isoformat()
            if self.publication_date
            else None,
            "pub_year": self.pub_year,
            "journal": self.journal,
            "study_design": self.study_design,
            "study_phase": self.study_phase,
            "sample_size": self.sample_size,
            "sections": [_section_to_dict(section) for section in self.sections],
        }


def _normalize_identifier(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _normalize_pmcid(pmcid: Optional[str]) -> Optional[str]:
    normalized = _normalize_identifier(pmcid)
    if not normalized:
        return None
    normalized = normalized.upper()
    if not normalized.startswith("PMC"):
        normalized = f"PMC{normalized}"
    return normalized


def _normalize_doi(doi: Optional[str]) -> Optional[str]:
    normalized = _normalize_identifier(doi)
    return normalized.lower() if normalized else None


def normalize_and_deduplicate(
    records: Iterable[EuropePMCSearchResult],
) -> Tuple[List[EuropePMCSearchResult], Dict[str, int]]:
    """Normalize identifiers and collapse duplicate records.

    Returns a tuple of ``(deduplicated_records, stats)`` where stats includes the
    ``input_count`` and ``duplicates_collapsed`` counts to aid validation/metrics.
    """

    normalized: List[EuropePMCSearchResult] = []
    for record in records:
        normalized.append(
            record.model_copy(
                update={
                    "pmid": _normalize_identifier(record.pmid),
                    "pmcid": _normalize_pmcid(record.pmcid),
                    "doi": _normalize_doi(record.doi),
                }
            )
        )

    merged: Dict[str, EuropePMCSearchResult] = {}
    duplicates = 0

    for record in normalized:
        canonical_key = (
            record.pmid
            or record.doi
            or record.pmcid
            or f"title:{(record.title or '').strip().lower()}"
        )

        if canonical_key in merged:
            base = merged[canonical_key]
            merged[canonical_key] = base.model_copy(
                update={
                    "pmid": base.pmid or record.pmid,
                    "pmcid": base.pmcid or record.pmcid,
                    "doi": base.doi or record.doi,
                    "publication_date": base.publication_date or record.publication_date,
                    "pub_year": base.pub_year or record.pub_year,
                    "study_design": base.study_design or record.study_design,
                    "study_phase": base.study_phase or record.study_phase,
                    "sample_size": base.sample_size or record.sample_size,
                    "raw": base.raw or record.raw,
                }
            )
            duplicates += 1
        else:
            merged[canonical_key] = record

    stats = {
        "input_count": len(normalized),
        "duplicates_collapsed": duplicates,
        "output_count": len(merged),
    }

    return list(merged.values()), stats
