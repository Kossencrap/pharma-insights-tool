from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Iterable, List, Optional

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
            "sections": [_section_to_dict(section) for section in self.sections],
        }
