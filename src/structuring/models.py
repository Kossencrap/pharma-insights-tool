from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Iterable, List, Optional

from src.ingestion.models import EuropePMCSearchResult


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
        return cls(
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
