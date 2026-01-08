from __future__ import annotations

import re
from typing import List, Optional

import spacy
from spacy.language import Language

from src.analytics.sections import normalize_section
from src.ingestion.models import EuropePMCSearchResult
from src.structuring.models import Document, Section, Sentence

DEFAULT_SECTION_TITLE = "title"
DEFAULT_SECTION_ABSTRACT = "abstract"


class SentenceSplitter:
    """
    Deterministic sentence splitter using a lightweight spaCy pipeline.

    A blank English pipeline with the built-in sentencizer is used by default
    to avoid heavyweight model downloads.
    """

    def __init__(self, nlp: Optional[Language] = None) -> None:
        self.nlp = nlp or spacy.blank("en")
        if "sentencizer" not in self.nlp.pipe_names:
            self.nlp.add_pipe("sentencizer")

    def _assign_sentence_sections(self, sentences: List[Sentence], *, default_section: str) -> None:
        """Derive canonical sections for each sentence using heading cues."""
        current_section = default_section
        for sentence in sentences:
            canonical, cleaned_text, derived = normalize_section(current_section, sentence.text)
            assigned = canonical or current_section or default_section
            should_strip_heading = (
                derived and cleaned_text and assigned not in {DEFAULT_SECTION_TITLE}
            )
            if should_strip_heading:
                sentence.text = cleaned_text
            sentence.section = assigned
            if derived and assigned:
                current_section = assigned
            elif assigned not in {None, DEFAULT_SECTION_ABSTRACT, DEFAULT_SECTION_TITLE}:
                current_section = assigned

    @staticmethod
    def _prepare_text(raw_text: str) -> str:
        """Insert whitespace so inline structured-abstract headings split cleanly."""
        if not raw_text:
            return ""
        text = re.sub(r"(?<=[.!?])(<h\d)", r" \1", raw_text)
        text = re.sub(r"(</h\d>)(?=[A-Za-z0-9])", r"\1 ", text)
        return text

    def split_section(self, *, name: str, text: Optional[str], starting_index: int = 0) -> Section:
        raw_text = text or ""
        prepared_text = self._prepare_text(raw_text)
        doc = self.nlp(prepared_text)
        sentences = []
        current_index = starting_index

        for sent in doc.sents:
            stripped = sent.text.strip()
            if not stripped:
                continue
            sentences.append(
                Sentence(
                    text=stripped,
                    index=current_index,
                    start_char=sent.start_char,
                    end_char=sent.end_char,
                    section=name,
                )
            )
            current_index += 1

        if sentences:
            self._assign_sentence_sections(sentences, default_section=name)

        return Section(name=name, text=text or "", sentences=sentences)

    def split_document(self, record: EuropePMCSearchResult) -> Document:
        document = Document.from_europe_pmc(record)

        sentence_index = 0
        if record.title:
            title_section = self.split_section(
                name=DEFAULT_SECTION_TITLE, text=record.title, starting_index=sentence_index
            )
            document.add_section(title_section)
            sentence_index += len(title_section.sentences)

        if record.abstract:
            abstract_section = self.split_section(
                name=DEFAULT_SECTION_ABSTRACT,
                text=record.abstract,
                starting_index=sentence_index,
            )
            document.add_section(abstract_section)

        return document
