from __future__ import annotations

from typing import Optional

import spacy
from spacy.language import Language

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

    def split_section(self, *, name: str, text: Optional[str], starting_index: int = 0) -> Section:
        doc = self.nlp(text or "")
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
