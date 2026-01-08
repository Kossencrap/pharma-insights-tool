import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingestion.models import EuropePMCSearchResult
from src.structuring.sentence_splitter import SentenceSplitter
from src.analytics import flattened_sentences, mean_sentence_length, sentence_counts_by_section


def _example_record() -> EuropePMCSearchResult:
    return EuropePMCSearchResult(
        pmid="12345",
        pmcid="PMC12345",
        doi="10.1000/example",
        title="Dupilumab improves outcomes. Safety profile remains consistent.",
        abstract="The trial enrolled 50 patients. Efficacy was observed early."
        " Adverse events were mild.",
        journal="Example Journal",
    )


def test_sentence_splitter_orders_sections_and_sentences():
    record = _example_record()
    splitter = SentenceSplitter()

    document = splitter.split_document(record)

    assert document.title == record.title
    assert document.doc_id.startswith("unknown:pmid:12345")
    assert document.pmid == "12345"

    sentences = list(document.iter_sentences())
    assert [s.section for s in sentences] == ["title", "title", "abstract", "abstract", "abstract"]
    assert sentences[0].text.startswith("Dupilumab")
    assert sentences[-1].index == 4


def test_sentence_and_section_analytics():
    splitter = SentenceSplitter()
    document = splitter.split_document(_example_record())

    counts = sentence_counts_by_section(document)
    assert counts == {"title": 2, "abstract": 3}

    flat = flattened_sentences(document)
    assert len(flat) == 5

    average_length = mean_sentence_length(document)
    assert average_length > 10


def test_document_to_dict_serializes_sections_and_sentence_indices():
    splitter = SentenceSplitter()
    document = splitter.split_document(_example_record())

    as_dict = document.to_dict()

    assert as_dict["doc_id"] == document.doc_id
    assert as_dict["source"] is None
    assert as_dict["pmid"] == "12345"
    assert as_dict["publication_date"] is None
    assert as_dict["sections"][0]["sentences"][0]["index"] == 0


def test_sentence_splitter_assigns_structured_abstract_sections():
    record = _example_record()
    record.abstract = (
        "<h4>Aims</h4>This is the introduction sentence. Additional rationale."
        "<h4>Methods and results</h4>Early quadruple therapy was defined. Outcomes improved."
    )
    splitter = SentenceSplitter()

    document = splitter.split_document(record)
    sections = [sentence.section for sentence in document.iter_sentences()]
    abstract_sections = sections[2:]

    assert sections[:2] == ["title", "title"]
    assert abstract_sections[:2] == ["introduction", "introduction"]
    assert abstract_sections[-1] == "results"


def test_sentence_splitter_handles_run_on_headings():
    record = _example_record()
    record.abstract = (
        "BackgroundThis cohort explored therapy adherence."
        "MethodsParticipants were randomized."
        "ResultsSignificant improvements observed."
    )
    splitter = SentenceSplitter()

    document = splitter.split_document(record)
    sections = [sentence.section for sentence in document.iter_sentences()]

    assert sections[:2] == ["title", "title"]
    assert sections[2:5] == ["introduction", "methods", "results"]
