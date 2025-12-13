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
