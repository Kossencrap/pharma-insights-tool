from datetime import date
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingestion.europe_pmc_client import EuropePMCClient
from src.ingestion.models import EuropePMCSearchResult
from src.structuring.models import Document, normalize_and_deduplicate


def test_build_drug_query_includes_names_and_date_range(execution_log):
    query = EuropePMCClient.build_drug_query(
        product_names=["Dupixent", "dupilumab"],
        from_date=date(2024, 1, 1),
        to_date=date(2024, 12, 31),
        require_abstract=True,
    )

    assert "TITLE:\"Dupixent\"" in query
    assert "ABSTRACT:\"dupilumab\"" in query
    assert "HAS_ABSTRACT:Y" in query
    assert "FIRST_PDATE:[2024-01-01 TO 2024-12-31]" in query
    execution_log.record(
        "Europe PMC query",
        "Dupixent/dupilumab query includes abstract requirement and 2024 date window",
    )


def test_parse_publication_date_variants(execution_log):
    client = EuropePMCClient()

    pub_date, pub_year = client._parse_publication_date(  # pylint: disable=protected-access
        {
            "firstPublicationDate": "2024-02-15",
            "pubYear": "2024",
        }
    )

    assert pub_date == date(2024, 2, 15)
    assert pub_year == 2024

    # Fallback to pubYear only
    pub_date, pub_year = client._parse_publication_date(  # pylint: disable=protected-access
        {"pubYear": "2020"}
    )
    assert pub_date is None
    assert pub_year == 2020
    execution_log.record(
        "Publication dates",
        "Parsed explicit 2024-02-15 and fallback pubYear=2020 variants",
    )


def test_normalize_and_capture_study_metadata(execution_log):
    records = [
        EuropePMCSearchResult(
            pmid=" 12345 ",
            pmcid="12345",
            doi="10.1000/TEST",
            title="Sample title",
            abstract="Study details",
            study_design="randomized",
            study_phase="Phase II",
            sample_size=150,
        ),
        EuropePMCSearchResult(
            pmid="12345",  # duplicate PMID; should collapse
            title="Sample title",
            study_design=None,
            study_phase=None,
            sample_size=None,
        ),
    ]

    deduped, stats = normalize_and_deduplicate(records)
    assert stats["input_count"] == 2
    assert stats["output_count"] == 1
    assert stats["duplicates_collapsed"] == 1

    doc = Document.from_europe_pmc(deduped[0])
    assert doc.pmcid == "PMC12345"
    assert doc.doi == "10.1000/test"
    assert doc.study_phase == "Phase II"
    assert doc.sample_size == 150
    execution_log.record(
        "Normalization",
        "Collapsed duplicate PMID entries and preserved study metadata for downstream structuring",
    )
