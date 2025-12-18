from datetime import date
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingestion.europe_pmc_client import EuropePMCClient


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
