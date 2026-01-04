import sqlite3
import sqlite3
from pathlib import Path

from src.analytics.evidence import fetch_sentence_evidence, serialize_sentence_evidence
from src.storage import init_db


def _seed(db_path: Path) -> sqlite3.Connection:
    con = init_db(db_path)
    con.execute(
        "INSERT INTO documents (doc_id, title, publication_date, pub_year, raw_json, journal) VALUES (?, ?, ?, ?, ?, ?)",
        ("doc-new", "Newer Doc", "2024-02-01", 2024, "{}", "Journal A"),
    )
    con.execute(
        "INSERT INTO documents (doc_id, title, publication_date, pub_year, raw_json, journal) VALUES (?, ?, ?, ?, ?, ?)",
        ("doc-old", "Older Doc", "2023-12-15", 2023, "{}", "Journal B"),
    )

    con.execute(
        "INSERT INTO document_weights (doc_id, recency_weight, study_type, study_type_weight, combined_weight) VALUES (?, ?, ?, ?, ?)",
        ("doc-new", 0.9, "randomized controlled trial", 1.5, 2.0),
    )
    con.execute(
        "INSERT INTO document_weights (doc_id, recency_weight, study_type, study_type_weight, combined_weight) VALUES (?, ?, ?, ?, ?)",
        ("doc-old", 0.6, "review", 1.0, 0.6),
    )

    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        ("s-new", "doc-new", "abstract", 0, "ProductA outperformed ProductB with fewer side effects."),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        ("s-old", "doc-old", "results", 1, "ProductA and ProductB were both effective."),
    )

    con.execute(
        "INSERT INTO co_mentions_sentences (doc_id, sentence_id, product_a, product_b, count) VALUES (?, ?, ?, ?, ?)",
        ("doc-new", "s-new", "ProductA", "ProductB", 2),
    )
    con.execute(
        "INSERT INTO co_mentions_sentences (doc_id, sentence_id, product_a, product_b, count) VALUES (?, ?, ?, ?, ?)",
        ("doc-old", "s-old", "ProductA", "ProductB", 1),
    )

    con.executemany(
        "INSERT INTO product_mentions (mention_id, doc_id, sentence_id, product_canonical, alias_matched, start_char, end_char, match_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("m1", "doc-new", "s-new", "ProductA", "ProdA", 0, 6, "regex"),
            ("m2", "doc-new", "s-new", "ProductB", "ProdB", 22, 29, "regex"),
            ("m3", "doc-old", "s-old", "ProductA", "Product A", 0, 9, "regex"),
            ("m4", "doc-old", "s-old", "ProductB", "Product B", 15, 24, "regex"),
        ],
    )

    con.execute(
        "INSERT INTO sentence_events (doc_id, sentence_id, product_a, product_b, comparative_terms, relationship_types, risk_terms, study_context, matched_terms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "doc-new",
            "s-new",
            "ProductA",
            "ProductB",
            "outperformed, superior",
            "improves",
            "reduced risk",
            "trial",
            "ProductA vs ProductB",
        ),
    )
    con.execute(
        "INSERT INTO sentence_events (doc_id, sentence_id, product_a, product_b, comparative_terms, relationship_types, risk_terms, study_context, matched_terms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "doc-old",
            "s-old",
            "ProductA",
            "ProductB",
            "effective",
            "neutral",
            "",
            "observational",
            "pair mention",
        ),
    )
    con.commit()
    return con


def test_fetch_sentence_evidence_orders_and_filters(tmp_path: Path) -> None:
    db_path = tmp_path / "evidence.sqlite"
    con = _seed(db_path)

    rows = fetch_sentence_evidence(con, product_a="ProductA", product_b="ProductB", limit=10)
    assert [row.doc_id for row in rows] == ["doc-new", "doc-old"]  # ordered by publication_date desc
    assert rows[0].evidence_weight == 4.0  # combined_weight 2.0 * count 2
    assert "superior" in rows[0].labels and "improves" in rows[0].labels
    assert rows[0].product_a_alias == "ProdA"
    assert rows[0].product_b_alias == "ProdB"

    filtered = fetch_sentence_evidence(con, product_a="ProductA", pub_after="2023-12-31")
    assert len(filtered) == 1
    assert filtered[0].doc_id == "doc-new"


def test_serialize_sentence_evidence_includes_computed_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "evidence.sqlite"
    con = _seed(db_path)

    rows = fetch_sentence_evidence(con, limit=5)
    serialized = serialize_sentence_evidence(rows)

    assert serialized[0]["evidence_weight"] == rows[0].evidence_weight
    assert serialized[0]["matched_terms"] == "ProductA vs ProductB"
    assert serialized[0]["product_a_alias"] == "ProdA"
    assert serialized[0]["product_b_alias"] == "ProdB"
    assert serialized[0]["labels"]  # should include combined labels from all sources
    assert serialized[0]["publication_date"] == "2024-02-01"
    assert serialized[1]["publication_date"] == "2023-12-15"

    # round-trip to CSV-like ordering stability
    keys = list(serialized[0].keys())
    assert "sentence_text" in keys and "combined_weight" in keys
