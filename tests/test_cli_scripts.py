import json
import sys
from pathlib import Path

from src.storage import init_db

from scripts import query_comentions, show_sentence_evidence


def _seed_cli_db(db_path: Path) -> None:
    con = init_db(db_path)
    con.execute(
        "INSERT INTO documents (doc_id, title, publication_date, pub_year, raw_json, journal) VALUES (?, ?, ?, ?, ?, ?)",
        ("doc-1", "Doc One", "2024-03-10", 2024, "{}", "Journal A"),
    )
    con.execute(
        "INSERT INTO documents (doc_id, title, publication_date, pub_year, raw_json, journal) VALUES (?, ?, ?, ?, ?, ?)",
        ("doc-2", "Doc Two", "2023-12-05", 2023, "{}", "Journal B"),
    )
    con.execute(
        "INSERT INTO document_weights (doc_id, recency_weight, study_type, study_type_weight, combined_weight) VALUES (?, ?, ?, ?, ?)",
        ("doc-1", 0.9, "randomized controlled trial", 1.4, 1.8),
    )
    con.execute(
        "INSERT INTO document_weights (doc_id, recency_weight, study_type, study_type_weight, combined_weight) VALUES (?, ?, ?, ?, ?)",
        ("doc-2", 0.6, "review", 1.0, 0.6),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        ("s-1", "doc-1", "abstract", 0, "ProductA outperformed ProductB in trials."),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        ("s-2", "doc-2", "results", 1, "ProductA and ProductB were both effective."),
    )
    con.execute(
        "INSERT INTO co_mentions (doc_id, product_a, product_b, count) VALUES (?, ?, ?, ?)",
        ("doc-1", "ProductA", "ProductB", 3),
    )
    con.execute(
        "INSERT INTO co_mentions (doc_id, product_a, product_b, count) VALUES (?, ?, ?, ?)",
        ("doc-2", "ProductA", "ProductB", 1),
    )
    con.execute(
        "INSERT INTO co_mentions_sentences (doc_id, sentence_id, product_a, product_b, count) VALUES (?, ?, ?, ?, ?)",
        ("doc-1", "s-1", "ProductA", "ProductB", 2),
    )
    con.execute(
        "INSERT INTO co_mentions_sentences (doc_id, sentence_id, product_a, product_b, count) VALUES (?, ?, ?, ?, ?)",
        ("doc-2", "s-2", "ProductA", "ProductB", 1),
    )
    con.execute(
        "INSERT INTO sentence_events (doc_id, sentence_id, product_a, product_b, comparative_terms, relationship_types, risk_terms, study_context, matched_terms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "doc-1",
            "s-1",
            "ProductA",
            "ProductB",
            "outperformed",
            "improves",
            "",
            "trial",
            "ProductA vs ProductB",
        ),
    )
    con.execute(
        "INSERT INTO sentence_events (doc_id, sentence_id, product_a, product_b, comparative_terms, relationship_types, risk_terms, study_context, matched_terms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "doc-2",
            "s-2",
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
    con.close()


def _write_weight_config(path: Path) -> None:
    weights = {"randomized controlled trial": 1.4, "review": 1.0, "other": 1.0}
    path.write_text(json.dumps(weights), encoding="utf-8")


def test_query_comentions_cli_outputs_ranked_pairs(tmp_path, capsys, monkeypatch) -> None:
    db_path = tmp_path / "comentions.sqlite"
    _seed_cli_db(db_path)
    weights_path = tmp_path / "weights.json"
    _write_weight_config(weights_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "query_comentions.py",
            "--db",
            str(db_path),
            "--limit",
            "5",
            "--study-weight-config",
            str(weights_path),
        ],
    )
    query_comentions.main()

    output = capsys.readouterr().out
    assert "Top co-mentions (doc-level):" in output
    assert "ProductA | ProductB" in output


def test_show_sentence_evidence_cli_outputs_sentences(tmp_path, capsys, monkeypatch) -> None:
    db_path = tmp_path / "evidence.sqlite"
    _seed_cli_db(db_path)
    weights_path = tmp_path / "weights.json"
    _write_weight_config(weights_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "show_sentence_evidence.py",
            "--db",
            str(db_path),
            "--product-a",
            "ProductA",
            "--product-b",
            "ProductB",
            "--limit",
            "5",
            "--study-weight-config",
            str(weights_path),
        ],
    )
    show_sentence_evidence.main()

    output = capsys.readouterr().out
    assert "ProductA vs ProductB" in output
    assert "Weights:" in output
