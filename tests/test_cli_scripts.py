import json
import sys
from pathlib import Path

from src.storage import init_db

from scripts import aggregate_metrics, query_comentions, show_sentence_evidence
from scripts import label_sentence_events


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


def test_aggregate_metrics_cli_emits_narrative_change(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "metrics.sqlite"
    _seed_cli_db(db_path)

    outdir = tmp_path / "metrics"
    outdir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "aggregate_metrics.py",
            "--db",
            str(db_path),
            "--outdir",
            str(outdir),
            "--freq",
            "W",
            "--change-lookback",
            "2",
            "--change-min-ratio",
            "0.4",
            "--change-min-count",
            "1"
        ],
    )

    aggregate_metrics.main()

    change_file = outdir / "narratives_change_w.parquet"
    assert change_file.exists()
    directional_file = outdir / "directional_w.parquet"
    assert directional_file.exists()

    try:
        import pandas as pd  # type: ignore

        df = pd.read_parquet(change_file)
        if not df.empty:
            assert "status" in df.columns
        directional = pd.read_parquet(directional_file)
        if not directional.empty:
            assert {"product", "direction_type", "role"}.issubset(directional.columns)
    except Exception:
        # When pandas/pyarrow are unavailable, the file is JSON; just ensure it has content.
        payload = change_file.read_text(encoding="utf-8")
        assert payload.strip()
        assert directional_file.read_text(encoding="utf-8").strip()


def _seed_labeling_db(db_path: Path) -> None:
    con = init_db(db_path)
    con.execute(
        "INSERT INTO documents (doc_id, title, publication_date, pub_year, raw_json) VALUES (?, ?, ?, ?, ?)",
        ("doc-l1", "Label Doc One", "2024-02-10", 2024, "{}"),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-pref",
            "doc-l1",
            "results",
            0,
            "DrugX was preferred over DrugY and showed superior tolerability in trials.",
        ),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-switch",
            "doc-l1",
            "results",
            1,
            "Patients switched to DrugY after discontinuing DrugX due to hypoglycemia.",
        ),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-parenthetical",
            "doc-l1",
            "results",
            2,
            "Therapy options (DrugX, DrugY) were discussed.",
        ),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-definition",
            "doc-l1",
            "results",
            3,
            "DrugX (DrugY) was defined as maintenance therapy in this registry study.",
        ),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-citation",
            "doc-l1",
            "results",
            4,
            "(Smith et al., 2020) DrugX and DrugY observations.",
        ),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-table",
            "doc-l1",
            "results",
            5,
            "Table 2 summarizes DrugX versus DrugY baseline characteristics.",
        ),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-list",
            "doc-l1",
            "results",
            6,
            "Predictors for DrugX and DrugY included age, sex, BMI, diabetes, and hypertension.",
        ),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        (
            "sent-list-penalty",
            "doc-l1",
            "results",
            7,
            "DrugX vs DrugY improved mortality, including reductions in blood pressure, NT-proBNP, and edema.",
        ),
    )
    con.executemany(
        "INSERT INTO co_mentions_sentences (doc_id, sentence_id, product_a, product_b, count) VALUES (?, ?, ?, ?, ?)",
        [
            ("doc-l1", "sent-pref", "DrugX", "DrugY", 1),
            ("doc-l1", "sent-switch", "DrugX", "DrugY", 1),
            ("doc-l1", "sent-parenthetical", "DrugX", "DrugY", 1),
            ("doc-l1", "sent-definition", "DrugX", "DrugY", 1),
            ("doc-l1", "sent-citation", "DrugX", "DrugY", 1),
            ("doc-l1", "sent-table", "DrugX", "DrugY", 1),
            ("doc-l1", "sent-list", "DrugX", "DrugY", 1),
            ("doc-l1", "sent-list-penalty", "DrugX", "DrugY", 1),
        ],
    )
    con.executemany(
        "INSERT INTO product_mentions (mention_id, doc_id, sentence_id, product_canonical, alias_matched, start_char, end_char, match_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("m-pref-x", "doc-l1", "sent-pref", "DrugX", "DrugX", 0, 5, "regex"),
            ("m-pref-y", "doc-l1", "sent-pref", "DrugY", "DrugY", 24, 29, "regex"),
            ("m-switch-x", "doc-l1", "sent-switch", "DrugX", "DrugX", 48, 53, "regex"),
            ("m-switch-y", "doc-l1", "sent-switch", "DrugY", "DrugY", 16, 21, "regex"),
            ("m-parent-x", "doc-l1", "sent-parenthetical", "DrugX", "DrugX", 16, 21, "regex"),
            ("m-parent-y", "doc-l1", "sent-parenthetical", "DrugY", "DrugY", 24, 29, "regex"),
            ("m-def-x", "doc-l1", "sent-definition", "DrugX", "DrugX", 0, 5, "regex"),
            ("m-def-y", "doc-l1", "sent-definition", "DrugY", "DrugY", 7, 12, "regex"),
            ("m-cit-x", "doc-l1", "sent-citation", "DrugX", "DrugX", 26, 31, "regex"),
            ("m-cit-y", "doc-l1", "sent-citation", "DrugY", "DrugY", 36, 41, "regex"),
            ("m-table-x", "doc-l1", "sent-table", "DrugX", "DrugX", 17, 22, "regex"),
            ("m-table-y", "doc-l1", "sent-table", "DrugY", "DrugY", 31, 36, "regex"),
            ("m-list-x", "doc-l1", "sent-list", "DrugX", "DrugX", 13, 18, "regex"),
            ("m-list-y", "doc-l1", "sent-list", "DrugY", "DrugY", 23, 28, "regex"),
            ("m-listp-x", "doc-l1", "sent-list-penalty", "DrugX", "DrugX", 0, 5, "regex"),
            ("m-listp-y", "doc-l1", "sent-list-penalty", "DrugY", "DrugY", 9, 14, "regex"),
        ],
    )
    con.commit()
    con.close()


def test_label_sentence_events_cli_writes_directional_roles(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "label.sqlite"
    _seed_labeling_db(db_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "label_sentence_events.py",
            "--db",
            str(db_path),
            "--limit",
            "10",
            "--only-missing",
        ],
    )

    label_sentence_events.main()

    con = init_db(db_path)
    rows = con.execute(
        """
        SELECT sentence_id, direction_type, product_a_role, product_b_role
        FROM sentence_events
        ORDER BY sentence_id
        """
    ).fetchall()
    con.close()

    assert rows
    pref_row = next(row for row in rows if row[0] == "sent-pref")
    assert pref_row[1] == "alternative"
    assert pref_row[2] == "favored"
    assert pref_row[3] == "disfavored"

    switch_row = next(row for row in rows if row[0] == "sent-switch")
    assert switch_row[1] == "switch"
    assert {"switch_source", "switch_destination"} == {switch_row[2], switch_row[3]}


def test_label_sentence_events_skips_parenthetical_pairs(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "label.sqlite"
    _seed_labeling_db(db_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "label_sentence_events.py",
            "--db",
            str(db_path),
            "--limit",
            "10",
            "--only-missing",
        ],
    )

    label_sentence_events.main()

    con = init_db(db_path)
    rows = con.execute(
        "SELECT sentence_id FROM sentence_events WHERE sentence_id = 'sent-parenthetical'"
    ).fetchall()
    con.close()

    assert rows == []


def test_label_sentence_events_filters_boilerplate_sentences(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "label.sqlite"
    _seed_labeling_db(db_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "label_sentence_events.py",
            "--db",
            str(db_path),
            "--limit",
            "20",
            "--only-missing",
        ],
    )

    label_sentence_events.main()

    con = init_db(db_path)
    for sid in ("sent-definition", "sent-citation", "sent-table", "sent-list"):
        assert (
            con.execute(
                "SELECT sentence_id FROM sentence_events WHERE sentence_id = ?", (sid,)
            ).fetchone()
            is None
        )
    penalty_row = con.execute(
        "SELECT narrative_confidence FROM sentence_events WHERE sentence_id = 'sent-list-penalty'"
    ).fetchone()
    con.close()
    assert penalty_row is not None
    assert penalty_row[0] is not None
    assert penalty_row[0] < 0.7
