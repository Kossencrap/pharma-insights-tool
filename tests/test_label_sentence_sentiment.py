from pathlib import Path

from src.analytics.sentiment import SentimentLabel, classify_batch
from src.storage import init_db, insert_sentence_events, insert_sentences, upsert_document
from src.structuring.models import Document, Sentence
from scripts.label_sentence_sentiment import _update_sentiment_in_db


def test_label_sentence_sentiment_updates_db(tmp_path: Path) -> None:
    db_path = tmp_path / "sentiment.sqlite"
    conn = init_db(db_path)

    doc = Document(doc_id="doc-1", source="unit-test")
    upsert_document(conn, doc)

    sentence = Sentence(
        text="The therapy improved outcomes.",
        index=0,
        start_char=0,
        end_char=30,
        section="abstract",
    )
    insert_sentences(conn, doc.doc_id, [("sent-1", sentence)])
    insert_sentence_events(
        conn,
        [
            (
                doc.doc_id,
                "sent-1",
                "ProductA",
                "ProductB",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                sentence.section,
            )
        ],
    )
    conn.commit()
    conn.close()

    labeled = classify_batch(
        [
            {
                "doc_id": doc.doc_id,
                "sentence_id": "sent-1",
                "product_a": "ProductA",
                "product_b": "ProductB",
                "sentence_text": sentence.text,
            }
        ]
    )

    _update_sentiment_in_db(db_path, labeled)

    conn = init_db(db_path)
    row = conn.execute(
        """
        SELECT sentiment_label, sentiment_score, sentiment_model, sentiment_inference_ts
        FROM sentence_events
        WHERE doc_id = ? AND sentence_id = ? AND product_a = ? AND product_b = ?
        """,
        (doc.doc_id, "sent-1", "ProductA", "ProductB"),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == SentimentLabel.POSITIVE.value
    assert row[1] is not None
    assert row[2] is not None
    assert row[3] is not None
