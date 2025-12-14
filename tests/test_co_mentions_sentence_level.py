import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.mention_extractor import MentionExtractor, co_mentions_from_sentence
from src.storage import (
    init_db,
    insert_co_mentions,
    insert_co_mentions_sentences,
    insert_mentions,
    insert_sentences,
    upsert_document,
)
from src.structuring.models import Document, Section, Sentence
from src.utils.identifiers import build_sentence_id


def test_sentence_level_co_mentions_are_persisted(tmp_path):
    db_path = tmp_path / "pharma.sqlite"
    conn = init_db(db_path)

    abstract_text = "Insulin and metformin improved outcomes."
    document = Document(
        doc_id="med:pmid:sentinel-1",
        source="MED",
        pmid="sentinel-1",
        title="Insulin plus metformin",
    )
    section = Section(
        name="abstract",
        text=abstract_text,
        sentences=[
            Sentence(
                text=abstract_text,
                index=0,
                start_char=0,
                end_char=len(abstract_text),
                section="abstract",
            )
        ],
    )
    document.sections.append(section)

    upsert_document(conn, document, raw_json={"mock": True})

    sentence_id = build_sentence_id(document.doc_id, section.name, 0)
    insert_sentences(conn, document.doc_id, [(sentence_id, section.sentences[0])])

    extractor = MentionExtractor({"insulin": ["insulin"], "metformin": ["metformin"]})
    mentions = extractor.extract(abstract_text)

    insert_mentions(
        conn,
        document.doc_id,
        sentence_id,
        [
            (
                f"{sentence_id}:{m.product_canonical}:{m.start_char}-{m.end_char}",
                m.product_canonical,
                m.alias_matched,
                m.start_char,
                m.end_char,
                m.match_method,
            )
            for m in mentions
        ],
    )

    sentence_pairs = co_mentions_from_sentence(mentions)
    insert_co_mentions_sentences(
        conn,
        document.doc_id,
        [(sentence_id, a, b, count) for a, b, count in sentence_pairs],
    )
    insert_co_mentions(conn, document.doc_id, co_mentions_from_sentence(mentions))
    conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT sentence_id FROM sentences WHERE sentence_id = ?", (sentence_id,))
    assert cur.fetchone()[0] == sentence_id

    cur.execute(
        "SELECT doc_id, sentence_id, product_a, product_b, count FROM co_mentions_sentences"
    )
    assert (
        document.doc_id,
        sentence_id,
        "insulin",
        "metformin",
        1,
    ) in cur.fetchall()

    cur.execute("SELECT doc_id, product_a, product_b, count FROM co_mentions")
    assert cur.fetchone() == (document.doc_id, "insulin", "metformin", 1)

    conn.close()
