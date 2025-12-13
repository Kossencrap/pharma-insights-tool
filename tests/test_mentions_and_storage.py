import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.mention_extractor import MentionExtractor, co_mentions_from_sentence
from src.storage import init_db, insert_co_mentions, insert_mentions, insert_sentences, upsert_document
from src.structuring.models import Document, Sentence, Section
from src.utils.identifiers import build_sentence_id


def test_mention_extraction_and_co_mentions():
    extractor = MentionExtractor({"dupilumab": ["Dupixent"], "insulin": ["insulin"]})
    text = "Dupixent outperformed insulin in the study."

    mentions = extractor.extract(text)
    aliases = sorted([m.alias_matched.lower() for m in mentions])
    assert aliases == ["dupixent", "insulin"]

    pairs = co_mentions_from_sentence(mentions)
    assert pairs == [("dupilumab", "insulin", 1)]


def test_sqlite_store_persists_documents_sentences_and_mentions(tmp_path):
    db_path = tmp_path / "pharma.sqlite"
    conn = init_db(db_path)

    document = Document(
        doc_id="med:pmid:1",
        source="MED",
        pmid="1",
        pmcid=None,
        doi=None,
        title="Dupixent and insulin",
        abstract=None,
        publication_date=None,
        pub_year=None,
        journal=None,
    )
    section = Section(
        name="title",
        text=document.title or "",
        sentences=[
            Sentence(
                text=document.title or "",
                index=0,
                start_char=0,
                end_char=len(document.title or ""),
                section="title",
            )
        ],
    )
    document.sections.append(section)

    upsert_document(conn, document, raw_json={"mock": True})

    sentence_id = build_sentence_id(document.doc_id, "title", 0)
    insert_sentences(conn, document.doc_id, [(sentence_id, section.sentences[0])])

    extractor = MentionExtractor({"dupilumab": ["Dupixent"]})
    mentions = extractor.extract(section.sentences[0].text)

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
    insert_co_mentions(conn, document.doc_id, sentence_id, co_mentions_from_sentence(mentions))
    conn.commit()

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM documents")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT sentence_id FROM sentences")
    assert cur.fetchone()[0] == sentence_id

    cur.execute("SELECT product_canonical, alias_matched FROM product_mentions")
    assert cur.fetchone() == ("dupilumab", "Dupixent")

    conn.close()
