from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from src.structuring.models import Document, Sentence

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS documents (
        doc_id TEXT PRIMARY KEY,
        source TEXT,
        pmid TEXT,
        pmcid TEXT,
        doi TEXT,
        title TEXT,
        abstract TEXT,
        journal TEXT,
        publication_date TEXT,
        pub_year INTEGER,
        raw_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sentences (
        sentence_id TEXT PRIMARY KEY,
        doc_id TEXT NOT NULL,
        section TEXT,
        sent_index INTEGER,
        text TEXT,
        start_char INTEGER,
        end_char INTEGER,
        FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS product_mentions (
        mention_id TEXT PRIMARY KEY,
        doc_id TEXT NOT NULL,
        sentence_id TEXT NOT NULL,
        product_canonical TEXT,
        alias_matched TEXT,
        start_char INTEGER,
        end_char INTEGER,
        match_method TEXT,
        FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE,
        FOREIGN KEY (sentence_id) REFERENCES sentences(sentence_id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS co_mentions (
        doc_id TEXT NOT NULL,
        product_a TEXT NOT NULL,
        product_b TEXT NOT NULL,
        count INTEGER DEFAULT 1,
        PRIMARY KEY (doc_id, product_a, product_b),
        FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sentences_doc ON sentences(doc_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_mentions_product ON product_mentions(product_canonical)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_mentions_doc ON product_mentions(doc_id)
    """,
]


def init_db(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    _ensure_co_mentions_schema(conn)

    for stmt in CREATE_TABLES_SQL:
        conn.execute(stmt)
    conn.commit()
    return conn


def _ensure_co_mentions_schema(conn: sqlite3.Connection) -> None:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='co_mentions'"
    )
    if not cur.fetchone():
        return

    rows = conn.execute("PRAGMA table_info(co_mentions)").fetchall()
    columns = [r[1] for r in rows]
    expected_columns = ["doc_id", "product_a", "product_b", "count"]

    if columns != expected_columns:
        conn.execute("DROP TABLE co_mentions")


def upsert_document(conn: sqlite3.Connection, document: Document, raw_json: Optional[dict] = None) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO documents (
            doc_id, source, pmid, pmcid, doi, title, abstract, journal, publication_date, pub_year, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            document.doc_id,
            document.source,
            document.pmid,
            document.pmcid,
            document.doi,
            document.title,
            document.abstract,
            document.journal,
            document.publication_date.isoformat() if document.publication_date else None,
            document.pub_year,
            json.dumps(raw_json) if raw_json is not None else None,
        ),
    )


def insert_sentences(conn: sqlite3.Connection, doc_id: str, sentences: Iterable[Tuple[str, Sentence]]) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO sentences (
            sentence_id, doc_id, section, sent_index, text, start_char, end_char
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                sentence_id,
                doc_id,
                sentence.section,
                sentence.index,
                sentence.text,
                sentence.start_char,
                sentence.end_char,
            )
            for sentence_id, sentence in sentences
        ),
    )


def insert_mentions(
    conn: sqlite3.Connection,
    doc_id: str,
    sentence_id: str,
    mentions: Iterable[Tuple[str, str, str, int, int, str]],
) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO product_mentions (
            mention_id, doc_id, sentence_id, product_canonical, alias_matched, start_char, end_char, match_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                mention_id,
                doc_id,
                sentence_id,
                canonical,
                alias,
                start,
                end,
                match_method,
            )
            for mention_id, canonical, alias, start, end, match_method in mentions
        ),
    )


def insert_co_mentions(
    conn: sqlite3.Connection, doc_id: str, co_mentions: Iterable[Tuple[str, str, int]]
) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO co_mentions (
            doc_id, product_a, product_b, count
        ) VALUES (?, ?, ?, ?)
        """,
        ((doc_id, a, b, count) for a, b, count in co_mentions),
    )
