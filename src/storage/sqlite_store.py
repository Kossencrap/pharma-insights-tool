from __future__ import annotations

import json
import sqlite3
from datetime import date
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
    CREATE TABLE IF NOT EXISTS co_mentions_sentences (
        doc_id TEXT NOT NULL,
        sentence_id TEXT NOT NULL,
        product_a TEXT NOT NULL,
        product_b TEXT NOT NULL,
        count INTEGER DEFAULT 1,
        PRIMARY KEY (doc_id, sentence_id, product_a, product_b),
        FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE,
        FOREIGN KEY (sentence_id) REFERENCES sentences(sentence_id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sentence_events (
        doc_id TEXT NOT NULL,
        sentence_id TEXT NOT NULL,
        product_a TEXT NOT NULL,
        product_b TEXT NOT NULL,
        comparative_terms TEXT,
        relationship_types TEXT,
        risk_terms TEXT,
        study_context TEXT,
        matched_terms TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (doc_id, sentence_id, product_a, product_b),
        FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE,
        FOREIGN KEY (sentence_id) REFERENCES sentences(sentence_id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_status (
        status_key TEXT PRIMARY KEY,
        last_publication_date TEXT,
        last_pmid TEXT,
        last_run_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_documents_pmid ON documents(pmid)
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
    """
    CREATE INDEX IF NOT EXISTS idx_mentions_sentence ON product_mentions(sentence_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_co_mentions_pair ON co_mentions(product_a, product_b)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_co_mentions_sentences_pair ON co_mentions_sentences(product_a, product_b)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_co_mentions_sentences_doc ON co_mentions_sentences(doc_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_co_mentions_sentences_sentence ON co_mentions_sentences(sentence_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sentence_events_doc ON sentence_events(doc_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sentence_events_sentence ON sentence_events(sentence_id)
    """,
]


def init_db(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    _ensure_co_mentions_schema(conn)
    _ensure_co_mentions_sentences_schema(conn)
    _ensure_sentence_events_schema(conn)
    _ensure_ingest_status_schema(conn)

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


def _ensure_co_mentions_sentences_schema(conn: sqlite3.Connection) -> None:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='co_mentions_sentences'"
    )
    if not cur.fetchone():
        return

    rows = conn.execute("PRAGMA table_info(co_mentions_sentences)").fetchall()
    columns = [r[1] for r in rows]
    expected_columns = ["doc_id", "sentence_id", "product_a", "product_b", "count"]
    expected_pk_positions = {
        "doc_id": 1,
        "sentence_id": 2,
        "product_a": 3,
        "product_b": 4,
    }

    pk_matches = all(
        row[5] == expected_pk_positions.get(row[1], 0) for row in rows
    )

    if columns != expected_columns or not pk_matches:
        conn.execute("DROP TABLE co_mentions_sentences")


def _ensure_sentence_events_schema(conn: sqlite3.Connection) -> None:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sentence_events'"
    )
    if not cur.fetchone():
        return

    rows = conn.execute("PRAGMA table_info(sentence_events)").fetchall()
    columns = [r[1] for r in rows]
    expected_columns = [
        "doc_id",
        "sentence_id",
        "product_a",
        "product_b",
        "comparative_terms",
        "relationship_types",
        "risk_terms",
        "study_context",
        "matched_terms",
        "created_at",
    ]
    expected_pk_positions = {
        "doc_id": 1,
        "sentence_id": 2,
        "product_a": 3,
        "product_b": 4,
    }

    pk_matches = all(row[5] == expected_pk_positions.get(row[1], 0) for row in rows)

    if columns != expected_columns or not pk_matches:
        conn.execute("DROP TABLE sentence_events")


def _ensure_ingest_status_schema(conn: sqlite3.Connection) -> None:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_status'"
    )
    if cur.fetchone():
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_status (
            status_key TEXT PRIMARY KEY,
            last_publication_date TEXT,
            last_pmid TEXT,
            last_run_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


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


def insert_sentence_events(
    conn: sqlite3.Connection,
    events: Iterable[
        Tuple[
            str,
            str,
            str,
            str,
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
        ]
    ],
) -> None:
    """Persist sentence-level context labels for product pairs."""

    conn.executemany(
        """
        INSERT OR REPLACE INTO sentence_events (
            doc_id, sentence_id, product_a, product_b,
            comparative_terms, relationship_types, risk_terms, study_context, matched_terms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                doc_id,
                sentence_id,
                product_a,
                product_b,
                comparative_terms,
                relationship_types,
                risk_terms,
                study_context,
                matched_terms,
            )
            for (
                doc_id,
                sentence_id,
                product_a,
                product_b,
                comparative_terms,
                relationship_types,
                risk_terms,
                study_context,
                matched_terms,
            ) in events
        ),
    )


def insert_co_mentions_sentences(
    conn: sqlite3.Connection,
    doc_id: str,
    co_mentions_sentence_rows: Iterable[Tuple[str, str, str, int]],
) -> None:
    """
    Insert sentence-level co-mentions.

    Each row should be a tuple of ``(sentence_id, product_a, product_b, count)``;
    ``doc_id`` is provided separately to keep the row shape focused on the
    sentence-level evidence.
    """
    conn.executemany(
        """
        INSERT OR REPLACE INTO co_mentions_sentences (
            doc_id, sentence_id, product_a, product_b, count
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            (doc_id, sentence_id, a, b, count)
            for sentence_id, a, b, count in co_mentions_sentence_rows
        ),
    )


def get_ingest_status(
    conn: sqlite3.Connection, status_key: str
) -> Optional[tuple[Optional[date], Optional[str]]]:
    row = conn.execute(
        "SELECT last_publication_date, last_pmid FROM ingest_status WHERE status_key = ?",
        (status_key,),
    ).fetchone()
    if not row:
        return None

    last_date = date.fromisoformat(row[0]) if row[0] else None
    return last_date, row[1]


def update_ingest_status(
    conn: sqlite3.Connection,
    status_key: str,
    *,
    last_publication_date: Optional[date],
    last_pmid: Optional[str],
) -> None:
    conn.execute(
        """
        INSERT INTO ingest_status (status_key, last_publication_date, last_pmid, last_run_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(status_key) DO UPDATE SET
            last_publication_date=excluded.last_publication_date,
            last_pmid=excluded.last_pmid,
            last_run_at=CURRENT_TIMESTAMP
        """,
        (
            status_key,
            last_publication_date.isoformat() if last_publication_date else None,
            last_pmid,
        ),
    )
