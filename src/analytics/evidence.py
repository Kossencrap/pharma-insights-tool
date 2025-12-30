from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import List, Mapping, Optional, Sequence

from src.analytics.weights import STUDY_TYPE_ALIASES


def _split_labels(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class SentenceEvidence:
    doc_id: str
    sentence_id: str
    product_a: str
    product_b: str
    sentence_text: str
    publication_date: Optional[str]
    journal: Optional[str]
    section: Optional[str]
    sent_index: Optional[int]
    count: int
    recency_weight: Optional[float]
    study_type: Optional[str]
    study_type_weight: Optional[float]
    combined_weight: Optional[float]
    labels: List[str]
    matched_terms: Optional[str]
    sentiment_label: Optional[str]
    sentiment_score: Optional[float]
    sentiment_model: Optional[str]
    sentiment_inference_ts: Optional[str]

    @property
    def evidence_weight(self) -> float:
        base_weight = self.combined_weight or self.recency_weight or 1.0
        return base_weight * max(self.count, 1)

    def confidence_breakdown(
        self, study_weight_lookup: Mapping[str, float] | None = None
    ) -> dict:
        resolved_study_weight = self.study_type_weight or resolve_study_weight(
            self.study_type, study_weight_lookup
        )
        base_weight = self.recency_weight or 1.0
        combined_weight = self.combined_weight or base_weight * (resolved_study_weight or 1.0)
        return {
            "recency_weight": self.recency_weight,
            "study_type": self.study_type,
            "study_type_weight": resolved_study_weight,
            "mention_count": max(self.count, 1),
            "combined_weight": combined_weight,
            "final_confidence": combined_weight * max(self.count, 1),
        }

    def to_dict(
        self,
        *,
        study_weight_lookup: Mapping[str, float] | None = None,
        include_confidence: bool = False,
    ) -> dict:
        payload = {
            "doc_id": self.doc_id,
            "sentence_id": self.sentence_id,
            "product_a": self.product_a,
            "product_b": self.product_b,
            "sentence_text": self.sentence_text,
            "publication_date": self.publication_date,
            "journal": self.journal,
            "section": self.section,
            "sent_index": self.sent_index,
            "count": self.count,
            "recency_weight": self.recency_weight,
            "study_type": self.study_type,
            "study_type_weight": self.study_type_weight,
            "combined_weight": self.combined_weight,
            "evidence_weight": self.evidence_weight,
            "labels": self.labels,
            "matched_terms": self.matched_terms,
            "sentiment_label": self.sentiment_label,
            "sentiment_score": self.sentiment_score,
            "sentiment_model": self.sentiment_model,
            "sentiment_inference_ts": self.sentiment_inference_ts,
        }

        if include_confidence:
            payload["confidence_breakdown"] = self.confidence_breakdown(
                study_weight_lookup
            )

        return payload


def fetch_sentence_evidence(
    conn: sqlite3.Connection,
    *,
    product_a: Optional[str] = None,
    product_b: Optional[str] = None,
    pub_after: Optional[str] = None,
    limit: int = 200,
) -> List[SentenceEvidence]:
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(sentence_events)").fetchall()
    }
    sentiment_label_expr = (
        "se.sentiment_label" if "sentiment_label" in columns else "NULL"
    )
    sentiment_score_expr = (
        "se.sentiment_score" if "sentiment_score" in columns else "NULL"
    )
    sentiment_model_expr = (
        "se.sentiment_model" if "sentiment_model" in columns else "NULL"
    )
    sentiment_ts_expr = (
        "se.sentiment_inference_ts"
        if "sentiment_inference_ts" in columns
        else "NULL"
    )
    query = [
        f"""
        SELECT cms.doc_id,
               cms.sentence_id,
               cms.product_a,
               cms.product_b,
               cms.count,
               s.text,
               s.section,
               s.sent_index,
               d.publication_date,
               d.journal,
               dw.recency_weight,
               dw.study_type,
               dw.study_type_weight,
               dw.combined_weight,
               se.comparative_terms,
               se.relationship_types,
               se.risk_terms,
               se.study_context,
               se.matched_terms,
               {sentiment_label_expr},
               {sentiment_score_expr},
               {sentiment_model_expr},
               {sentiment_ts_expr}
        FROM co_mentions_sentences cms
        JOIN sentences s ON cms.sentence_id = s.sentence_id
        JOIN documents d ON cms.doc_id = d.doc_id
        LEFT JOIN document_weights dw ON cms.doc_id = dw.doc_id
        LEFT JOIN sentence_events se
          ON cms.doc_id = se.doc_id
         AND cms.sentence_id = se.sentence_id
         AND cms.product_a = se.product_a
         AND cms.product_b = se.product_b
        WHERE 1=1
        """,
    ]

    params: list[object] = []
    if product_a:
        query.append("AND lower(cms.product_a) = lower(?)")
        params.append(product_a)

    if product_b:
        query.append("AND lower(cms.product_b) = lower(?)")
        params.append(product_b)

    if pub_after:
        query.append("AND d.publication_date >= ?")
        params.append(pub_after)

    query.append("ORDER BY d.publication_date DESC, cms.doc_id, cms.sentence_id LIMIT ?")
    params.append(limit)

    cur = conn.execute("\n".join(query), params)
    rows: List[SentenceEvidence] = []
    for row in cur.fetchall():
        labels: list[str] = []
        seen: set[str] = set()
        for idx in range(14, 18):
            for label in _split_labels(row[idx]):
                key = label.lower()
                if key in seen:
                    continue
                seen.add(key)
                labels.append(label)

        rows.append(
            SentenceEvidence(
                doc_id=row[0],
                sentence_id=row[1],
                product_a=row[2],
                product_b=row[3],
                count=int(row[4] or 0),
                sentence_text=row[5],
                section=row[6],
                sent_index=row[7],
                publication_date=row[8],
                journal=row[9],
                recency_weight=row[10],
                study_type=row[11],
                study_type_weight=row[12],
                combined_weight=row[13],
                labels=labels,
                matched_terms=row[18],
                sentiment_label=row[19],
                sentiment_score=row[20],
                sentiment_model=row[21],
                sentiment_inference_ts=row[22],
            )
        )

    return rows


def serialize_sentence_evidence(
    evidence_rows: Sequence[SentenceEvidence],
    *,
    study_weight_lookup: Mapping[str, float] | None = None,
    include_confidence: bool = False,
) -> List[dict]:
    return [
        row.to_dict(
            study_weight_lookup=study_weight_lookup, include_confidence=include_confidence
        )
        for row in evidence_rows
    ]


def resolve_study_weight(
    study_type: str | None, study_weight_lookup: Mapping[str, float] | None
) -> float | None:
    if not study_weight_lookup:
        return None
    if not study_type:
        return study_weight_lookup.get("other")
    normalized = study_type.strip().lower()
    canonical = STUDY_TYPE_ALIASES.get(normalized, normalized)
    return study_weight_lookup.get(canonical, study_weight_lookup.get("other"))


def explain_confidence(
    evidence: SentenceEvidence, study_weight_lookup: Mapping[str, float] | None = None
) -> dict:
    return evidence.confidence_breakdown(study_weight_lookup)
