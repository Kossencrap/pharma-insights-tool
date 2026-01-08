from __future__ import annotations

import json
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
    product_a_alias: Optional[str]
    product_b: str
    product_b_alias: Optional[str]
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
    context_rule_hits: tuple[str, ...] = ()
    indications: tuple[str, ...] = ()
    narrative_type: Optional[str] = None
    narrative_subtype: Optional[str] = None
    narrative_confidence: Optional[float] = None
    sentiment_label: Optional[str] = None
    sentiment_score: Optional[float] = None
    sentiment_model: Optional[str] = None
    sentiment_inference_ts: Optional[str] = None

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
            "product_a_alias": self.product_a_alias,
            "product_b": self.product_b,
            "product_b_alias": self.product_b_alias,
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
            "context_rule_hits": list(self.context_rule_hits),
            "indications": list(self.indications),
            "narrative_type": self.narrative_type,
            "narrative_subtype": self.narrative_subtype,
            "narrative_confidence": self.narrative_confidence,
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
    narrative_type: Optional[str] = None,
    narrative_subtype: Optional[str] = None,
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
    narrative_type_expr = (
        "se.narrative_type" if "narrative_type" in columns else "NULL"
    )
    narrative_subtype_expr = (
        "se.narrative_subtype" if "narrative_subtype" in columns else "NULL"
    )
    narrative_conf_expr = (
        "se.narrative_confidence" if "narrative_confidence" in columns else "NULL"
    )
    has_indications = (
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sentence_indications'"
        ).fetchone()
        is not None
    )
    indication_expr = (
        """
        (
            SELECT GROUP_CONCAT(si.indication_canonical, '|')
            FROM sentence_indications si
            WHERE si.doc_id = cms.doc_id
              AND si.sentence_id = cms.sentence_id
        )
        """
        if has_indications
        else "NULL"
    )
    query = [
        f"""
        SELECT cms.doc_id,
               cms.sentence_id,
               cms.product_a,
               (
                   SELECT alias_matched
                   FROM product_mentions pm
                   WHERE pm.doc_id = cms.doc_id
                     AND pm.sentence_id = cms.sentence_id
                     AND lower(pm.product_canonical) = lower(cms.product_a)
                   ORDER BY pm.start_char
                   LIMIT 1
               ) AS product_a_alias,
               cms.product_b,
               (
                   SELECT alias_matched
                   FROM product_mentions pm
                   WHERE pm.doc_id = cms.doc_id
                     AND pm.sentence_id = cms.sentence_id
                     AND lower(pm.product_canonical) = lower(cms.product_b)
                   ORDER BY pm.start_char
                   LIMIT 1
               ) AS product_b_alias,
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
        se.context_rule_hits,
        {indication_expr} AS indications,
               {narrative_type_expr},
               {narrative_subtype_expr},
               {narrative_conf_expr},
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
    if narrative_type:
        query.append("AND se.narrative_type = ?")
        params.append(narrative_type)
    if narrative_subtype:
        query.append("AND se.narrative_subtype = ?")
        params.append(narrative_subtype)

    query.append("ORDER BY d.publication_date DESC, cms.doc_id, cms.sentence_id LIMIT ?")
    params.append(limit)

    cur = conn.execute("\n".join(query), params)
    rows: List[SentenceEvidence] = []
    for row in cur.fetchall():
        (
            doc_id,
            sentence_id,
            product_a,
            product_a_alias,
            product_b,
            product_b_alias,
            count,
            sentence_text,
            section,
            sent_index,
            publication_date,
            journal,
            recency_weight,
            study_type,
            study_type_weight,
            combined_weight,
            comparative_terms,
            relationship_types,
            risk_terms,
            study_context,
            matched_terms,
            context_rule_hits_raw,
            indication_list,
            narrative_type_val,
            narrative_subtype_val,
            narrative_confidence_val,
            sentiment_label_val,
            sentiment_score_val,
            sentiment_model_val,
            sentiment_ts_val,
        ) = row

        labels: list[str] = []
        seen: set[str] = set()
        for value in (
            comparative_terms,
            relationship_types,
            risk_terms,
            study_context,
        ):
            for label in _split_labels(value):
                key = label.lower()
                if key in seen:
                    continue
                seen.add(key)
                labels.append(label)

        indications = tuple(
            sorted(
                {
                    item.strip()
                    for item in (indication_list or "").split("|")
                    if item and item.strip()
                }
            )
        )

        context_rules: tuple[str, ...] = ()
        if context_rule_hits_raw:
            try:
                parsed_rules = json.loads(context_rule_hits_raw)
                if isinstance(parsed_rules, list):
                    context_rules = tuple(str(rule) for rule in parsed_rules)
            except json.JSONDecodeError:
                context_rules = tuple(
                    item.strip()
                    for item in (context_rule_hits_raw or "").split(",")
                    if item.strip()
                )

        rows.append(
            SentenceEvidence(
                doc_id=doc_id,
                sentence_id=sentence_id,
                product_a=product_a,
                product_a_alias=product_a_alias,
                product_b=product_b,
                product_b_alias=product_b_alias,
                count=int(count or 0),
                sentence_text=sentence_text,
                section=section,
                sent_index=sent_index,
                publication_date=publication_date,
                journal=journal,
                recency_weight=recency_weight,
                study_type=study_type,
                study_type_weight=study_type_weight,
                combined_weight=combined_weight,
                labels=labels,
                matched_terms=matched_terms,
                context_rule_hits=context_rules,
                indications=indications,
                narrative_type=narrative_type_val,
                narrative_subtype=narrative_subtype_val,
                narrative_confidence=narrative_confidence_val,
                sentiment_label=sentiment_label_val,
                sentiment_score=sentiment_score_val,
                sentiment_model=sentiment_model_val,
                sentiment_inference_ts=sentiment_ts_val,
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
