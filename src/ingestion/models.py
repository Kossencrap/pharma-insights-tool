from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


class EuropePMCSearchResult(BaseModel):
    """
    Minimal normalized representation of a Europe PMC search hit.
    """
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    doi: Optional[str] = None

    title: str = ""
    abstract: Optional[str] = None

    journal: Optional[str] = None
    publication_date: Optional[date] = None
    pub_year: Optional[int] = None

    author_string: Optional[str] = None
    first_author: Optional[str] = None

    is_open_access: Optional[bool] = None
    cited_by_count: Optional[int] = None

    study_design: Optional[str] = None
    study_phase: Optional[str] = None
    sample_size: Optional[int] = None

    source: Optional[str] = Field(default=None, description="Europe PMC source field, e.g. 'MED' or 'PMC'")
    raw: dict = Field(default_factory=dict, description="Raw Europe PMC record for audit/debug.")
