"""
API Schemas
===========
Pydantic models for request / response validation on API endpoints.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


# ── Upload ────────────────────────────────────────────────
class UploadResponse(BaseModel):
    """Response returned after a successful PDF upload."""

    job_id: str
    filenames: list[str]
    message: str = "PDFs received — processing started."


# ── Job Status ────────────────────────────────────────────
class JobStatus(BaseModel):
    """Current status of a processing pipeline job."""

    job_id: str
    status: str  # pending | ocr | carbon | audit | completed | error
    current_agent: Optional[str] = None
    progress_pct: float = 0.0
    updated_at: datetime = datetime.utcnow()


# ── Agent-specific Results ────────────────────────────────
class ExtractionResult(BaseModel):
    """Fields extracted from a document by the OCR agent."""

    doc_id: str
    fields: dict[str, Any]


class CarbonCalculation(BaseModel):
    """Carbon output computed by the carbon agent."""

    doc_id: str
    total_carbon_kg: float
    breakdown: list[dict[str, Any]]


class AuditReportResponse(BaseModel):
    """Final audit produced by the auditor agent."""

    doc_id: str
    hotspots: list[dict[str, Any]]
    recommendations: list[str]
    total_emissions: float
    risk_level: str


# ── Combined Pipeline Result ─────────────────────────────
class PipelineResult(BaseModel):
    """Aggregated result returned to the frontend."""

    job_id: str
    documents: list[str]
    extraction: list[ExtractionResult]
    carbon: list[CarbonCalculation]
    audit: list[AuditReportResponse]
