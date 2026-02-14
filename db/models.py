"""
Database Models
===============
Pydantic models representing rows in Snowflake tables.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class Document(BaseModel):
    """A single uploaded PDF document."""

    doc_id: str
    filename: str
    upload_ts: datetime = Field(default_factory=datetime.utcnow)
    status: str = "pending"


class ExtractedField(BaseModel):
    """A key-value field extracted from a document by the OCR agent."""

    id: str
    doc_id: str
    field_name: str
    field_value: str
    confidence: float = 1.0
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CarbonResult(BaseModel):
    """Carbon emission calculation for a specific category."""

    id: str
    doc_id: str
    category: str
    emission_factor: float
    quantity: float
    carbon_output: float
    unit: str = "kgCO2e"
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AuditReport(BaseModel):
    """Final audit report with hotspots and recommendations."""

    id: str
    doc_id: str
    hotspots: list[dict[str, Any]] = []
    recommendations: list[str] = []
    total_emissions: float = 0.0
    risk_level: str = "low"
    created_at: datetime = Field(default_factory=datetime.utcnow)
