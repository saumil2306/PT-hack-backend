"""
Orchestrator — LangGraph Pipeline
===================================
A LangGraph StateGraph that chains the three agents in a fixed
sequential workflow:

  Router → OCR Agent → Carbon Agent → Auditor Agent → END

The router node inspects the incoming state and kicks off the pipeline.
"""

from __future__ import annotations

import operator
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from agents.ocr_agent import extract_fields
from agents.carbon_agent import calculate_carbon
from agents.auditor_agent import audit
from services.pdf_service import get_pdf, update_status


# ── Pipeline State ────────────────────────────────────────
class PipelineState(TypedDict, total=False):
    """Shared state passed between nodes in the LangGraph."""

    doc_id: str
    pdf_bytes: bytes
    extracted_fields: dict[str, Any]
    carbon_result: dict[str, Any]
    audit_report: dict[str, Any]
    error: str | None


# ── Node Functions ────────────────────────────────────────
def router_node(state: PipelineState) -> PipelineState:
    """
    Router / entry node.
    Loads the PDF binary from Snowflake and prepares state for Agent 1.
    """
    doc_id = state["doc_id"]
    try:
        _filename, pdf_bytes = get_pdf(doc_id)
        update_status(doc_id, "ocr")
        return {**state, "pdf_bytes": pdf_bytes}
    except Exception as exc:
        return {**state, "error": str(exc)}


def ocr_node(state: PipelineState) -> PipelineState:
    """Agent 1 — OCR extraction."""
    if state.get("error"):
        return state
    doc_id = state["doc_id"]
    pdf_bytes = state["pdf_bytes"]
    try:
        fields = extract_fields(doc_id, pdf_bytes)
        update_status(doc_id, "carbon")
        return {**state, "extracted_fields": fields}
    except Exception as exc:
        return {**state, "error": str(exc)}


def carbon_node(state: PipelineState) -> PipelineState:
    """Agent 2 — Carbon footprint calculation."""
    if state.get("error"):
        return state
    doc_id = state["doc_id"]
    extracted = state["extracted_fields"]
    try:
        result = calculate_carbon(doc_id, extracted)
        update_status(doc_id, "audit")
        return {**state, "carbon_result": result}
    except Exception as exc:
        return {**state, "error": str(exc)}


def auditor_node(state: PipelineState) -> PipelineState:
    """Agent 3 — Audit & hotspot analysis."""
    if state.get("error"):
        return state
    doc_id = state["doc_id"]
    carbon = state["carbon_result"]
    try:
        report = audit(doc_id, carbon)
        update_status(doc_id, "completed")
        return {**state, "audit_report": report}
    except Exception as exc:
        return {**state, "error": str(exc)}


# ── Build the Graph ───────────────────────────────────────
def build_pipeline() -> StateGraph:
    """Construct and compile the agent pipeline graph."""
    graph = StateGraph(PipelineState)

    graph.add_node("router", router_node)
    graph.add_node("ocr", ocr_node)
    graph.add_node("carbon", carbon_node)
    graph.add_node("auditor", auditor_node)

    graph.set_entry_point("router")
    graph.add_edge("router", "ocr")
    graph.add_edge("ocr", "carbon")
    graph.add_edge("carbon", "auditor")
    graph.add_edge("auditor", END)

    return graph.compile()


# Pre-compiled pipeline instance
pipeline = build_pipeline()


def run_pipeline(doc_id: str) -> PipelineState:
    """Execute the full agent pipeline for a given document."""
    initial_state: PipelineState = {"doc_id": doc_id}
    result = pipeline.invoke(initial_state)
    return result
