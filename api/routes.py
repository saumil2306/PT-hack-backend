"""
API Routes
==========
FastAPI router exposing endpoints for PDF upload, job status,
and pipeline result retrieval.
"""

import uuid
from typing import Any

from fastapi import APIRouter, File, UploadFile, HTTPException

from schemas.api_schemas import (
    UploadResponse,
    JobStatus,
    PipelineResult,
)
from services.pdf_service import save_pdf
from agents.orchestrator import run_pipeline

router = APIRouter()

# In-memory job store (swap for Redis / Snowflake in production)
_jobs: dict[str, dict[str, Any]] = {}


@router.post("/upload", response_model=UploadResponse)
async def upload_pdfs(files: list[UploadFile] = File(...)):
    """
    Accept one or more PDF files, store them in Snowflake,
    and trigger the agent pipeline for each document.
    """
    job_id = str(uuid.uuid4())
    doc_ids: list[str] = []
    filenames: list[str] = []

    for f in files:
        contents = await f.read()
        doc_id = save_pdf(f.filename or "unnamed.pdf", contents)
        doc_ids.append(doc_id)
        filenames.append(f.filename or "unnamed.pdf")

    # Store job metadata
    _jobs[job_id] = {
        "doc_ids": doc_ids,
        "status": "processing",
        "results": [],
    }

    # Run pipeline for each document (sequential for now)
    for doc_id in doc_ids:
        result = run_pipeline(doc_id)
        _jobs[job_id]["results"].append(result)

    _jobs[job_id]["status"] = "completed"

    return UploadResponse(job_id=job_id, filenames=filenames)


@router.get("/status/{job_id}", response_model=JobStatus)
async def get_status(job_id: str):
    """Return the current processing status for a job."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return JobStatus(
        job_id=job_id,
        status=job["status"],
        current_agent=None,
        progress_pct=100.0 if job["status"] == "completed" else 50.0,
    )


@router.get("/results/{job_id}")
async def get_results(job_id: str):
    """Return the full pipeline results for a completed job."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job["status"] != "completed":
        raise HTTPException(status_code=202, detail="Job still processing.")
    return {
        "job_id": job_id,
        "status": job["status"],
        "results": job["results"],
    }
