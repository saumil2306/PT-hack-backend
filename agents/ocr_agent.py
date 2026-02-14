"""
Agent 1 — OCR Extractor
========================
Uses Gemini (vision-capable) to extract text and key fields from PDF pages.
Instead of local OCR tools, we send PDF page images directly to Gemini
and ask it to extract predefined fields.
"""

import base64
import json
from typing import Any

import fitz  # PyMuPDF — used to render PDF pages as images
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage

from config.settings import settings
from db.snowflake_client import get_connection

# ── Key fields to extract from supply-chain documents ────
EXTRACTION_FIELDS = [
    "supplier_name",
    "material_type",
    "quantity",
    "unit",
    "energy_source",
    "energy_consumption",
    "transport_mode",
    "transport_distance_km",
    "manufacturing_process",
    "waste_generated",
]

# ── Gemini LLM setup ────────────────────────────────────
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    google_api_key=settings.GEMINI_API_KEY,
    temperature=0,
)


def _pdf_pages_to_base64_images(pdf_bytes: bytes) -> list[str]:
    """Render each PDF page to a PNG and return as base64-encoded strings."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    images: list[str] = []
    for page in doc:
        pix = page.get_pixmap(dpi=200)
        img_bytes = pix.tobytes(output="png")
        images.append(base64.b64encode(img_bytes).decode("utf-8"))
    doc.close()
    return images


def extract_fields(doc_id: str, pdf_bytes: bytes) -> dict[str, Any]:
    """
    Extract key fields from a PDF using Gemini Vision.

    1. Converts PDF pages to images.
    2. Sends each image to Gemini with a structured extraction prompt.
    3. Aggregates and returns extracted data.
    4. Saves extracted fields to Snowflake.
    """
    page_images = _pdf_pages_to_base64_images(pdf_bytes)

    extraction_prompt = (
        "You are a document-data-extraction assistant. "
        "Analyze the following document page image and extract these fields "
        "if present. Return ONLY valid JSON with the field names as keys. "
        "If a field is not found, set its value to null.\n\n"
        f"Fields to extract: {json.dumps(EXTRACTION_FIELDS)}"
    )

    all_extracted: dict[str, Any] = {}

    for idx, img_b64 in enumerate(page_images):
        message = HumanMessage(
            content=[
                {"type": "text", "text": extraction_prompt},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{img_b64}"},
                },
            ]
        )
        response = llm.invoke([message])
        try:
            page_data = json.loads(response.content)
        except json.JSONDecodeError:
            page_data = {"_raw_page_text": response.content}

        # Merge — later pages can override earlier ones
        for key, val in page_data.items():
            if val is not None:
                all_extracted[key] = val

    # ── Persist to Snowflake ──────────────────────────────
    _save_extracted_fields(doc_id, all_extracted)

    return all_extracted


def _save_extracted_fields(doc_id: str, fields: dict[str, Any]) -> None:
    """Write extracted key-value pairs to the extracted_fields table."""
    import uuid

    conn = get_connection()
    try:
        cur = conn.cursor()
        for field_name, field_value in fields.items():
            cur.execute(
                """
                INSERT INTO extracted_fields (id, doc_id, field_name, field_value, confidence)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (str(uuid.uuid4()), doc_id, field_name, str(field_value), 1.0),
            )
    finally:
        conn.close()
