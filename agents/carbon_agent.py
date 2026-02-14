"""
Agent 2 — Carbon Footprint Calculator
======================================
Takes extracted fields from Agent 1 and the carbon-emission-factor
reference index, then uses Gemini to compute total carbon output
for each emission category.
"""

import json
import uuid
from pathlib import Path
from typing import Any

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage

from config.settings import settings
from db.snowflake_client import get_connection

# ── Load carbon index / rulebook ─────────────────────────
CARBON_INDEX_PATH = Path(__file__).resolve().parent.parent / "data" / "carbon_index.json"

# ── Gemini LLM setup ────────────────────────────────────
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    google_api_key=settings.GEMINI_API_KEY,
    temperature=0,
)


def _load_carbon_index() -> dict[str, Any]:
    """Load the carbon emission factor reference data."""
    with open(CARBON_INDEX_PATH, "r") as f:
        return json.load(f)


def calculate_carbon(doc_id: str, extracted_fields: dict[str, Any]) -> dict[str, Any]:
    """
    Calculate carbon footprint using extracted data + carbon index.

    1. Loads the carbon emission factor reference index.
    2. Sends extracted fields + reference data to Gemini.
    3. Gemini maps fields → emission factors → carbon output per category.
    4. Saves breakdown to Snowflake.
    5. Returns total emissions and per-category breakdown.
    """
    carbon_index = _load_carbon_index()

    system_prompt = (
        "You are a carbon-footprint calculation expert. "
        "Given a set of extracted supply-chain data fields and a carbon "
        "emission factor reference index, compute the carbon output for "
        "each relevant category.\n\n"
        "Return ONLY valid JSON with this structure:\n"
        "{\n"
        '  "total_carbon_kg": <number>,\n'
        '  "breakdown": [\n'
        '    {"category": "<name>", "emission_factor": <number>, '
        '"quantity": <number>, "carbon_output": <number>, "unit": "kgCO2e"}\n'
        "  ]\n"
        "}"
    )

    user_prompt = (
        f"## Extracted Document Fields\n```json\n{json.dumps(extracted_fields, indent=2)}\n```\n\n"
        f"## Carbon Emission Factor Index\n```json\n{json.dumps(carbon_index, indent=2)}\n```\n\n"
        "Compute the carbon footprint breakdown."
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]

    response = llm.invoke(messages)

    try:
        result = json.loads(response.content)
    except json.JSONDecodeError:
        result = {
            "total_carbon_kg": 0,
            "breakdown": [],
            "_raw_response": response.content,
        }

    # ── Persist to Snowflake ──────────────────────────────
    _save_carbon_results(doc_id, result)

    return result


def _save_carbon_results(doc_id: str, result: dict[str, Any]) -> None:
    """Write carbon breakdown rows to the carbon_results table."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        for item in result.get("breakdown", []):
            cur.execute(
                """
                INSERT INTO carbon_results
                    (id, doc_id, category, emission_factor, quantity, carbon_output, unit)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(uuid.uuid4()),
                    doc_id,
                    item.get("category", "unknown"),
                    item.get("emission_factor", 0),
                    item.get("quantity", 0),
                    item.get("carbon_output", 0),
                    item.get("unit", "kgCO2e"),
                ),
            )
    finally:
        conn.close()
