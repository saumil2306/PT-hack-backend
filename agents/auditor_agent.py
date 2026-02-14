"""
Agent 3 — Auditor
==================
Receives carbon calculation results from Agent 2, analyzes them
using Gemini, and produces an audit report highlighting carbon
hotspots, risk levels, and actionable recommendations.
"""

import json
import uuid
from typing import Any

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage

from config.settings import settings
from db.snowflake_client import get_connection

# ── Gemini LLM setup ────────────────────────────────────
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    google_api_key=settings.GEMINI_API_KEY,
    temperature=0.2,
)


def audit(doc_id: str, carbon_result: dict[str, Any]) -> dict[str, Any]:
    """
    Produce an audit report from carbon calculation data.

    1. Sends carbon breakdown to Gemini for hotspot analysis.
    2. Gemini identifies the highest-impact categories, assigns risk level,
       and generates improvement recommendations.
    3. Saves the audit report to Snowflake.
    4. Returns structured report for frontend display.
    """
    system_prompt = (
        "You are a sustainability auditor. Given a carbon footprint "
        "breakdown for a supply-chain document, perform the following:\n"
        "1. Identify the top carbon hotspots (highest-emission categories).\n"
        "2. Assign an overall risk level: low, medium, high, or critical.\n"
        "3. Provide actionable recommendations to reduce emissions.\n\n"
        "Return ONLY valid JSON with this structure:\n"
        "{\n"
        '  "hotspots": [\n'
        '    {"category": "<name>", "carbon_output": <number>, '
        '"percentage_of_total": <number>, "severity": "<low|medium|high|critical>"}\n'
        "  ],\n"
        '  "recommendations": ["<recommendation 1>", "..."],\n'
        '  "total_emissions": <number>,\n'
        '  "risk_level": "<low|medium|high|critical>"\n'
        "}"
    )

    user_prompt = (
        f"## Carbon Footprint Data\n"
        f"```json\n{json.dumps(carbon_result, indent=2)}\n```\n\n"
        "Analyze and produce the audit report."
    )

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ]

    response = llm.invoke(messages)

    try:
        report = json.loads(response.content)
    except json.JSONDecodeError:
        report = {
            "hotspots": [],
            "recommendations": [],
            "total_emissions": carbon_result.get("total_carbon_kg", 0),
            "risk_level": "unknown",
            "_raw_response": response.content,
        }

    # ── Persist to Snowflake ──────────────────────────────
    _save_audit_report(doc_id, report)

    return report


def _save_audit_report(doc_id: str, report: dict[str, Any]) -> None:
    """Write the audit report to the audit_reports table."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO audit_reports
                (id, doc_id, hotspots, recommendations, total_emissions, risk_level)
            VALUES (%s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s)
            """,
            (
                str(uuid.uuid4()),
                doc_id,
                json.dumps(report.get("hotspots", [])),
                json.dumps(report.get("recommendations", [])),
                report.get("total_emissions", 0),
                report.get("risk_level", "unknown"),
            ),
        )
    finally:
        conn.close()
