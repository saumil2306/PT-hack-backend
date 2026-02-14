"""
PDF Service
===========
Handles PDF upload, storage in Snowflake, and retrieval for processing.
"""

import uuid
from typing import BinaryIO

from db.snowflake_client import get_connection


def save_pdf(filename: str, file_bytes: bytes) -> str:
    """
    Store an uploaded PDF in the Snowflake `documents` table.

    Returns the generated doc_id.
    """
    doc_id = str(uuid.uuid4())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO documents (doc_id, filename, raw_pdf, status)
            VALUES (%s, %s, %s, %s)
            """,
            (doc_id, filename, file_bytes, "pending"),
        )
    finally:
        conn.close()
    return doc_id


def get_pdf(doc_id: str) -> tuple[str, bytes]:
    """
    Retrieve a stored PDF by its doc_id.

    Returns (filename, raw_pdf_bytes).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT filename, raw_pdf FROM documents WHERE doc_id = %s",
            (doc_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Document {doc_id} not found.")
        return row[0], row[1]
    finally:
        conn.close()


def update_status(doc_id: str, status: str) -> None:
    """Update the processing status of a document."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE documents SET status = %s WHERE doc_id = %s",
            (status, doc_id),
        )
    finally:
        conn.close()
