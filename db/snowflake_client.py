"""
Snowflake Database Client
=========================
Handles connections and table initialization for Snowflake.
"""

import snowflake.connector
from config.settings import settings


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Return a Snowflake connection using environment credentials."""
    return snowflake.connector.connect(
        account=settings.SNOWFLAKE_ACCOUNT,
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        database=settings.SNOWFLAKE_DATABASE,
        schema=settings.SNOWFLAKE_SCHEMA,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
    )


def init_tables() -> None:
    """Create application tables if they do not already exist."""
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id        STRING PRIMARY KEY,
            filename      STRING,
            upload_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            status        STRING DEFAULT 'pending',
            raw_pdf       BINARY
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS extracted_fields (
            id            STRING PRIMARY KEY,
            doc_id        STRING REFERENCES documents(doc_id),
            field_name    STRING,
            field_value   STRING,
            confidence    FLOAT,
            created_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS carbon_results (
            id            STRING PRIMARY KEY,
            doc_id        STRING REFERENCES documents(doc_id),
            category      STRING,
            emission_factor FLOAT,
            quantity      FLOAT,
            carbon_output FLOAT,
            unit          STRING DEFAULT 'kgCO2e',
            created_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS audit_reports (
            id            STRING PRIMARY KEY,
            doc_id        STRING REFERENCES documents(doc_id),
            hotspots      VARIANT,
            recommendations VARIANT,
            total_emissions FLOAT,
            risk_level    STRING,
            created_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
    ]

    conn = get_connection()
    try:
        cur = conn.cursor()
        for ddl in ddl_statements:
            cur.execute(ddl)
    finally:
        conn.close()
