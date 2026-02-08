"""
Database utility functions for PostgreSQL data warehouse access.

This module provides helper functions for:
- Creating database connections using environment variables
- Checking table existence in a PostgreSQL database

Designed for use in ELT pipelines, Airflow tasks, and data validation scripts.
"""

import psycopg2
import os

def get_connection():
    
    """
    Create and return a PostgreSQL database connection.

    Connection parameters are loaded from environment variables:

    - DATA_WAREHOUSE_POSTGRES_HOST
    - DATA_WAREHOUSE_POSTGRES_PORT (default: 5432)
    - DATA_WAREHOUSE_POSTGRES_DB
    - DATA_WAREHOUSE_POSTGRES_USER
    - DATA_WAREHOUSE_POSTGRES_PASSWORD

    Returns:
        psycopg2.extensions.connection:
            An active PostgreSQL connection object.

    Raises:
        psycopg2.OperationalError:
            If the connection to the database cannot be established.
    """
    
    connection = psycopg2.connect(
        host=os.getenv("DATA_WAREHOUSE_POSTGRES_HOST"),
        port=os.getenv("DATA_WAREHOUSE_POSTGRES_PORT", 5432),
        dbname=os.getenv("DATA_WAREHOUSE_POSTGRES_DB"),
        user=os.getenv("DATA_WAREHOUSE_POSTGRES_USER"),
        password=os.getenv("DATA_WAREHOUSE_POSTGRES_PASSWORD"),
    )
    
    return connection

def check_table_exists(conn, table_name):
    
    """
    Check whether a table exists in the connected PostgreSQL database.

    The function queries `information_schema.tables` to determine
    if a table with the given name exists.

    Args:
        conn (psycopg2.extensions.connection):
            An active PostgreSQL database connection.
        table_name (str):
            Name of the table to check.

    Returns:
        bool:
            True if the table exists, False otherwise.
    """
    
    with conn.cursor() as cur:
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
        )
        """
        cur.execute(query, (table_name,))
        return cur.fetchone()[0]