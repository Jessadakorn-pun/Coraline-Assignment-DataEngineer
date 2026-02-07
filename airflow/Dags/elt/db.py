import psycopg2
import os

def get_connection():
    
    connection = psycopg2.connect(
        host=os.getenv("DATA_WAREHOUSE_POSTGRES_HOST"),
        port=os.getenv("DATA_WAREHOUSE_POSTGRES_PORT", 5432),
        dbname=os.getenv("DATA_WAREHOUSE_POSTGRES_DB"),
        user=os.getenv("DATA_WAREHOUSE_POSTGRES_USER"),
        password=os.getenv("DATA_WAREHOUSE_POSTGRES_PASSWORD"),
    )
    
    return connection

def check_table_exists(conn, table_name):
    
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