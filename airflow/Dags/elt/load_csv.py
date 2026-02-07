import csv
from datetime import datetime
from .db import get_connection, check_table_exists
from .config import CONFIG

TABLE = CONFIG["TABLE"]
CSV_PATH = CONFIG["CSV_PATH"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS food_sales (
    id VARCHAR(255) PRIMARY KEY,
    date DATE,
    region VARCHAR(255),
    city VARCHAR(255),
    category VARCHAR(255),
    product VARCHAR(255),
    qty INT,
    unitprice DECIMAL(10,2),
    totalprice DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_SQL = """
INSERT INTO food_sales (
    id, date, region, city, category, product, qty, unitprice, totalprice
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    date = EXCLUDED.date,
    region = EXCLUDED.region,
    city = EXCLUDED.city,
    category = EXCLUDED.category,
    product = EXCLUDED.product,
    qty = EXCLUDED.qty,
    unitprice = EXCLUDED.unitprice,
    totalprice = EXCLUDED.totalprice,
    updated_at = CURRENT_TIMESTAMP;
"""

def main():
    print(f"Starting ELT load from {CSV_PATH}")

    conn = get_connection()
    conn.autocommit = False

    try:
        if not check_table_exists(conn, TABLE):
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)

        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []

            for r in reader:
                rows.append(
                    (
                        r["ID"],
                        datetime.strptime(r["Date"], "%d/%m/%Y").date(),
                        r["Region"],
                        r["City"],
                        r["Category"],
                        r["Product"],
                        int(r["Qty"]),
                        float(r["UnitPrice"]),
                        float(r["TotalPrice"]),
                    )
                )

        if not rows:
            raise RuntimeError("CSV file is empty")

        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)

        conn.commit()
        print(f"Loaded {len(rows)} rows into {TABLE}")

    except Exception as err:
        conn.rollback()
        raise RuntimeError(f"ELT load fail: {err}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()