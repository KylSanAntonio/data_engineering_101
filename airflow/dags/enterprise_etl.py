from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
import psycopg2
import time

# DB = {
#     "host": "postgres",
#     "database": "airflow",
#     "user": "airflow",
#     "password": "airflow"
# }

CONN_ID = "warehouse_postgres"

def get_conn():
    return PostgresHook(
        postgres_conn_id=CONN_ID
    ).get_conn()

# -------------------------
# Get watermark (last processed id)
# -------------------------
def get_watermark():
    # conn = psycopg2.connect(**DB)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT last_processed_id
        FROM raw.etl_control
        WHERE job_name = 'sales_etl'
    """)

    row = cur.fetchone()

    if not row:
        cur.execute("""
            INSERT INTO raw.etl_control (job_name, last_processed_id)
            VALUES ('sales_etl', 0)
        """)
        conn.commit()
        last_id = 0
    else:
        last_id = row[0]

    conn.close()
    return last_id


# -------------------------
# Extract incremental data
# -------------------------
def extract(**context):
    last_id = get_watermark()

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, product, amount, created_at
        FROM raw.sales_raw
        WHERE id > %s
        ORDER BY id
    """, (last_id,))

    rows = cur.fetchall()
    conn.close()

    context["ti"].xcom_push(key="raw_data", value=rows)
    context["ti"].xcom_push(key="max_id", value=last_id)


# -------------------------
# Transform
# -------------------------
def transform(**context):
    data = context["ti"].xcom_pull(key="raw_data")

    transformed = []

    for r in data:
        id, product, amount, created_at = r

        transformed.append((
            id,
            product.lower().strip(),
            float(amount),
            float(amount) * 1.12,
            created_at
        ))

    context["ti"].xcom_push(key="staging_data", value=transformed)


# -------------------------
# Load STAGING (UPSERT style)
# -------------------------
def load_staging(**context):
    data = context["ti"].xcom_pull(key="staging_data")

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.sales_clean (
            id INT PRIMARY KEY,
            product TEXT,
            amount NUMERIC,
            amount_with_tax NUMERIC,
            created_at TIMESTAMP
        )
    """)

    for row in data:
        cur.execute("""
            INSERT INTO staging.sales_clean
            (id, product, amount, amount_with_tax, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                product = EXCLUDED.product,
                amount = EXCLUDED.amount,
                amount_with_tax = EXCLUDED.amount_with_tax,
                created_at = EXCLUDED.created_at
        """, row)

    conn.commit()
    conn.close()


# -------------------------
# Warehouse incremental aggregation
# -------------------------
def load_warehouse():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS warehouse.sales_summary (
            product TEXT PRIMARY KEY,
            total_sales NUMERIC,
            total_orders INT
        )
    """)

    # incremental UPSERT aggregation
    cur.execute("""
        INSERT INTO warehouse.sales_summary (product, total_sales, total_orders)
        SELECT
            product,
            SUM(amount_with_tax),
            COUNT(*)
        FROM staging.sales_clean
        GROUP BY product
        ON CONFLICT (product) DO UPDATE SET
            total_sales = EXCLUDED.total_sales,
            total_orders = EXCLUDED.total_orders
    """)

    conn.commit()
    conn.close()


# -------------------------
# Update watermark
# -------------------------
def update_watermark(**context):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute("""
        SELECT MAX(id)
        FROM staging.sales_clean
    """)

    max_id = cur.fetchone()[0] or 0

    cur.execute("""
        UPDATE raw.etl_control
        SET last_processed_id = %s
        WHERE job_name = 'sales_etl'
    """, (max_id,))

    conn.commit()
    conn.close()


# -------------------------
# Log job execution
# -------------------------
def log_start(**context):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO raw.etl_log (job_name, status, rows_processed)
        VALUES ('sales_etl', 'STARTED', 0)
    """)

    conn.commit()
    conn.close()


def log_end(**context):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute("""
        UPDATE raw.etl_log
        SET status = 'SUCCESS',
            finished_at = NOW()
        WHERE id = (
            SELECT MAX(id) FROM raw.etl_log
            WHERE job_name = 'sales_etl'
        )
    """)

    conn.commit()
    conn.close()


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="prod_layered_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["production", "etl", "warehouse"]
) as dag:

    t0 = PythonOperator(task_id="log_start", python_callable=log_start)
    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load_staging", python_callable=load_staging)
    t4 = PythonOperator(task_id="load_warehouse", python_callable=load_warehouse)
    t5 = PythonOperator(task_id="update_watermark", python_callable=update_watermark)
    t6 = PythonOperator(task_id="log_end", python_callable=log_end)

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6