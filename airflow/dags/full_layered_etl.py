from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# -------------------------
# RAW → STAGING EXTRACT
# -------------------------
def extract_raw(**context):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, product, amount, created_at
        FROM raw.sales_raw
    """)

    rows = cur.fetchall()
    conn.close()

    context["ti"].xcom_push(key="raw_data", value=rows)


# -------------------------
# STAGING TRANSFORM
# -------------------------
def transform_to_staging(**context):
    data = context["ti"].xcom_pull(key="raw_data")

    transformed = []
    for row in data:
        id, product, amount, created_at = row
        tax_amount = float(amount) * 1.12

        transformed.append((
            id,
            product.lower().strip(),
            float(amount),
            tax_amount,
            created_at
        ))

    context["ti"].xcom_push(key="staging_data", value=transformed)


# -------------------------
# LOAD TO STAGING TABLE
# -------------------------
def load_staging(**context):
    data = context["ti"].xcom_pull(key="staging_data")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.sales_clean (
            id INT,
            product TEXT,
            amount NUMERIC,
            amount_with_tax NUMERIC,
            created_at TIMESTAMP
        )
    """)

    # Optional: clear before reload (simple full refresh model)
    cur.execute("TRUNCATE staging.sales_clean")

    cur.executemany("""
        INSERT INTO staging.sales_clean
        (id, product, amount, amount_with_tax, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """, data)

    conn.commit()
    conn.close()


# -------------------------
# STAGING → WAREHOUSE
# -------------------------
def build_warehouse(**context):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS warehouse.sales_summary (
            product TEXT,
            total_sales NUMERIC,
            total_orders INT
        )
    """)

    cur.execute("TRUNCATE warehouse.sales_summary")

    cur.execute("""
        INSERT INTO warehouse.sales_summary
        SELECT
            product,
            SUM(amount_with_tax) AS total_sales,
            COUNT(*) AS total_orders
        FROM staging.sales_clean
        GROUP BY product
    """)

    conn.commit()
    conn.close()


# -------------------------
# DAG DEFINITION
# -------------------------
with DAG(
    dag_id="full_layered_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "warehouse", "3-layer"]
) as dag:

    t1 = PythonOperator(
        task_id="extract_raw",
        python_callable=extract_raw
    )

    t2 = PythonOperator(
        task_id="transform_staging",
        python_callable=transform_to_staging
    )

    t3 = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging
    )

    t4 = PythonOperator(
        task_id="build_warehouse",
        python_callable=build_warehouse
    )

    t1 >> t2 >> t3 >> t4