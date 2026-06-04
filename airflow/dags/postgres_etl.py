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

def extract(**context):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT product, amount FROM sales_raw")
    rows = cur.fetchall()

    conn.close()
    context["ti"].xcom_push(key="raw_data", value=rows)

def transform(**context):
    data = context["ti"].xcom_pull(key="raw_data")

    transformed = [
        (product, float(amount) * 1.12)  # add tax
        for product, amount in data
    ]

    context["ti"].xcom_push(key="transformed_data", value=transformed)

def load(**context):
    data = context["ti"].xcom_pull(key="transformed_data")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_processed (
            product TEXT,
            total_amount NUMERIC
        )
    """)

    cur.executemany(
        "INSERT INTO sales_processed VALUES (%s, %s)",
        data
    )

    conn.commit()
    conn.close()

with DAG(
    dag_id="postgres_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3