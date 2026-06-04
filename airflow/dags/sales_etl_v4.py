from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from common.postgres import get_conn
from common.watermark import get_watermark, update_watermark
from common.sql_loader import load_sql

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# --------------------------------------------------
# RAW -> STAGING
# --------------------------------------------------
def run_staging():

    watermark = get_watermark()

    with get_conn() as conn:
        with conn.cursor() as cur:

            sql = load_sql("load_staging.sql")

            cur.execute(
                sql,
                {
                    "watermark": watermark
                }
            )

            conn.commit()
            conn.close()


# --------------------------------------------------
# DATA QUALITY CHECK
# --------------------------------------------------
def quality_check():

    with get_conn() as conn:
        with conn.cursor() as cur:

            sql = load_sql("quality_check.sql")

            cur.execute(sql)

            count = cur.fetchone()[0]

            conn.close()

            if count > 0:
                raise Exception(
                    f"Quality check failed. Found {count} NULL IDs."
                )


# --------------------------------------------------
# STAGING -> WAREHOUSE
# --------------------------------------------------
def run_warehouse():

    with get_conn() as conn:
        with conn.cursor() as cur:

            sql = load_sql("load_warehouse.sql")

            cur.execute(sql)

            conn.commit()
            conn.close()


# --------------------------------------------------
# UPDATE WATERMARK
# --------------------------------------------------
def save_watermark():

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT COALESCE(MAX(id), 0)
                FROM raw.sales_raw
            """)

            max_id = cur.fetchone()[0]

            conn.close()

            update_watermark(max_id)


# --------------------------------------------------
# DAG
# --------------------------------------------------
with DAG(
    dag_id="sales_etl",
    description="Incremental RAW -> STAGING -> WAREHOUSE ETL",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "warehouse", "incremental"],
) as dag:

    load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=run_staging,
        sla=timedelta(minutes=15),
    )

    validate = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        sla=timedelta(minutes=5),
    )

    load_wh = PythonOperator(
        task_id="load_warehouse",
        python_callable=run_warehouse,
        sla=timedelta(minutes=15),
    )

    watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=save_watermark,
        sla=timedelta(minutes=2),
    )

    (
        load_staging
        >> validate
        >> load_wh
        >> watermark
    )