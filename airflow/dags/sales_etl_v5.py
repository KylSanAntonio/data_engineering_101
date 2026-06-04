from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from common.postgres import get_conn
from common.watermark import get_watermark, update_watermark
from common.sql_loader import load_sql

import logging

logger = logging.getLogger(__name__)


# --------------------------------------------------
# DEFAULT CONFIG
# --------------------------------------------------
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

    logger.info("Starting staging load. Watermark=%s", watermark)

    conn = get_conn()

    try:
        cur = conn.cursor()

        sql = load_sql("load_staging.sql")

        cur.execute(
            sql,
            {"watermark": watermark}
        )

        conn.commit()

        logger.info("Staging load completed successfully")

    except Exception as e:
        conn.rollback()
        logger.error("Staging load failed: %s", str(e))
        raise

    finally:
        conn.close()


# --------------------------------------------------
# DATA QUALITY CHECK
# --------------------------------------------------
def quality_check():

    conn = get_conn()

    try:
        cur = conn.cursor()

        sql = load_sql("quality_check.sql")

        cur.execute(sql)

        count = cur.fetchone()[0]

        if count > 0:
            raise Exception(
                f"Quality check failed. Found {count} invalid records."
            )

        logger.info("Quality check passed")

    finally:
        conn.close()


# --------------------------------------------------
# STAGING -> WAREHOUSE
# --------------------------------------------------
def run_warehouse():

    conn = get_conn()

    try:
        cur = conn.cursor()

        sql = load_sql("load_warehouse.sql")

        cur.execute(sql)

        conn.commit()

        logger.info("Warehouse load completed")

    except Exception as e:
        conn.rollback()
        logger.error("Warehouse load failed: %s", str(e))
        raise

    finally:
        conn.close()


# --------------------------------------------------
# UPDATE WATERMARK
# --------------------------------------------------
def save_watermark():

    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT COALESCE(MAX(id), 0)
            FROM raw.sales_raw
        """)

        max_id = cur.fetchone()[0]

        update_watermark(max_id)

        logger.info("Watermark updated to %s", max_id)

    finally:
        conn.close()


# --------------------------------------------------
# DAG DEFINITION
# --------------------------------------------------
with DAG(
    dag_id="sales_etl",
    description="Enterprise incremental ETL pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "warehouse", "production"],
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

    update_wm = PythonOperator(
        task_id="update_watermark",
        python_callable=save_watermark,
        sla=timedelta(minutes=2),
    )

    load_staging >> validate >> load_wh >> update_wm