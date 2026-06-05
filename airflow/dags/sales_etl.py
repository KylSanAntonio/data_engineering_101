from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging

from common.db import get_postgres_conn, get_mysql_conn
from common.watermark import get_watermark, update_watermark, update_freshness
from common.sql_loader import (
    load_sql,
    execute_sql,
    fetch_sql
)
from common.observability import log_dag_start, log_dag_end, track_task
from common.config import DAG_VERSION, BATCH_SIZE

logger = logging.getLogger(__name__)


# ==================================================
# 1. EXTRACT → RAW LAYER
# ==================================================
@track_task("extract_raw")
def extract_raw(**context):

    watermark = get_watermark()

    logger.info(
        "Starting streaming extraction. Watermark=%s",
        watermark
    )

    mysql_conn = get_mysql_conn()
    pg_conn = get_postgres_conn()

    try:

        mysql_cur = mysql_conn.cursor()
        pg_cur = pg_conn.cursor()
        extract_sql = load_sql("extract_raw.sql")
        insert_sql = load_sql("load_raw.sql")

        # -----------------------------------------
        # STREAM FROM MYSQL
        # -----------------------------------------
        mysql_cur.execute(extract_sql, (watermark,))

        total_rows = 0

        while True:

            batch = mysql_cur.fetchmany(BATCH_SIZE)

            if not batch:
                break

            # -----------------------------------------
            # BULK INSERT INTO POSTGRES
            # -----------------------------------------
            pg_cur.executemany(insert_sql, batch)
            pg_conn.commit()

            total_rows += len(batch)

            logger.info(
                "Inserted batch of %s rows (total=%s)",
                len(batch),
                total_rows
            )

        logger.info(
            "Extraction completed. total_rows=%s",
            total_rows
        )

        rows = fetch_sql(
            mysql_cur,
            extract_sql,
            (watermark,)
        )

        logger.info(
            "Fetched %s rows from MySQL",
            len(rows)
        )

        if not rows:
            logger.info(
                "No new rows found"
            )
            return

        pg_cur = pg_conn.cursor()

        insert_sql = load_sql(
            "load_raw.sql"
        )

        pg_cur.executemany(
            insert_sql,
            rows
        )

        pg_conn.commit()

        logger.info(
            "Loaded %s rows into raw.sales_raw",
            len(rows)
        )

    except Exception as e:

        pg_conn.rollback()

        logger.error(
            "Extraction failed: %s",
            str(e)
        )

        raise

    finally:

        mysql_conn.close()
        pg_conn.close()

# ==================================================
# 2. RAW → STAGING
# ==================================================
@track_task("run_staging")
def run_staging(**context):

    watermark = get_watermark()

    logger.info("Starting staging load. Watermark=%s", watermark)

    conn = get_postgres_conn()

    try:
        cur = conn.cursor()

        sql = load_sql("load_staging.sql")

        execute_sql(cur, sql, (watermark,))

        conn.commit()

        logger.info("Staging load completed")

    except Exception as e:
        conn.rollback()
        logger.error("Staging load failed: %s", str(e))
        raise

    finally:
        conn.close()


# ==================================================
# 3. DATA QUALITY CHECK
# ==================================================
@track_task("quality_check")
def quality_check(**context):

    conn = get_postgres_conn()

    try:
        cur = conn.cursor()

        sql = load_sql("quality_check.sql")

        execute_sql(cur, sql)

        count = cur.fetchone()[0]

        if count > 0:
            raise Exception(f"Quality check failed: {count} invalid rows")

        logger.info("Quality check passed")

    finally:
        conn.close()


# ==================================================
# 4. STAGING → WAREHOUSE
# ==================================================
@track_task("run_warehouse")
def run_warehouse(**context):

    conn = get_postgres_conn()

    try:
        cur = conn.cursor()

        sql = load_sql("load_warehouse.sql")

        execute_sql(cur, sql)

        conn.commit()

        logger.info("Warehouse load completed")

    except Exception as e:
        conn.rollback()
        logger.error("Warehouse load failed: %s", str(e))
        raise

    finally:
        conn.close()


# ==================================================
# 5. UPDATE WATERMARK
# ==================================================
@track_task("update_watermark")
def save_watermark(**context):

    conn = get_postgres_conn()

    try:
        cur = conn.cursor()

        execute_sql(cur, """
            SELECT COALESCE(MAX(id), 0)
            FROM raw.sales_raw
        """)

        max_id = cur.fetchone()[0]

        update_watermark(max_id)
        update_freshness("sales_raw", max_id)

        logger.info("Watermark updated to %s", max_id)

    finally:
        conn.close()


# ==================================================
# 6. DAG DEFINITION
# ==================================================
with DAG(
    dag_id="sales_etl",
    description=f"Production ETL Pipeline v{DAG_VERSION}",
    start_date=datetime(2026, 6, 4),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl", "warehouse", "development", f"v{DAG_VERSION}"],
) as dag:

    start = PythonOperator(
        task_id="log_dag_start",
        python_callable=log_dag_start,
    )

    extract = PythonOperator(
        task_id="extract_raw",
        python_callable=extract_raw,
        sla=timedelta(minutes=15),
    )

    staging = PythonOperator(
        task_id="load_staging",
        python_callable=run_staging,
        sla=timedelta(minutes=15),
    )

    validate = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        sla=timedelta(minutes=5),
    )

    warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=run_warehouse,
        sla=timedelta(minutes=15),
    )

    watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=save_watermark,
        sla=timedelta(minutes=2),
    )

    end = PythonOperator(
        task_id="log_dag_end",
        python_callable=log_dag_end,
    )

    # ==================================================
    # PIPELINE FLOW
    # ==================================================
    start >> extract >> staging >> validate >> warehouse >> watermark >> end