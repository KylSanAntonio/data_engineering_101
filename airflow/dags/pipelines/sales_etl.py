from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging

from common.config import (
    DAG_VERSION,
    BATCH_SIZE,
    PIPELINES
)

from common.db import get_connection
from common.sql_loader import load_sql
from common.watermark import (
    get_watermark,
    update_watermark
)

from common.observability import (
    log_dag_start,
    log_dag_end,
    track_task
)

logger = logging.getLogger(__name__)

# ==================================================
# PIPELINE CONFIG
# ==================================================

PIPELINE = PIPELINES["sales"]

SOURCE_CONN = PIPELINE["source_conn"]
TARGET_CONN = PIPELINE["target_conn"]
WATERMARK_KEY = PIPELINE["watermark_key"]

# ==================================================
# BRONZE LOAD
# ==================================================

@track_task("load_bronze")
def load_bronze(**context):

    watermark = get_watermark(WATERMARK_KEY)

    logger.info(
        "Starting Bronze Load. Watermark=%s",
        watermark
    )

    source_conn = get_connection(SOURCE_CONN)
    target_conn = get_connection(TARGET_CONN)

    try:

        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()

        extract_sql = load_sql(
            "sales/extract.sql"
        )

        insert_sql = load_sql(
            "sales/load_bronze.sql"
        )

        source_cur.execute(
            extract_sql,
            (watermark,)
        )

        total_rows = 0
        max_id = watermark

        while True:

            batch = source_cur.fetchmany(
                BATCH_SIZE
            )

            if not batch:
                break

            target_cur.executemany(
                insert_sql,
                batch
            )

            target_conn.commit()

            total_rows += len(batch)

            batch_max = max(
                row[0] for row in batch
            )

            max_id = max(
                max_id,
                batch_max
            )

            logger.info(
                "Inserted %s rows (total=%s)",
                len(batch),
                total_rows
            )

        context["ti"].xcom_push(
            key="max_id",
            value=max_id
        )

        logger.info(
            "Bronze load completed. Rows=%s",
            total_rows
        )

    except Exception:

        target_conn.rollback()
        raise

    finally:

        source_conn.close()
        target_conn.close()


# ==================================================
# SILVER LOAD
# ==================================================

@track_task("load_silver")
def load_silver(**context):

    watermark = get_watermark(
        WATERMARK_KEY
    )

    logger.info(
        "Starting Silver Load. Watermark=%s",
        watermark
    )

    conn = get_connection(TARGET_CONN)

    try:

        cur = conn.cursor()

        sql = load_sql(
            "sales/load_silver.sql"
        )

        cur.execute(
            sql,
            (watermark,)
        )

        conn.commit()

        logger.info(
            "Silver load completed"
        )

    except Exception:

        conn.rollback()
        raise

    finally:

        conn.close()


# ==================================================
# QUALITY CHECK
# ==================================================

@track_task("quality_check")
def quality_check(**context):

    conn = get_connection(TARGET_CONN)

    try:

        cur = conn.cursor()

        sql = load_sql(
            "sales/quality_check.sql"
        )

        cur.execute(sql)

        count = cur.fetchone()[0]

        if count > 0:

            raise Exception(
                f"Quality check failed: {count} invalid rows"
            )

        logger.info(
            "Quality check passed"
        )

    finally:

        conn.close()


# ==================================================
# GOLD LOAD
# ==================================================

@track_task("load_gold")
def load_gold(**context):

    conn = get_connection(TARGET_CONN)

    try:

        cur = conn.cursor()

        sql = load_sql(
            "sales/load_gold.sql"
        )

        cur.execute(sql)

        conn.commit()

        logger.info(
            "Gold load completed"
        )

    except Exception:

        conn.rollback()
        raise

    finally:

        conn.close()


# ==================================================
# UPDATE WATERMARK
# ==================================================

@track_task("update_watermark")
def save_watermark(**context):

    max_id = context["ti"].xcom_pull(
        task_ids="load_bronze",
        key="max_id"
    )

    if max_id is None:

        logger.info(
            "No watermark update required"
        )

        return

    update_watermark(
        WATERMARK_KEY,
        max_id
    )

    logger.info(
        "Watermark updated to %s",
        max_id
    )


# ==================================================
# DAG
# ==================================================

with DAG(
    dag_id="sales_etl",
    description=f"Sales ETL v{DAG_VERSION}",
    start_date=datetime(
        2026,
        6,
        1
    ),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(
            minutes=5
        )
    },
    tags=[
        "sales",
        "medallion",
        f"v{DAG_VERSION}"
    ]
) as dag:

    start = PythonOperator(
        task_id="log_dag_start",
        python_callable=log_dag_start
    )

    bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=load_bronze,
        sla=timedelta(
            minutes=15
        )
    )

    silver = PythonOperator(
        task_id="load_silver",
        python_callable=load_silver,
        sla=timedelta(
            minutes=10
        )
    )

    validate = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        sla=timedelta(
            minutes=5
        )
    )

    gold = PythonOperator(
        task_id="load_gold",
        python_callable=load_gold,
        sla=timedelta(
            minutes=10
        )
    )

    watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=save_watermark,
        sla=timedelta(
            minutes=2
        )
    )

    end = PythonOperator(
        task_id="log_dag_end",
        python_callable=log_dag_end
    )

    (
        start
        >> bronze
        >> silver
        >> validate
        >> gold
        >> watermark
        >> end
    )