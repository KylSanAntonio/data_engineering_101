from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

from common.postgres import get_conn
from common.watermark import get_watermark
from common.watermark import update_watermark
from common.audit import log_run

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}


TARGET_SCHEMA = Variable.get(
    "target_schema",
    default_var="warehouse"
)


def run_staging(**context):

    watermark = get_watermark()

    conn = get_conn()
    cur = conn.cursor()

    with open(
        "/opt/airflow/dags/sql/load_staging.sql"
    ) as f:
        sql = f.read()

    cur.execute(
        sql,
        {"watermark": watermark}
    )

    conn.commit()
    conn.close()

    cur.execute("""
        SELECT MAX(id)
        FROM raw.sales_raw
        WHERE id > current_watermark
    """)

    max_id = cur.fetchone()[0]

    context["ti"].xcom_push(
        key="max_id",
        value=max_id
    )

    conn.close()


def quality_check():

    conn = get_conn()
    cur = conn.cursor()

    with open(
        "/opt/airflow/dags/sql/quality_check.sql"
    ) as f:
        sql = f.read()

    cur.execute(sql)

    count = cur.fetchone()[0]

    conn.close()

    if count > 0:
        raise Exception(
            f"Quality check failed. Found {count} NULL IDs."
        )


def run_warehouse():

    conn = get_conn()
    cur = conn.cursor()

    with open(
        "/opt/airflow/dags/sql/load_warehouse.sql"
    ) as f:
        sql = f.read()

    cur.execute(sql)

    conn.commit()
    conn.close()


def save_watermark(**context):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT COALESCE(MAX(id),0)
        FROM staging.sales_clean
    """)
    max_id = cur.fetchone()[0]

    update_watermark(max_id)

    conn.close()


with DAG(
    dag_id="sales_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "warehouse"],
) as dag:

    load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=run_staging,
        sla=timedelta(minutes=15)
    )

    validate = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        sla=timedelta(minutes=5)
    )

    load_wh = PythonOperator(
        task_id="load_warehouse",
        python_callable=run_warehouse,
        sla=timedelta(minutes=15)
    )

    watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=save_watermark
    )

    load_staging >> validate >> load_wh >> watermark