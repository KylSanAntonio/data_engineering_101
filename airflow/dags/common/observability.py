from common.config import DAG_VERSION
import time
from airflow.operators.python import get_current_context
from common.db import get_postgres_conn

# --------------------------
# DAG START
# --------------------------
def log_dag_start():

    context = get_current_context()

    conn = get_postgres_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO metadata.dag_run_audit
        (dag_id, run_id, dag_version, git_sha, execution_date, status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        context["dag"].dag_id,
        context["run_id"],
        DAG_VERSION,
        "local-dev",
        context["logical_date"],
        "RUNNING"
    ))

    conn.commit()
    conn.close()


# --------------------------
# DAG END
# --------------------------
def log_dag_end():

    context = get_current_context()

    conn = get_postgres_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE metadata.dag_run_audit
        SET end_time = NOW(),
            status = 'SUCCESS'
        WHERE dag_id = %s
        AND run_id = %s
    """, (
        context["dag"].dag_id,
        context["run_id"]
    ))

    conn.commit()
    conn.close()


# --------------------------
# TASK TRACKING DECORATOR
# --------------------------
def track_task(task_name):

    def decorator(func):

        def wrapper(*args, **kwargs):

            context = get_current_context()
            start = time.time()

            conn = get_postgres_conn()
            cur = conn.cursor()

            try:
                result = func(*args, **kwargs)

                duration = time.time() - start

                cur.execute("""
                    INSERT INTO metadata.task_metrics
                    (dag_id, task_id, run_id, start_time, end_time, duration_seconds, status)
                    VALUES (%s, %s, %s, NOW(), NOW(), %s, %s)
                """, (
                    context["dag"].dag_id,
                    task_name,
                    context["run_id"],
                    duration,
                    "SUCCESS"
                ))

                conn.commit()

                return result

            except Exception:

                duration = time.time() - start

                cur.execute("""
                    INSERT INTO metadata.task_metrics
                    (dag_id, task_id, run_id, start_time, end_time, duration_seconds, status)
                    VALUES (%s, %s, %s, NOW(), NOW(), %s, %s)
                """, (
                    context["dag"].dag_id,
                    task_name,
                    context["run_id"],
                    duration,
                    "FAILED"
                ))

                conn.commit()

                raise

            finally:
                conn.close()

        return wrapper

    return decorator