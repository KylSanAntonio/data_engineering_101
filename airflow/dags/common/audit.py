from common.postgres import get_conn


def log_run(
        dag_id,
        task_id,
        status,
        rows_processed=0,
        error_message=None):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO audit.etl_run_log
        (
            dag_id,
            task_id,
            status,
            rows_processed,
            start_time,
            end_time,
            error_message
        )
        VALUES
        (
            %s,
            %s,
            %s,
            %s,
            NOW(),
            NOW(),
            %s
        )
    """,
    (
        dag_id,
        task_id,
        status,
        rows_processed,
        error_message
    ))

    conn.commit()
    conn.close()