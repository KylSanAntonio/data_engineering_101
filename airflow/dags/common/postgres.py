from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "warehouse_postgres"

def get_conn():
    return PostgresHook(
        postgres_conn_id=CONN_ID
    ).get_conn()