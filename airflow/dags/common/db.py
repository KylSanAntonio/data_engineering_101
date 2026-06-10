from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

def get_connection(conn_id: str):

    conn = BaseHook.get_connection(conn_id)

    if conn.conn_type == "postgres":
        return PostgresHook(
            postgres_conn_id=conn_id
        ).get_conn()

    if conn.conn_type == "mysql":
        return MySqlHook(
            mysql_conn_id=conn_id
        ).get_conn()

    raise ValueError(
        f"Unsupported connection type: {conn.conn_type}"
    )