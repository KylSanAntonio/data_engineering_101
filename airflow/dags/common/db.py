from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

POSTGRES_CONN_ID = "warehouse_postgres"
MYSQL_CONN_ID = "source_mysql"

def get_postgres_conn():
    return PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    ).get_conn()

def get_mysql_conn():
    return MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    ).get_conn()