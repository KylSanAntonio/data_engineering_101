DAG_VERSION = "1.0.0"

# DEFAULT_RETRY = 3

BATCH_SIZE = 10000

PLATFORM_CONN_ID = "airflow_postgres"

PIPELINES = {
    "sales": {
        "source_conn": "sales_mysql",
        "target_conn": "sales_warehouse_postgres",
        "watermark_key": "sales_etl",
        "sql_path": "sales",
        "dag_id": "sales"
    },

    "caf": {
        "source_conn": "caf_mysql",
        "target_conn": "caf_warehouse_postgres",
        "watermark_key": "caf_etl",
        "sql_path": "caf",
        "dag_id": "caf"
    }
}