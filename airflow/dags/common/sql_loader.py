from pathlib import Path

def load_sql(filename):
    path = Path("/opt/airflow/dags/sql") / filename
    return path.read_text()