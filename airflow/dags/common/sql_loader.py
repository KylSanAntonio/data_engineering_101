from pathlib import Path
import re
from typing import Union, Optional

SQL_DIR = Path("/opt/airflow/dags/sql")


# --------------------------------------------------
# LOAD SQL FILE
# --------------------------------------------------
def load_sql(filename: str) -> str:
    path = SQL_DIR / filename
    return path.read_text()


# --------------------------------------------------
# VALIDATE SQL PARAMS (PYTHON 3.8 SAFE)
# --------------------------------------------------
def validate_sql(sql: str, params: Optional[Union[dict, tuple]] = None):

    named_params = re.findall(r"%\([a-zA-Z0-9_]+\)s", sql)
    positional_params = sql.count("%s")

    if named_params and not isinstance(params, dict):
        raise ValueError(
            f"SQL expects named params {named_params} but got {type(params)}"
        )

    if positional_params > 0 and params is None:
        raise ValueError("SQL expects positional params but none provided")

    if positional_params > 0 and isinstance(params, dict):
        raise ValueError("SQL expects positional params but dict provided")

    return True


# --------------------------------------------------
# SAFE EXECUTION WRAPPER
# --------------------------------------------------
def execute_sql(cur, sql: str, params=None):

    validate_sql(sql, params)

    if params is None:
        return cur.execute(sql)

    return cur.execute(sql, params)


# --------------------------------------------------
# FETCH SQL RESULTS
# --------------------------------------------------
def fetch_sql(cur, sql: str, params=None):

    validate_sql(sql, params)

    cur.execute(sql, params)

    return cur.fetchall()