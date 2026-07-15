from airflow.models import Connection
from airflow.settings import Session


def create_connection(
    conn_id,
    conn_type,
    host,
    schema,
    login,
    password,
    port
):

    session = Session()

    existing = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first()
    )

    if existing:

        print(
            f"{conn_id} already exists"
        )

        return

    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        schema=schema,
        login=login,
        password=password,
        port=port
    )

    session.add(conn)

    session.commit()

    print(
        f"{conn_id} created"
    )


# =====================================================
# WAREHOUSE POSTGRES
# =====================================================

create_connection(
    conn_id="sales_warehouse_postgres",
    conn_type="postgres",
    host="postgres",
    schema="warehouse",
    login="airflow",
    password="airflow",
    port=5432
)


# =====================================================
# SALES MYSQL
# =====================================================

create_connection(
    conn_id="sales_mysql",
    conn_type="mysql",
    host="192.168.13.169",
    schema="rdmd",
    login="ksanantonio",
    password="KSANANTONIOadm1n!",
    port=3306
)