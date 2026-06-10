from common.db import get_connection
from common.config import PLATFORM_CONN_ID

def get_watermark(pipeline_name):

    conn = get_connection(PLATFORM_CONN_ID)

    try:

        cur = conn.cursor()

        cur.execute("""
            SELECT last_processed_id
            FROM metadata.watermark
            WHERE pipeline_name = %s
        """, (pipeline_name,))

        row = cur.fetchone()

        if row:
            return row[0]

        cur.execute("""
            INSERT INTO metadata.watermark
            (
                pipeline_name,
                last_processed_id,
                updated_at
            )
            VALUES
            (
                %s,
                0,
                NOW()
            )
        """, (pipeline_name,))

        conn.commit()

        return 0

    finally:

        conn.close()


def update_watermark(
    pipeline_name,
    last_id
):

    conn = get_connection(PLATFORM_CONN_ID)

    try:

        cur = conn.cursor()

        cur.execute("""
            INSERT INTO metadata.watermark
            (
                pipeline_name,
                last_processed_id,
                updated_at
            )
            VALUES
            (
                %s,
                %s,
                NOW()
            )
            ON CONFLICT (pipeline_name)
            DO UPDATE
            SET
                last_processed_id = EXCLUDED.last_processed_id,
                updated_at = NOW()
        """,
        (
            pipeline_name,
            last_id
        ))

        conn.commit()

    finally:

        conn.close()