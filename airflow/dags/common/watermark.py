from common.config import PIPELINE_NAME
from common.postgres import get_conn

def get_watermark():

    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT last_processed_id
            FROM metadata.watermark
            WHERE pipeline_name = %s
        """, (PIPELINE_NAME,))

        row = cur.fetchone()

        if not row:
            cur.execute("""
                INSERT INTO metadata.watermark
                (pipeline_name, last_processed_id)
                VALUES (%s, 0)
            """, (PIPELINE_NAME,))

            conn.commit()

            return 0

        return row[0]

    finally:
        conn.close()


def update_watermark(last_id):

    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            UPDATE metadata.watermark
            SET
                last_processed_id = %s,
                updated_at = NOW()
            WHERE pipeline_name = %s
        """, (last_id, PIPELINE_NAME))

        conn.commit()

    finally:
        conn.close()


def update_freshness(dataset_name, watermark):

    conn = get_conn()

    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO metadata.data_freshness
            (dataset_name, last_watermark, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (dataset_name)
            DO UPDATE SET
                last_watermark = EXCLUDED.last_watermark,
                updated_at = NOW()
        """, (dataset_name, watermark))

        conn.commit()

    finally:
        conn.close()