from common.postgres import get_conn

PIPELINE_NAME = "sales_etl"


# --------------------------------------------------
# GET WATERMARK
# --------------------------------------------------
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

        # If no record exists, initialize it
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


# --------------------------------------------------
# UPDATE WATERMARK
# --------------------------------------------------
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