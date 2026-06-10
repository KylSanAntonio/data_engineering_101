import mysql.connector
import csv
from mysql.connector import Error

# --------------------------------
# DATABASE CONFIGURATION
# --------------------------------
DB_CONFIG = {
    "host": "192.168.13.63",
    "user": "ksantonio",
    "password": "w19xjcrNjs5DIajkr3-Q",
    "port": 3306,
}

# --------------------------------
# REGION DATABASES
# --------------------------------
REGIONS = [f"cph_region{i:02d}" for i in range(1, 18)]

# --------------------------------
# MAIN
# --------------------------------
distinct_codes = set()

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for region_db in REGIONS:
        print(f"Processing {region_db}...")

        query = f"""
        SELECT
            a01_reg,
            a02_prov
        FROM `{region_db}`.`level-1`
        GROUP BY a01_reg, a02_prov
        """

        cursor.execute(query)

        rows = cursor.fetchall()

        for row in rows:
            distinct_codes.add(row)

        print(f"  Found {len(rows)} combinations")

    # --------------------------------
    # EXPORT TO CSV
    # --------------------------------
    output_file = "region_province_codes.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        writer.writerow([
            "region_code",
            "province_code"
        ])

        for region_code, province_code in sorted(distinct_codes):
            writer.writerow([
                region_code,
                province_code
            ])

    print(f"\nCSV exported successfully: {output_file}")
    print(f"Total distinct combinations: {len(distinct_codes)}")

except Error as e:
    print(f"Database Error: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()

    if 'conn' in locals() and conn.is_connected():
        conn.close()
