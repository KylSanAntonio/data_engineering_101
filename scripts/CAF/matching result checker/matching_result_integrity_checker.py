import pandas as pd
import os
import glob
import csv
import re

# =========================
# CONFIG
# =========================
INPUT_FOLDER = "./csv_files"
OUTPUT_FILE = "caf_integrity_report.csv"

REQUIRED_COLUMNS = [
    "PROVINCE_CODE",
    "MUNICIPALITY_CODE",
    "BARANGAY_CODE",
    "FARMERS_SEX",
    "FARMERS_FIRST_NAME",
    "FARMERS_MIDDLE_NAME",
    "FARMERS_LAST_NAME",
    "CROP_AREA"
]

PSA_REF_COL = "PSAREF_ID"
SOURCE_COL = "DATA SOURCE NAME"


# =========================
# VALIDATION FUNCTIONS
# =========================
def is_integer(value):
    if value is None:
        return False
    value = str(value).strip()
    return re.fullmatch(r"-?\d+", value) is not None


def is_valid_sex(value):
    if value is None:
        return False
    return str(value).strip().upper() in ["MALE", "FEMALE"]


def contains_number(value):
    if value is None:
        return False
    return bool(re.search(r"\d", str(value)))


def is_valid_crop_area(value):
    if value is None:
        return True

    v = str(value).strip().upper()
    if v in ["", "NA", "N/A", "NULL"]:
        return True

    return re.fullmatch(r"\d+(\.\d+)?", str(value).strip()) is not None


# =========================
# SAFE CSV PARSER (detect broken rows)
# =========================
def parse_csv_safely(file_path):
    valid_rows = []
    structure_errors = []

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)

        try:
            header = next(reader)
        except StopIteration:
            return [], [], [{"row_index": 0, "error": "EMPTY FILE", "raw": None}]

        expected_cols = len(header)

        for i, row in enumerate(reader, start=2):
            if len(row) != expected_cols:
                structure_errors.append({
                    "row_index": i,
                    "error": f"BROKEN ROW: expected {expected_cols} cols, got {len(row)}",
                    "raw": row
                })
                continue

            valid_rows.append(dict(zip(header, row)))

    return header, valid_rows, structure_errors


# =========================
# PROCESS FILES
# =========================
all_results = []

csv_files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv"))

if not csv_files:
    print("No CSV files found.")
    exit()

for file in csv_files:
    filename = os.path.basename(file)

    header, rows, structure_errors = parse_csv_safely(file)

    if not header:
        continue

    header_upper = [h.strip().upper() for h in header]
    col_map = {col.strip().upper(): col for col in header}

    # helper for safe column access
    def get(row, col):
        return row.get(col_map.get(col))

    # =========================
    # LOG FUNCTION
    # =========================
    def log(row_idx, row, column, value, reason):
        all_results.append({
            "filename": filename,
            "row_index": row_idx,
            "psaref_id": get(row, PSA_REF_COL) if row else None,
            "data_source_name": get(row, SOURCE_COL) if row else None,
            "column": column,
            "value": value,
            "reason": reason
        })

    # =========================
    # LOG STRUCTURE ERRORS
    # =========================
    for err in structure_errors:
        all_results.append({
            "filename": filename,
            "row_index": err["row_index"],
            "psaref_id": None,
            "data_source_name": None,
            "column": "STRUCTURE",
            "value": str(err.get("raw")),
            "reason": err["error"]
        })

    # =========================
    # SKIP IF REQUIRED COLS MISSING
    # =========================
    missing = [c for c in REQUIRED_COLUMNS if c not in header_upper]

    if missing:
        all_results.append({
            "filename": filename,
            "row_index": 1,
            "psaref_id": None,
            "data_source_name": None,
            "column": "HEADER",
            "value": str(missing),
            "reason": "Missing required columns"
        })
        continue

    # =========================
    # FIELD VALIDATION
    # =========================
    for idx, row in enumerate(rows, start=2):

        # Province
        if not is_integer(get(row, "PROVINCE_CODE")):
            log(idx, row, "PROVINCE_CODE", get(row, "PROVINCE_CODE"), "Must be integer")

        # Municipality
        if not is_integer(get(row, "MUNICIPALITY_CODE")):
            log(idx, row, "MUNICIPALITY_CODE", get(row, "MUNICIPALITY_CODE"), "Must be integer")

        # Barangay
        if not is_integer(get(row, "BARANGAY_CODE")):
            log(idx, row, "BARANGAY_CODE", get(row, "BARANGAY_CODE"), "Must be integer")

        # Sex
        if not is_valid_sex(get(row, "FARMERS_SEX")):
            log(idx, row, "FARMERS_SEX", get(row, "FARMERS_SEX"), "Must be MALE or FEMALE")

        # Names
        if contains_number(get(row, "FARMERS_FIRST_NAME")):
            log(idx, row, "FARMERS_FIRST_NAME", get(row, "FARMERS_FIRST_NAME"), "Must not contain numbers")

        if contains_number(get(row, "FARMERS_MIDDLE_NAME")):
            log(idx, row, "FARMERS_MIDDLE_NAME", get(row, "FARMERS_MIDDLE_NAME"), "Must not contain numbers")

        if contains_number(get(row, "FARMERS_LAST_NAME")):
            log(idx, row, "FARMERS_LAST_NAME", get(row, "FARMERS_LAST_NAME"), "Must not contain numbers")

        # Crop area
        if not is_valid_crop_area(get(row, "CROP_AREA")):
            log(idx, row, "CROP_AREA", get(row, "CROP_AREA"), "Must be numeric or NA")


# =========================
# OUTPUT
# =========================
df_report = pd.DataFrame(all_results)
df_report.to_csv(OUTPUT_FILE, index=False)

print("CAF Integrity Check Completed")
print(f"Total issues found: {len(df_report)}")
print(f"Output saved to: {OUTPUT_FILE}")