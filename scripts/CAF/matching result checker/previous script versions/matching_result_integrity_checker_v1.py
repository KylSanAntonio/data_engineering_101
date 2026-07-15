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

EXPECTED_COL_COUNT = None  # will be determined from header


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
# CSV STRUCTURE CHECKER
# =========================
def parse_csv_safely(file_path):
    """
    Returns:
    - header columns
    - valid rows (dict)
    - structural errors (broken lines)
    """
    valid_rows = []
    errors = []

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)

        try:
            header = next(reader)
        except StopIteration:
            return [], [], [{"error": "EMPTY FILE", "row": 0}]

        expected_cols = len(header)

        for i, row in enumerate(reader, start=2):  # line numbers start at 2
            if len(row) != expected_cols:
                errors.append({
                    "row_index": i,
                    "error": f"BROKEN ROW: expected {expected_cols} cols, got {len(row)}",
                    "raw_row": row
                })
                continue

            valid_rows.append(dict(zip(header, row)))

    return header, valid_rows, errors


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

    # -------------------------
    # log structural errors first
    # -------------------------
    for err in structure_errors:
        all_results.append({
            "filename": filename,
            "row_index": err["row_index"],
            "column": "STRUCTURE",
            "value": str(err.get("raw_row")),
            "reason": err["error"]
        })

    # skip file if no usable header
    if not header:
        continue

    # check missing required columns
    header_upper = [h.strip().upper() for h in header]
    missing = [c for c in REQUIRED_COLUMNS if c not in header_upper]

    if missing:
        all_results.append({
            "filename": filename,
            "row_index": 1,
            "column": "HEADER",
            "value": str(missing),
            "reason": "Missing required columns"
        })
        continue

    # map column index
    col_map = {col.strip().upper(): col for col in header}

    # -------------------------
    # FIELD VALIDATION
    # -------------------------
    for idx, row in enumerate(rows, start=2):

        def log(col, value, reason):
            all_results.append({
                "filename": filename,
                "row_index": idx,
                "column": col,
                "value": value,
                "reason": reason
            })

        # province / municipality / barangay
        if not is_integer(row.get(col_map["PROVINCE_CODE"])):
            log("PROVINCE_CODE", row.get(col_map["PROVINCE_CODE"]), "Must be integer")

        if not is_integer(row.get(col_map["MUNICIPALITY_CODE"])):
            log("MUNICIPALITY_CODE", row.get(col_map["MUNICIPALITY_CODE"]), "Must be integer")

        if not is_integer(row.get(col_map["BARANGAY_CODE"])):
            log("BARANGAY_CODE", row.get(col_map["BARANGAY_CODE"]), "Must be integer")

        # sex
        if not is_valid_sex(row.get(col_map["FARMERS_SEX"])):
            log("FARMERS_SEX", row.get(col_map["FARMERS_SEX"]), "Must be MALE or FEMALE")

        # names
        if contains_number(row.get(col_map["FARMERS_FIRST_NAME"])):
            log("FARMERS_FIRST_NAME", row.get(col_map["FARMERS_FIRST_NAME"]), "Must not contain numbers")

        if contains_number(row.get(col_map["FARMERS_MIDDLE_NAME"])):
            log("FARMERS_MIDDLE_NAME", row.get(col_map["FARMERS_MIDDLE_NAME"]), "Must not contain numbers")

        if contains_number(row.get(col_map["FARMERS_LAST_NAME"])):
            log("FARMERS_LAST_NAME", row.get(col_map["FARMERS_LAST_NAME"]), "Must not contain numbers")

        # crop area
        if not is_valid_crop_area(row.get(col_map["CROP_AREA"])):
            log("CROP_AREA", row.get(col_map["CROP_AREA"]), "Must be numeric or NA")

# =========================
# OUTPUT
# =========================
df_report = pd.DataFrame(all_results)
df_report.to_csv(OUTPUT_FILE, index=False)

print("Integrity check completed.")
print(f"Total issues: {len(df_report)}")
print(f"Saved: {OUTPUT_FILE}")