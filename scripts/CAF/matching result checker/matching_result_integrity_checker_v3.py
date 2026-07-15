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

PSA_REF_COL = "PSAREF_ID"
SOURCE_COL = "DATA SOURCE NAME"

# =========================
# REQUIRED COLUMNS PER SOURCE
# =========================
RCEF_REQUIRED = [
    "PSAREF_ID",
    "REGION_CODE",
    "PROVINCE_CODE",
    "MUNICIPALITY_CODE",
    "BARANGAY_CODE",
    "HOUSEHOLD_HEAD_NAME",
    "FARMERS_NAME",
    "FARMERS_FIRST_NAME",
    "FARMERS_MIDDLE_NAME",
    "FARMERS_LAST_NAME_ORIGINAL",
    "FARMERS_EXTENSION_NAME",
    "HOUSEHOLD_ADDRESS",
    "CROP_AREA",
    "FARMERS_SEX",
    "DM_HH_FULLNAME",
    "DM_FARMERS_LN",
    "DM_FARMERS_FN_MI"
]

CPH_REQUIRED = [
    "CROP_AREA",
    "REGION_CODE",
    "PROVINCE_CODE",
    "MUNICIPALITY_CODE",
    "BARANGAY_CODE",
    "HOUSEHOLD_HEAD_NAME",
    "FARMERS_NAME",
    "HOUSEHOLD_ADDRESS",
    "FARMERS_SEX",
    "DM_HH_FULLNAME",
    "HOUSEHOLD_HEAD_GENDER",
    "DM_FARMERS_LN",
    "DM_FARMERS_FN_MI",
    "HOUSEHOLD_ID"
]


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
# SAFE CSV PARSER
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
# DETERMINE REQUIRED COLUMNS
# =========================
def get_required_columns(source_name):
    if not source_name:
        return []

    source_name = str(source_name).strip()

    if re.match(r"RCEFlist_UPD_region_\d+\.csv", source_name, re.IGNORECASE):
        return RCEF_REQUIRED

    elif re.match(r"cph_region\d+", source_name, re.IGNORECASE):
        return CPH_REQUIRED

    return []


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

    def get(row, col):
        return row.get(col_map.get(col.upper()))

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
    # STRUCTURE ERRORS
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
    # FIELD VALIDATION
    # =========================
    for idx, row in enumerate(rows, start=2):

        source_name = get(row, SOURCE_COL)
        required_columns = get_required_columns(source_name)

        if not required_columns:
            log(idx, row, SOURCE_COL, source_name, "Unknown source format")
            continue

        # Check missing required columns in file
        for col in required_columns:
            if col.upper() not in header_upper:
                log(idx, row, col, None, "Missing required column")

        # Check empty required fields
        for col in required_columns:
            val = get(row, col)
            if val is None or str(val).strip() == "":
                log(idx, row, col, val, "Empty required value")

        # Province
        if "PROVINCE_CODE" in header_upper:
            if not is_integer(get(row, "PROVINCE_CODE")):
                log(idx, row, "PROVINCE_CODE", get(row, "PROVINCE_CODE"), "Must be integer")

        # Municipality
        if "MUNICIPALITY_CODE" in header_upper:
            if not is_integer(get(row, "MUNICIPALITY_CODE")):
                log(idx, row, "MUNICIPALITY_CODE", get(row, "MUNICIPALITY_CODE"), "Must be integer")

        # Barangay
        if "BARANGAY_CODE" in header_upper:
            if not is_integer(get(row, "BARANGAY_CODE")):
                log(idx, row, "BARANGAY_CODE", get(row, "BARANGAY_CODE"), "Must be integer")

        # Sex
        if "FARMERS_SEX" in header_upper:
            if not is_valid_sex(get(row, "FARMERS_SEX")):
                log(idx, row, "FARMERS_SEX", get(row, "FARMERS_SEX"), "Must be MALE or FEMALE")

        # Names
        for name_col in ["FARMERS_FIRST_NAME", "FARMERS_MIDDLE_NAME", "FARMERS_LAST_NAME", "FARMERS_LAST_NAME_ORIGINAL"]:
            if name_col in header_upper:
                if contains_number(get(row, name_col)):
                    log(idx, row, name_col, get(row, name_col), "Must not contain numbers")

        # Crop area
        crop_col = "CROP_AREA" if "CROP_AREA" in header_upper else "DM_CROP_AREA"
        if crop_col in header_upper:
            if not is_valid_crop_area(get(row, crop_col)):
                log(idx, row, crop_col, get(row, crop_col), "Must be numeric or NA")


# =========================
# OUTPUT
# =========================
df_report = pd.DataFrame(all_results)
df_report.to_csv(OUTPUT_FILE, index=False)

print("CAF Integrity Check Completed")
print(f"Total issues found: {len(df_report)}")
print(f"Output saved to: {OUTPUT_FILE}")