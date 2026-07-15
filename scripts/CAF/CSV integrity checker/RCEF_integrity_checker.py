import csv
import re

# =========================
# CONFIG
# =========================
INPUT_FILE = "rcef.csv"
REPORT_FILE = "rcef_integrity_report.csv"

EXPECTED_COLUMNS = [
    "psaref_id","REGION_CODE","PROVINCE_CODE","MUNICIPALITY_CODE","BARANGAY_CODE",
    "REGION_NAME","PROVINCE_NAME","MUNICIPALITY_NAME","BARANGAY_NAME",
    "HOUSEHOLD_HEAD_NAME","FARMERS_NAME","FARMERS_FIRST_NAME","FARMERS_MIDDLE_NAME",
    "FARMERS_LAST_NAME","FARMERS_EXTENSION_NAME","HOUSEHOLD_ADDRESS",
    "CROP_AREA","FARMERS_SEX"
]

INT_FIELDS = {
    "REGION_CODE",
    "PROVINCE_CODE",
    "MUNICIPALITY_CODE",
    "BARANGAY_CODE"
}

NAME_FIELDS = {
    "FARMERS_FIRST_NAME",
    "FARMERS_MIDDLE_NAME",
    "FARMERS_LAST_NAME"
}

SEX_ALLOWED = {"MALE", "FEMALE"}

NA_VALUES = {"NA", "N/A", "NULL", ""}


# =========================
# STRICT CSV CORRUPTION DETECTOR
# =========================
def detect_csv_corruption(line: str) -> bool:
    """
    STRICT CSV CORRUPTION DETECTOR

    Detects:
    - unclosed quotes
    - malformed quote sequences (e.g. triple-quote corruption patterns)
    - broken CSV structural integrity
    """

    i = 0
    n = len(line)
    in_quote = False

    while i < n:
        c = line[i]

        if c == '"':

            # escaped quote ""
            if i + 1 < n and line[i + 1] == '"':
                i += 2
                continue

            in_quote = not in_quote

            # ❌ detect suspicious corruption patterns inside quote context
            if in_quote:
                lookahead = line[i:i+20]

                # detects patterns like """-''", ""--, etc.
                if re.search(r'"{2,}[-\']+|"\s*[-]{2,}|"\s*\'+', lookahead):
                    return True

        i += 1

    # still inside quote = invalid CSV
    return in_quote


# =========================
# FIELD VALIDATORS
# =========================
def is_int(value):
    try:
        return str(int(value)) == str(value)
    except:
        return False


def contains_number(value):
    return bool(re.search(r"\d", str(value)))


def is_float_or_na(value):
    v = str(value).strip().upper()
    if v in NA_VALUES:
        return True
    try:
        float(v)
        return True
    except:
        return False


# =========================
# MAIN
# =========================
def main():

    errors = []

    with open(INPUT_FILE, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    # -------------------------
    # HEADER VALIDATION (STRICT)
    # -------------------------
    header_line = lines[0].rstrip("\n")

    if detect_csv_corruption(header_line):
        errors.append([1, "HEADER_CORRUPTION", "Malformed CSV header structure"])
    else:
        header = next(csv.reader([header_line]))
        expected_cols = len(EXPECTED_COLUMNS)

        if header != EXPECTED_COLUMNS:
            errors.append([
                1,
                "HEADER_MISMATCH",
                f"Expected {expected_cols} columns, got {len(header)}"
            ])

    expected_cols = len(EXPECTED_COLUMNS)

    # -------------------------
    # ROW VALIDATION
    # -------------------------
    for line_num, line in enumerate(lines[1:], start=2):

        raw = line.rstrip("\n")

        # 1. STRICT RAW CORRUPTION CHECK
        if detect_csv_corruption(raw):
            errors.append([
                line_num,
                "CORRUPT_CSV_STRUCTURE",
                "Broken/malformed quote structure detected (RAW LINE REJECTED)"
            ])
            continue

        # 2. SAFE PARSE ONLY AFTER VALIDATION
        try:
            row = next(csv.reader([raw]))
        except Exception as e:
            errors.append([
                line_num,
                "PARSE_ERROR",
                str(e)
            ])
            continue

        # 3. COLUMN COUNT CHECK
        if len(row) != expected_cols:
            errors.append([
                line_num,
                "COLUMN_MISMATCH",
                f"Expected {expected_cols}, got {len(row)}"
            ])
            continue

        record = dict(zip(EXPECTED_COLUMNS, row))

        # -------------------------
        # INTEGER VALIDATION
        # -------------------------
        for field in INT_FIELDS:
            if not is_int(record[field]):
                errors.append([
                    line_num,
                    "INVALID_INTEGER",
                    f"{field} = {record[field]}"
                ])

        # -------------------------
        # CROP AREA VALIDATION
        # -------------------------
        if not is_float_or_na(record["CROP_AREA"]):
            errors.append([
                line_num,
                "INVALID_CROP_AREA",
                record["CROP_AREA"]
            ])

        # -------------------------
        # SEX VALIDATION
        # -------------------------
        if str(record["FARMERS_SEX"]).strip().upper() not in SEX_ALLOWED:
            errors.append([
                line_num,
                "INVALID_SEX",
                record["FARMERS_SEX"]
            ])

        # -------------------------
        # NAME VALIDATION
        # -------------------------
        for field in NAME_FIELDS:
            if contains_number(record[field]):
                errors.append([
                    line_num,
                    "INVALID_NAME",
                    f"{field} contains numbers: {record[field]}"
                ])

    # -------------------------
    # OUTPUT REPORT
    # -------------------------
    with open(REPORT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ROW_NUMBER", "ERROR_TYPE", "DETAILS"])
        writer.writerows(errors)

    print("======================================")
    print("STRICT RCEF INTEGRITY CHECK COMPLETE")
    print("======================================")
    print(f"Report saved to: {REPORT_FILE}")
    print(f"Total issues found: {len(errors)}")


if __name__ == "__main__":
    main()