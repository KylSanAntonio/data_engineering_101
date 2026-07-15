import pandas as pd
from pathlib import Path

# ==========================
# CONFIGURATION
# ==========================
file1 = r"reg_12_prov_farmers_final.csv"   # reference file
file2 = r"reg_12_prov_farmers_groups.csv"   # file with extra columns
output_report = "validation_report.csv"

# Optional: unique key column for row matching (recommended)
KEY_COLUMN = "psaref_id"   # set to None if no unique key

# ==========================
# LOAD FILES
# ==========================
print("Loading files...")

df1 = pd.read_csv(file1, dtype=str).fillna("")
df2 = pd.read_csv(file2, dtype=str).fillna("")

# Normalize column names
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

# ==========================
# FIND COMMON COLUMNS
# ==========================
common_cols = [col for col in df1.columns if col in df2.columns]

if not common_cols:
    raise ValueError("No common columns found between the two files.")

print(f"Common columns found: {len(common_cols)}")

# ==========================
# COMPARE ROWS
# ==========================
issues = []

if KEY_COLUMN and KEY_COLUMN in common_cols:
    print(f"Using key column: {KEY_COLUMN}")

    df1 = df1.set_index(KEY_COLUMN)
    df2 = df2.set_index(KEY_COLUMN)

    # Check missing and extra keys
    missing_keys = set(df1.index) - set(df2.index)
    extra_keys = set(df2.index) - set(df1.index)

    for key in missing_keys:
        issues.append({
            "Issue Type": "Missing in File2",
            "Key": key,
            "Column": "",
            "File1 Value": "",
            "File2 Value": ""
        })

    for key in extra_keys:
        issues.append({
            "Issue Type": "Extra in File2",
            "Key": key,
            "Column": "",
            "File1 Value": "",
            "File2 Value": ""
        })

    # Compare matching rows
    common_keys = set(df1.index) & set(df2.index)

    for key in common_keys:
        for col in common_cols:
            if col == KEY_COLUMN:
                continue

            val1 = str(df1.at[key, col]).strip()
            val2 = str(df2.at[key, col]).strip()

            if val1 != val2:
                issues.append({
                    "Issue Type": "Value Mismatch",
                    "Key": key,
                    "Column": col,
                    "File1 Value": val1,
                    "File2 Value": val2
                })

else:
    print("No key column provided. Comparing row-by-row by position.")

    min_rows = min(len(df1), len(df2))

    for i in range(min_rows):
        for col in common_cols:
            val1 = str(df1.iloc[i][col]).strip()
            val2 = str(df2.iloc[i][col]).strip()

            if val1 != val2:
                issues.append({
                    "Issue Type": "Value Mismatch",
                    "Row Number": i + 1,
                    "Column": col,
                    "File1 Value": val1,
                    "File2 Value": val2
                })

    if len(df1) > len(df2):
        for i in range(len(df2), len(df1)):
            issues.append({
                "Issue Type": "Missing Row in File2",
                "Row Number": i + 1
            })

    elif len(df2) > len(df1):
        for i in range(len(df1), len(df2)):
            issues.append({
                "Issue Type": "Extra Row in File2",
                "Row Number": i + 1
            })

# ==========================
# SAVE REPORT
# ==========================
if issues:
    report_df = pd.DataFrame(issues)
    report_df.to_csv(output_report, index=False)
    print(f"Issues found: {len(issues)}")
    print(f"Report saved to: {output_report}")
else:
    print("Validation successful: all common columns match exactly.")