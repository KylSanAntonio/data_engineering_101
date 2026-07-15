import pandas as pd

# ==========================
# CONFIGURATION
# ==========================
file1 = r"reg_12_prov_farmers_final_2.csv"
file2 = r"reg_12_prov_farmers_groups.csv"
output_report = "validation_report.csv"

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
# GET COMMON COLUMNS
# ==========================
common_cols = [col for col in df1.columns if col in df2.columns]

if not common_cols:
    raise ValueError("No common columns found.")

print(f"Comparing {len(common_cols)} common columns...")

# Keep only common columns
df1_common = df1[common_cols].copy()
df2_common = df2[common_cols].copy()

# Normalize values (trim spaces)
for col in common_cols:
    df1_common[col] = df1_common[col].astype(str).str.strip()
    df2_common[col] = df2_common[col].astype(str).str.strip()

# ==========================
# SORT ALL ROWS (ignore order)
# ==========================
df1_sorted = df1_common.sort_values(by=common_cols).reset_index(drop=True)
df2_sorted = df2_common.sort_values(by=common_cols).reset_index(drop=True)

issues = []

# Check row count
if len(df1_sorted) != len(df2_sorted):
    issues.append({
        "Issue Type": "Row Count Mismatch",
        "File1 Rows": len(df1_sorted),
        "File2 Rows": len(df2_sorted)
    })

# Compare row-by-row after sorting
min_rows = min(len(df1_sorted), len(df2_sorted))

for i in range(min_rows):
    for col in common_cols:
        val1 = df1_sorted.at[i, col]
        val2 = df2_sorted.at[i, col]

        if val1 != val2:
            issues.append({
                "Issue Type": "Value Mismatch",
                "Sorted Row": i + 1,
                "Column": col,
                "File1 Value": val1,
                "File2 Value": val2
            })

# ==========================
# SAVE REPORT
# ==========================
if issues:
    report_df = pd.DataFrame(issues)
    report_df.to_csv(output_report, index=False)
    print(f"Issues found: {len(issues)}")
    print(f"Report saved to {output_report}")
else:
    print("Validation successful: both files match exactly (ignoring extra columns and row order).")