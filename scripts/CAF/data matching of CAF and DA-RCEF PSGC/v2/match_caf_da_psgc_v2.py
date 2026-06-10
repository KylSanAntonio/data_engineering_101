import pandas as pd

CAF_FILE = "caf.csv"
DA_FILE = "da.csv"

# ==================================================
# LOAD + NORMALIZE
# ==================================================
def load_csv(path):
    df = pd.read_csv(path, dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    return df

caf = load_csv(CAF_FILE)
da = load_csv(DA_FILE)

# ==================================================
# STANDARDIZE REQUIRED COLUMNS
# (adjust here if your CAF uses different names)
# ==================================================
caf = caf.rename(columns={
    "region_code": "region_code",
    "province_code": "province_code"
})

da = da.rename(columns={
    "region_code": "region_code",
    "province_code": "province_code"
})

caf = caf[["region_code", "province_code"]].drop_duplicates()
da = da[["region_code", "province_code"]].drop_duplicates()

caf["source"] = "CAF"
da["source"] = "DA"

# ==================================================
# FULL OUTER MERGE (CORE LOGIC)
# ==================================================
merged = caf.merge(
    da,
    on=["region_code", "province_code"],
    how="outer",
    indicator=True,
    suffixes=("_caf", "_da")
)

# ==================================================
# BUILD STATUS LOGIC
# ==================================================
def classify(row):
    if row["_merge"] == "both":
        return "MATCHED_REGION_PROVINCE"

    elif row["_merge"] == "left_only":
        return "CAF_ONLY"

    elif row["_merge"] == "right_only":
        return "DA_ONLY"

    return "UNKNOWN"

merged["status"] = merged.apply(classify, axis=1)

# ==================================================
# PROVINCE-ONLY MATCH CHECK (REGION IGNORED)
# ==================================================
province_match = caf.merge(
    da,
    on="province_code",
    how="inner",
    suffixes=("_caf", "_da")
)

# mark province-only matches (region mismatch detection)
province_match["status"] = province_match.apply(
    lambda r: "PROVINCE_MATCH_SAME_REGION"
    if r["region_code_caf"] == r["region_code_da"]
    else "PROVINCE_MATCH_DIFF_REGION",
    axis=1
)

# ==================================================
# BUILD FINAL OUTPUT STRUCTURE
# ==================================================

# normalize columns for DA + CAF side-by-side view
final_cols = pd.DataFrame()

# start from merged (region+province match universe)
final_cols["da_region"] = merged.get("region_code", None)
final_cols["da_province"] = merged.get("province_code", None)

final_cols["caf_region"] = merged.get("region_code", None)
final_cols["caf_province"] = merged.get("province_code", None)

final_cols["status"] = merged["status"]

# ==================================================
# EXPORT MAIN FILE
# ==================================================
final_cols.to_csv("caf_da_reconciliation.csv", index=False)

# ==================================================
# OPTIONAL: APPEND PROVINCE MATCH DETAIL (separate block)
# ==================================================
province_output = province_match[[
    "region_code_caf",
    "province_code",
    "region_code_da",
    "status"
]].rename(columns={
    "region_code_caf": "caf_region",
    "region_code_da": "da_region"
})

province_output.to_csv("caf_da_province_match_detail.csv", index=False)

# ==================================================
# SUMMARY
# ==================================================
print("\n===== SUMMARY =====")
print("Total CAF:", len(caf))
print("Total DA:", len(da))
print("Reconciled rows:", len(final_cols))
print("\nStatus breakdown:")
print(final_cols["status"].value_counts())
print("===================\n")

print("Done.")