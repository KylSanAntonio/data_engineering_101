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

# Keep only needed columns
caf = caf[["region_code", "province_code"]].drop_duplicates()
da = da[["region_code", "province_code"]].drop_duplicates()

# rename for clarity
caf = caf.rename(columns={
    "region_code": "caf_region",
    "province_code": "province_code"
})

da = da.rename(columns={
    "region_code": "da_region",
    "province_code": "province_code"
})

# ==================================================
# STEP 1: PROVINCE-LEVEL MATCH (PRIMARY JOIN)
# ==================================================
province_merge = caf.merge(
    da,
    on="province_code",
    how="outer",
    indicator=True
)

# ==================================================
# STEP 2: CLASSIFY PROVINCE STATUS
# ==================================================
def province_status(row):
    if row["_merge"] == "both":
        return "PROVINCE_MATCHED"
    elif row["_merge"] == "left_only":
        return "CAF_ONLY_PROVINCE"
    elif row["_merge"] == "right_only":
        return "DA_ONLY_PROVINCE"
    return "UNKNOWN"

province_merge["province_status"] = province_merge.apply(province_status, axis=1)

# ==================================================
# STEP 3: REGION COMPARISON (ONLY FOR MATCHED PROVINCES)
# ==================================================
province_merge["region_status"] = None

matched_mask = province_merge["_merge"] == "both"

province_merge.loc[matched_mask, "region_status"] = province_merge.loc[matched_mask].apply(
    lambda r: "REGION_MATCHED"
    if r["caf_region"] == r["da_region"]
    else "REGION_MISMATCH",
    axis=1
)

# Optional: label unmatched provinces region status
province_merge.loc[~matched_mask, "region_status"] = "NOT_APPLICABLE"

# ==================================================
# STEP 4: FINAL STRUCTURE
# ==================================================
final = province_merge[[
    "province_code",
    "caf_region",
    "da_region",
    "province_status",
    "region_status",
    "_merge"
]]

# ==================================================
# EXPORT
# ==================================================
final.to_csv("caf_da_province_first_recon.csv", index=False)

# ==================================================
# SUMMARY
# ==================================================
print("\n===== PROVINCE STATUS =====")
print(final["province_status"].value_counts())

print("\n===== REGION STATUS (FOR MATCHED PROVINCES) =====")
print(final["region_status"].value_counts())

print("\nDone.")