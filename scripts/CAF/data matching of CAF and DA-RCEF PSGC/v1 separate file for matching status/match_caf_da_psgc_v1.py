import pandas as pd

def load_csv(path):
    df = pd.read_csv(path, dtype=str)

    # normalize column names (CRITICAL FIX)
    df.columns = df.columns.str.strip().str.lower()

    return df

# ==================================================
# CONFIG, 
# ==================================================
CAF_FILE = "caf.csv"
DA_FILE = "da.csv"

# ==================================================
# LOAD DATA
# ==================================================
# caf = pd.read_csv(CAF_FILE, dtype=str)
# da = pd.read_csv(DA_FILE, dtype=str)
caf = load_csv(CAF_FILE)
da = load_csv(DA_FILE)

# ==================================================
# CLEAN DATA
# ==================================================
for df in [caf, da]:
    df["region_code"] = df["region_code"].astype(str).str.strip()
    df["province_code"] = df["province_code"].astype(str).str.strip()

# Keep only needed columns
caf = caf[["region_code", "province_code"]].drop_duplicates()
da = da[["region_code", "province_code"]].drop_duplicates()

# ==================================================
# 1. EXACT MATCH (REGION + PROVINCE)
# ==================================================
matched_region_province = caf.merge(
    da,
    on=["region_code", "province_code"],
    how="inner"
)

matched_region_province.to_csv(
    "01_matched_region_province.csv",
    index=False
)

print(f"[1] Matched Region+Province: {len(matched_region_province):,}")

# ==================================================
# 2. UNMATCHED REGION + PROVINCE
# ==================================================
unmatched_region_province = caf.merge(
    da,
    on=["region_code", "province_code"],
    how="outer",
    indicator=True
)

unmatched_region_province = unmatched_region_province[
    unmatched_region_province["_merge"] != "both"
]

unmatched_region_province.to_csv(
    "02_unmatched_region_province.csv",
    index=False
)

print(f"[2] Unmatched Region+Province: {len(unmatched_region_province):,}")

# ==================================================
# 3. MATCHED PROVINCE ONLY (KEEP REGION COMPARISON)
# ==================================================
province_match = caf.merge(
    da,
    on="province_code",
    how="inner",
    suffixes=("_caf", "_da")
)

province_match.to_csv(
    "03_matched_province_only.csv",
    index=False
)

print(f"[3] Matched Province Only: {len(province_match):,}")

# ==================================================
# 4. PROVINCE MATCH ANALYSIS (SAME vs DIFFERENT REGION)
# ==================================================
same_region = province_match[
    province_match["region_code_caf"] == province_match["region_code_da"]
]

different_region = province_match[
    province_match["region_code_caf"] != province_match["region_code_da"]
]

same_region.to_csv(
    "04_province_same_region.csv",
    index=False
)

different_region.to_csv(
    "05_province_different_region.csv",
    index=False
)

print(f"[4] Province SAME region: {len(same_region):,}")
print(f"[5] Province DIFFERENT region: {len(different_region):,}")

# ==================================================
# SUMMARY
# ==================================================
print("\n========== SUMMARY ==========")
print(f"CAF records: {len(caf):,}")
print(f"DA records: {len(da):,}")
print(f"Exact matches: {len(matched_region_province):,}")
print(f"Unmatched region+province: {len(unmatched_region_province):,}")
print(f"Province matches: {len(province_match):,}")
print(f"Same region provinces: {len(same_region):,}")
print(f"Different region provinces: {len(different_region):,}")
print("=============================\n")

print("Done.")