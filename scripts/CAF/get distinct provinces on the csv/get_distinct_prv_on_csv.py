import pandas as pd
import chardet

INPUT_FILE = "RCEFlist_UPD.csv"
OUTPUT_FILE = "distinct_region_province.csv"

# Detect encoding
with open(INPUT_FILE, "rb") as f:
    raw_data = f.read(1024 * 1024)  # Read first 1 MB
    result = chardet.detect(raw_data)

encoding = result["encoding"]
confidence = result["confidence"]

print(f"Detected encoding: {encoding}")
print(f"Confidence: {confidence:.2%}")

# Read CSV using detected encoding
df = pd.read_csv(INPUT_FILE, encoding=encoding)

# Extract distinct combinations
distinct_df = (
    df[["REGION_CODE", "PROVINCE_CODE"]]
    .drop_duplicates()
    .sort_values(["REGION_CODE", "PROVINCE_CODE"])
)

# Export
distinct_df.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8"
)

print(f"Exported {len(distinct_df)} unique combinations")
print(f"Output file: {OUTPUT_FILE}")