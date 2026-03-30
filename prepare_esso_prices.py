import pandas as pd
import os
from datetime import datetime

print("🚛 OBYR Esso Price Helper v3 — Handles messy Adobe CSV")

raw_file = input("\nDrag and drop your raw Esso CSV file here and press Enter: ").strip().strip('"')

if not os.path.exists(raw_file):
    print("❌ File not found.")
    input("Press Enter to exit...")
    exit()

# Read the messy CSV
df = pd.read_csv(raw_file, header=None, low_memory=False)

# Find the header row that contains "SITE NUMBER"
header_row = None
for i, row in df.iterrows():
    row_str = ' '.join(str(x) for x in row if pd.notna(x))
    if 'SITE NUMBER' in row_str.upper():
        header_row = i
        break

if header_row is None:
    print("❌ Could not find SITE NUMBER header.")
    input("Press Enter to exit...")
    exit()

df.columns = df.iloc[header_row]
df = df.iloc[header_row + 1:].reset_index(drop=True)

# Clean column names
df.columns = [str(col).strip() for col in df.columns]

# Locate required columns
site_col = next((c for c in df.columns if "SITE NUMBER" in str(c).upper()), None)
prov_col = next((c for c in df.columns if "PROVINCE" in str(c).upper()), None)
price_col = next((c for c in df.columns if "FUEL PRICE" in str(c).upper()), None)

if not all([site_col, prov_col, price_col]):
    print("❌ Could not locate required columns.")
    print("Found columns:", list(df.columns))
    input("Press Enter to exit...")
    exit()

# Keep only needed columns
df = df[[site_col, prov_col, price_col]].copy()
df.columns = ["SITE NUMBER", "Province", "Price"]

# Clean and convert Price (this fixes the string error)
df["Price"] = df["Price"].astype(str).str.strip()
df["Price"] = pd.to_numeric(df["Price"], errors='coerce')

# Drop rows where Price could not be converted
df = df.dropna(subset=["Price"]).reset_index(drop=True)

# Convert cents to dollars
df["Price"] = df["Price"] / 100

# Today's date for filename
today = datetime.now().strftime("%Y-%m-%d")
output_path = os.path.expanduser(f"~/Documents/OBYR Fuel/Prices/esso_prices_{today}.csv")

# Save the clean file
df.to_csv(output_path, index=False)

print(f"\n✅ SUCCESS! Clean Esso file created:")
print(f"   {output_path}")
print(f"   ({len(df)} stations • prices now in dollars • ready for the app)")

input("\nPress Enter to close the helper...")