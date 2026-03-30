import pandas as pd
import os
from datetime import datetime

print("🚛 OBYR Petro Price Helper — Robust version")

raw_input = input("\nDrag and drop your raw Petro-Pass CSV file here and press Enter: ").strip()

# Clean the path (removes quotes, backslashes, extra spaces)
raw_file = raw_input.strip('"').strip("'").replace("\\", "")

if not os.path.exists(raw_file):
    print("❌ File not found. Please try dragging the file again.")
    input("Press Enter to exit...")
    exit()

# Read the Petro CSV
df = pd.read_csv(raw_file, skiprows=17, header=0)

# Keep only the columns we need
df = df.iloc[:, [0, 1, 2]].copy()
df.columns = ["Station_Name", "Province", "Price"]

# Clean data
df = df.dropna(subset=['Price']).reset_index(drop=True)
df["Station_Name"] = df["Station_Name"].astype(str).str.strip().str.upper()
df["Province"] = df["Province"].astype(str).str.strip().str.upper()

# Today's date for filename
today = datetime.now().strftime("%Y-%m-%d")
output_path = os.path.expanduser(f"~/Documents/OBYR Fuel/Prices/petro_prices_{today}.csv")

# Save the clean file
df.to_csv(output_path, index=False)

print(f"\n✅ SUCCESS! Clean Petro file created:")
print(f"   {output_path}")
print(f"   ({len(df)} stations • ready for the app)")

input("\nPress Enter to close the helper...")