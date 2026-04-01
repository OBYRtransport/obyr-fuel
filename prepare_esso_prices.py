import pdfplumber
import pandas as pd
import sys
import re
from pathlib import Path
import subprocess
import sys as py_sys

# Auto-install pdfplumber if needed
try:
    import pdfplumber
except ImportError:
    print("📦 Installing pdfplumber (one-time)...")
    subprocess.check_call([py_sys.executable, "-m", "pip", "install", "pdfplumber"])
    print("✅ Installed!")

print("📂 Esso PDF → Perfect CSV (Full Automation - FUEL PRICE version)")

# Drag & drop support
if len(sys.argv) > 1:
    pdf_file = sys.argv[1]
else:
    pdf_file = input("Drag & drop your Fuel-pricing_u716_*.pdf here and press Enter: ").strip().strip('"')

# Extract full text from PDF
all_text = ""
with pdfplumber.open(pdf_file) as pdf:
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            all_text += text + "\n"

print(f"✅ PDF text extracted")

# Regex that reliably captures FUEL PRICE (the pre-tax column you need)
pattern = r'(\d{6})\s+([A-Za-z\s\-]+?)\s+([A-Z]{2})\s+DIESEL LS.*?\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+\s+(\d{3}\.\d)'
matches = re.findall(pattern, all_text, re.IGNORECASE)

diesel_data = []
for site, city, province, fuel_price in matches:
    diesel_data.append({
        "SITE_NUMBER": site.strip(),
        "CITY": city.strip(),
        "PROVINCE": province.strip(),
        "FUEL_PRICE": fuel_price.strip(),      # ← FUEL PRICE (pre-tax)
        "EFFECTIVE_DATE": "2026-03-31"
    })

diesel = pd.DataFrame(diesel_data)
print(f"✅ Extracted {len(diesel)} DIESEL LS prices (using FUEL PRICE column)")

# Merge with your master directory
master_path = Path(__file__).parent / "esso_cardlock_master.csv"
master = pd.read_csv(master_path, on_bad_lines='skip', quoting=3, engine='python')

diesel["SITE_NUMBER"] = diesel["SITE_NUMBER"].astype(str)
master["SITE NUMBER"] = master["SITE NUMBER"].astype(str)

merged = diesel.merge(master[["SITE NUMBER", "Station_Name", "Address", "City"]],
                      left_on="SITE_NUMBER", right_on="SITE NUMBER", how="left")

# City fallback for the 5 missing sites
missing = merged[merged["Station_Name"].isna()].copy()
if not missing.empty:
    print(f"🔄 Filling {len(missing)} stations by city name...")
    for idx, row in missing.iterrows():
        city = str(row["CITY"]).strip()
        if city:
            match = master[master["Station_Name"].str.contains(city, case=False, na=False)]
            if not match.empty:
                merged.at[idx, "Station_Name"] = match.iloc[0]["Station_Name"]
                merged.at[idx, "Address"] = match.iloc[0]["Address"]
                merged.at[idx, "City"] = match.iloc[0]["City"]

final = merged[["SITE_NUMBER", "Station_Name", "CITY", "PROVINCE", "Address", "FUEL_PRICE", "EFFECTIVE_DATE"]]
final = final.sort_values(by=["PROVINCE", "Station_Name"]).reset_index(drop=True)

# Save
prices_dir = Path(__file__).parent / "Prices"
prices_dir.mkdir(exist_ok=True)
output_csv = prices_dir / f"esso_prices_{final['EFFECTIVE_DATE'].iloc[0]}.csv"
final.to_csv(output_csv, index=False)

print("\n" + "="*70)
print(f"🎉 SUCCESS! Saved {len(final)} prices → {output_csv}")
print("   Your main app can now read this file directly.")
print("\n✅ All done! Press Enter to close...")
input()