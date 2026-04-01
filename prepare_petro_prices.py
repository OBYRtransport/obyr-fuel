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

print("📂 Petro-Pass PDF → Perfect CSV (Ultra-Clean version)")

# Drag & drop support
if len(sys.argv) > 1:
    pdf_file = sys.argv[1]
else:
    pdf_file = input("Drag & drop your Petro-Pass PDF here and press Enter: ").strip().strip('"')

# Extract full text from PDF
all_text = ""
with pdfplumber.open(pdf_file) as pdf:
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            all_text += text + "\n"

print(f"✅ PDF text extracted")

# SUPER STRICT regex + post-filter to kill all header/dash garbage
pattern = r'([A-Z][A-Za-z\s\-\'\.]{4,})\s+([A-Z]{2})\s+(\d+\.\d{4})'
matches = re.findall(pattern, all_text, re.IGNORECASE | re.MULTILINE)

diesel_data = []
for site_name, province, price in matches:
    site_name = re.sub(r'\s+', ' ', site_name.strip())
    
    # BLOCK any garbage
    if any(word in site_name.upper() for word in ["------", "PAGE", "DUE TO", "PRICE*", "EXCL", "GST/HST", "ACCOUNT", "REGION", "PRODUCT", "SITE NAME"]):
        continue
    if len(site_name) < 5 or site_name.upper().startswith("L ") or site_name.upper().startswith("TH "):
        continue
        
    diesel_data.append({
        "SITE_NAME": site_name,
        "CITY": "",
        "PROVINCE": province,
        "FUEL_PRICE": price.strip(),    # PRICE* EXCL. GST/HST (pre-tax)
        "EFFECTIVE_DATE": "2026-03-31"
    })

diesel = pd.DataFrame(diesel_data)
print(f"✅ Extracted {len(diesel)} clean DIESEL prices (no dashes, no headers)")

# Merge with your master directory
master_path = Path(__file__).parent / "esso_cardlock_master.csv"
master = pd.read_csv(master_path, on_bad_lines='skip', quoting=3, engine='python')

merged = diesel.copy()
merged["Station_Name"] = ""
merged["Address"] = ""

for idx, row in merged.iterrows():
    site = str(row["SITE_NAME"]).strip()
    prov = str(row["PROVINCE"]).strip()
    match = master[master["Station_Name"].str.contains(site, case=False, na=False)]
    if match.empty:
        match = master[(master["City"].str.contains(site, case=False, na=False)) & (master["Province"] == prov)]
    if not match.empty:
        merged.at[idx, "Station_Name"] = match.iloc[0]["Station_Name"]
        merged.at[idx, "Address"] = match.iloc[0]["Address"]
        merged.at[idx, "CITY"] = match.iloc[0]["City"]
    else:
        merged.at[idx, "Station_Name"] = site

final = merged[["SITE_NAME", "Station_Name", "CITY", "PROVINCE", "Address", "FUEL_PRICE", "EFFECTIVE_DATE"]]
final = final.sort_values(by=["PROVINCE", "Station_Name"]).reset_index(drop=True)

# Save
prices_dir = Path(__file__).parent / "Prices"
prices_dir.mkdir(exist_ok=True)
output_csv = prices_dir / f"petro_prices_{final['EFFECTIVE_DATE'].iloc[0]}.csv"
final.to_csv(output_csv, index=False)

print("\n" + "="*70)
print(f"🎉 SUCCESS! Saved {len(final)} clean prices → {output_csv}")
print("   Your main app can now read this file directly.")
print("\n✅ All done! Press Enter to close...")
input()