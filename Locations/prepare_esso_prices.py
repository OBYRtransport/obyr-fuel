import pandas as pd
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Auto-install openpyxl if missing (one-time only)
try:
    import openpyxl
except ImportError:
    print("🔧 Installing missing openpyxl library (one-time setup)...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
    print("✅ openpyxl installed — continuing...\n")

def extract_fuel_prices(xlsx_path: str):
    print(f"📂 Loading {xlsx_path}...")
    df = pd.read_excel(xlsx_path, header=None, engine="openpyxl")
    
    print(f"Total rows in file: {len(df)}")
    
    # Find every real DIESEL LS row with a valid 6-digit SITE NUMBER (checks col 0 and col 1)
    diesel_rows = []
    for idx, row in df.iterrows():
        row_str = row.astype(str).str.cat(sep=" | ").upper()
        if "DIESEL LS" in row_str:
            for col_idx in [0, 1]:
                cell = str(row.iloc[col_idx]).strip().replace(".0", "").replace(",", "")
                if cell.isdigit() and len(cell) == 6:
                    diesel_rows.append(idx)
                    break
    
    print(f"✅ Found {len(diesel_rows)} valid DIESEL LS rows with SITE NUMBER")
    
    if not diesel_rows:
        raise ValueError("No DIESEL LS rows found.")
    
    # Extract using the exact column positions that work in your file
    diesel = df.iloc[diesel_rows].copy()
    diesel = diesel.rename(columns={
        1: "SITE_NUMBER",   # SITE NUMBER
        3: "CITY",
        7: "PROVINCE",
        21: "FUEL_PRICE",   # ← ONLY this column (pre-tax)
        27: "EFFECTIVE_DATE"
    })
    
    diesel = diesel[["SITE_NUMBER", "CITY", "PROVINCE", "FUEL_PRICE", "EFFECTIVE_DATE"]]
    diesel["SITE_NUMBER"] = diesel["SITE_NUMBER"].astype(str).str.strip()
    diesel = diesel[diesel["SITE_NUMBER"].str.len() == 6]  # final safety filter
    
    print(f"✅ Cleaned to {len(diesel):,} valid DIESEL LS prices (FUEL PRICE only)")
    
    # Get date
    date_series = df.stack().dropna().astype(str).str.strip()
    date_val = None
    for val in date_series:
        if str(val).strip().isdigit() and 40000 < int(val) < 50000:
            date_val = int(val)
            break
        if "2026-03-31" in str(val):
            date_val = "2026-03-31"
            break
    
    if isinstance(date_val, int):
        date_str = (datetime(1899, 12, 30) + pd.Timedelta(days=date_val)).strftime("%Y-%m-%d")
    else:
        date_str = date_val
    
    print(f"✅ Date detected: {date_str}")
    
    # Extract directory
    directory_start = None
    for i in range(len(df)):
        if "Site Number" in str(df.iloc[i].values) and "Location Name" in str(df.iloc[i].values):
            directory_start = i
            print(f"✅ Found directory table start at row {i}")
            break
    
    dir_df = df.iloc[directory_start:].copy()
    dir_df.columns = [f"col{k}" for k in range(len(dir_df.columns))]
    
    directory = dir_df[
        dir_df["col0"].notna() & dir_df["col0"].astype(str).str.contains(r"^\d{6}$", na=False)
    ].copy()
    
    directory = directory.rename(columns={
        "col0": "SITE_NUMBER",
        "col3": "Station_Name",
        "col7": "Address",
        "col11": "City",
        "col15": "Province"
    })
    directory = directory[["SITE_NUMBER", "Station_Name", "Address", "City", "Province"]]
    directory["SITE_NUMBER"] = directory["SITE_NUMBER"].astype(str).str.strip()
    
    # Merge (SITE NUMBER first, then name fallback)
    merged = diesel.merge(directory[["SITE_NUMBER", "Station_Name", "Address", "City"]], 
                          on="SITE_NUMBER", how="left")
    
    missing = merged[merged["Station_Name"].isna()].copy()
    if not missing.empty:
        print(f"🔄 {len(missing)} sites using name fallback...")
        for idx, row in missing.iterrows():
            city = str(row["CITY"]).strip() if pd.notna(row["CITY"]) else ""
            if city:
                name_match = directory[
                    directory["Station_Name"].str.contains(city, case=False, na=False)
                ]
                if not name_match.empty:
                    merged.at[idx, "Station_Name"] = name_match.iloc[0]["Station_Name"]
                    merged.at[idx, "Address"] = name_match.iloc[0]["Address"]
                    merged.at[idx, "City"] = name_match.iloc[0]["City"]
    
    # Final columns for your main app
    final = merged[["SITE_NUMBER", "Station_Name", "CITY", "PROVINCE", "Address", 
                    "FUEL_PRICE", "EFFECTIVE_DATE"]]
    final = final.sort_values(by=["PROVINCE", "Station_Name"])
    
    # Save to Prices folder
    script_dir = Path(__file__).parent
    prices_dir = script_dir / "Prices"
    prices_dir.mkdir(exist_ok=True)
    output_csv = prices_dir / f"esso_prices_{date_str}.csv"
    
    final.to_csv(output_csv, index=False)
    print(f"🎉 SUCCESS! Saved {len(final):,} prices → {output_csv}")
    print(f"   Matched by SITE NUMBER: {len(final) - len(missing)}")
    print(f"   Matched by name fallback: {len(missing)}")
    print(f"   Your main app can now read {output_csv} directly")
    return final

# ========================= ONE-CLICK USAGE =========================
if __name__ == "__main__":
    if len(sys.argv) > 1:
        xlsx_file = sys.argv[1]
    else:
        xlsx_file = input("Drag & drop your Fuel-pricing_u716_*.xlsx here and press Enter: ").strip().strip('"')
    
    extract_fuel_prices(xlsx_file)
    
    print("\n✅ All done!")
    input("Press Enter to close this window...")