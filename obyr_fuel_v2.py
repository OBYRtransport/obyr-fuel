import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

PROV_TAX = {
    "NL": 0.15, "NS": 0.15, "NB": 0.15, "QC": 0.14975,
    "ON": 0.13, "MB": 0.07, "SK": 0.06, "AB": 0.05,
    "BC": 0.12, "YT": 0.05, "NT": 0.05,
}

BASE_DIR = os.path.expanduser("~/Documents/OBYR Fuel")
master_petro = pd.read_csv(os.path.join(BASE_DIR, "Locations/petro_pass_master.csv"), quotechar='"')
master_esso  = pd.read_csv(os.path.join(BASE_DIR, "Locations/esso_cardlock_master.csv"), quotechar='"')

st.set_page_config(page_title="OBYR Fuel V2.9", page_icon="⛽", layout="wide")

# Professional logo header
logo_path = os.path.join(BASE_DIR, "obyr_logo.png")
if os.path.exists(logo_path):
    st.image(logo_path, width=340)
else:
    st.caption("👉 Add your company logo as obyr_logo.png in the OBYR Fuel folder")

st.subheader("Official Dual Network")
st.caption("✅ Real-time pricing • Both networks • Driver-focused")

network_choice = st.sidebar.radio("🌐 Show network", ["Petro", "Esso", "Both"], index=2, horizontal=True)

st.sidebar.header("📍 Your Location")
driver_lat = st.sidebar.number_input("Latitude", value=43.69823, format="%.6f")
driver_lon = st.sidebar.number_input("Longitude", value=-79.58937, format="%.6f")
max_miles = st.sidebar.slider("Maximum miles from my location", 50, 1000, 300, 50)

petro_file = st.file_uploader("📄 Upload Petro-Pass price CSV", type="csv", key="petro")
esso_file  = st.file_uploader("📄 Upload Esso price CSV (with SITE NUMBER)", type="csv", key="esso")

if petro_file is None and esso_file is None:
    st.info("👆 Upload at least one price file to begin")
    st.stop()

# Petro
if petro_file is not None:
    petro_df = pd.read_csv(petro_file, skiprows=17, header=0)
    petro_df = petro_df.iloc[:, [0, 1, 2]].copy()
    petro_df.columns = ["Station_Name", "Province", "Price"]
    petro_df = petro_df.dropna(subset=['Price']).reset_index(drop=True)
    petro_df["Station_Name"] = petro_df["Station_Name"].astype(str).str.strip().str.upper()
    petro_df["Province"] = petro_df["Province"].astype(str).str.strip().str.upper()
    petro_df["Network"] = "Petro"
    petro_df = petro_df.merge(master_petro[["Station_Name", "Address", "Latitude", "Longitude"]], on="Station_Name", how="left")
else:
    petro_df = pd.DataFrame()

# Esso
if esso_file is not None:
    esso_prices = pd.read_csv(esso_file)
    esso_prices.columns = [col.strip() for col in esso_prices.columns]
    if "PROVINCE" in esso_prices.columns:
        esso_prices = esso_prices.rename(columns={"PROVINCE": "Province"})
    if "FUEL PRICE" in esso_prices.columns:
        esso_prices = esso_prices.rename(columns={"FUEL PRICE": "Price"})
    esso_prices = esso_prices.dropna(subset=['Price']).reset_index(drop=True)
    esso_prices["Province"] = esso_prices["Province"].astype(str).str.strip().str.upper()

    esso_df = esso_prices.merge(
        master_esso[["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude"]],
        on="SITE NUMBER", how="left"
    )
    esso_df["Network"] = "Esso"
else:
    esso_df = pd.DataFrame()

# Combine
if network_choice == "Petro":
    prices_df = petro_df
elif network_choice == "Esso":
    prices_df = esso_df
else:
    prices_df = pd.concat([petro_df, esso_df], ignore_index=True)

if prices_df.empty:
    st.warning("No prices loaded")
    st.stop()

# Calculations
prices_df["Address"] = prices_df.get("Address", pd.Series(["Address missing"] * len(prices_df))).fillna("Address missing")
prices_df["Latitude"] = pd.to_numeric(prices_df.get("Latitude", pd.Series([0.0] * len(prices_df))), errors="coerce").fillna(0)
prices_df["Longitude"] = pd.to_numeric(prices_df.get("Longitude", pd.Series([0.0] * len(prices_df))), errors="coerce").fillna(0)

prices_df["Sales_Tax_Rate"] = prices_df["Province"].map(PROV_TAX).fillna(0.13)
prices_df["All_In_Price"] = (prices_df["Price"] * (1 + prices_df["Sales_Tax_Rate"])).round(3)

avg_all_in = round(prices_df["All_In_Price"].mean(), 3)

prices_df["Miles_from_You"] = haversine(driver_lat, driver_lon, prices_df["Latitude"], prices_df["Longitude"]).round(1)
prices_df["Miles_from_Yard"] = haversine(43.69823, -79.58937, prices_df["Latitude"], prices_df["Longitude"]).round(1)

prices_df = prices_df[prices_df["Miles_from_You"] <= max_miles]
prices_df = prices_df.sort_values(by=["All_In_Price", "Miles_from_You"]).reset_index(drop=True)
prices_df["Savings_per_1000L"] = round((avg_all_in - prices_df["All_In_Price"]) * 1000, 0)

st.success(f"📊 National all-in average: **${avg_all_in:.3f}**/L   |   Showing {len(prices_df)} stations within {max_miles} miles")

st.subheader(f"🚛 Best options for you at {driver_lat:.4f}, {driver_lon:.4f} ({network_choice})")

# Highlight savings: GREEN for positive, RED for negative
def highlight_savings(val):
    if pd.isna(val):
        return ""
    if val > 0:
        return "background-color: #d1fae5; color: #166534"
    if val < 0:
        return "background-color: #fee2e2; color: #991b1b"
    return ""

# Clean display DataFrame (no underscores)
display_df = prices_df[["Station_Name", "Province", "Network", "Address", "Miles_from_You", "Miles_from_Yard", "All_In_Price", "Savings_per_1000L"]].copy().head(20)
display_df.columns = ["Station", "Prov", "Network", "Address", "Your Distance", "Yard Distance", "ALL-IN Price", "Savings per 1,000 L"]

# Formatting + coloring
styled_df = display_df.style.format({
    "Your Distance": "{:.1f}",
    "Yard Distance": "{:.1f}",
    "ALL-IN Price": "${:.3f}",
    "Savings per 1,000 L": "${:,.0f}"
}).applymap(highlight_savings, subset=["Savings per 1,000 L"])

st.dataframe(styled_df, use_container_width=True, hide_index=True)

col1, col2 = st.columns(2)
with col1: st.metric("Cheapest for YOU", f"${prices_df['All_In_Price'].iloc[0]:.3f}" if len(prices_df) > 0 else "—")
with col2: st.metric("Your best savings", f"${prices_df['Savings_per_1000L'].iloc[0]:,.0f}" if len(prices_df) > 0 else "—")

st.download_button(
    "📥 Download this list",
    prices_df.to_csv(index=False),
    f"obyr_fuel_v2_{datetime.now().strftime('%Y-%m-%d')}.csv"
)

st.markdown("---")
st.caption(f"© {datetime.now().year} OBYR Transport Inc. • OBYR Fuel V2.8 • Prices as of {datetime.now().strftime('%Y-%m-%d %H:%M')}")