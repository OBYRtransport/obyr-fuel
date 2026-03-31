import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from geopy.geocoders import Nominatim
from streamlit_geolocation import streamlit_geolocation

# ====================== PATHS ======================
BASE_DIR = os.getcwd()
DRIVER_MASTER = os.path.join(BASE_DIR, "Locations", "driver_master.csv")
LOGO_PATH = os.path.join(BASE_DIR, "obyr_logo.png")

# ====================== DRIVER LOGIN ======================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.driver_name = ""

if not st.session_state.logged_in:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, width=340)
        else:
            st.caption("👉 Add your company logo as obyr_logo.png")
    st.title("🚛 OBYR Fuel")
    st.subheader("Driver Login")
    if os.path.exists(DRIVER_MASTER):
        driver_df = pd.read_csv(DRIVER_MASTER)
    else:
        st.error("❌ driver_master.csv not found")
        st.stop()
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login", type="primary"):
        match = driver_df[(driver_df["Username"] == username) & (driver_df["Password"] == password)]
        if not match.empty:
            st.session_state.logged_in = True
            st.session_state.driver_name = username
            st.rerun()
        else:
            st.error("❌ Wrong credentials")
    st.stop()

# ====================== MAIN APP ======================
st.success(f"✅ Logged in as **{st.session_state.driver_name}**")

def haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return 0.0
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

PROV_TAX = {"NL":0.15,"NS":0.15,"NB":0.15,"QC":0.14975,"ON":0.13,"MB":0.07,"SK":0.06,"AB":0.05,"BC":0.12,"YT":0.05,"NT":0.05}

PRICES_DIR = os.path.join(BASE_DIR, "Prices")
master_petro = pd.read_csv(os.path.join(BASE_DIR, "Locations", "petro_pass_master.csv"), quotechar='"')
master_esso = pd.read_csv(os.path.join(BASE_DIR, "Locations", "esso_cardlock_master.csv"), quotechar='"')

st.set_page_config(page_title="OBYR Fuel V3.9", page_icon="⛽", layout="wide")
st.subheader("Official Dual Network")
st.caption("✅ Auto-loads latest prices • Address search + GPS")

# Sidebar inputs (same as before)
st.sidebar.header("📍 My Current Location")
current_address = st.sidebar.text_input("Current Address", placeholder="Enter address or city")
if st.sidebar.button("📍 Get My Current GPS Location"):
    loc = streamlit_geolocation()
    if loc and loc.get("latitude"):
        st.session_state.current_lat = loc["latitude"]
        st.session_state.current_lon = loc["longitude"]
        st.sidebar.success("GPS acquired")
st.sidebar.header("🏁 Destination")
dest_address = st.sidebar.text_input("Destination Address", placeholder="Enter address or city")
max_miles = st.sidebar.slider("Maximum miles from my current location", 50, 2000, 1000, 50)

# Geocode helper (unchanged)
@st.cache_resource
def get_geocoder(): return Nominatim(user_agent="obyr_fuel_app")
geolocator = get_geocoder()
def geocode(addr):
    if not addr: return None, None
    try:
        loc = geolocator.geocode(addr, timeout=5)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except: return None, None

current_lat = st.session_state.get("current_lat", 43.69823)
current_lon = st.session_state.get("current_lon", -79.58937)
if current_address:
    lat, lon = geocode(current_address)
    if lat is not None: current_lat, current_lon = lat, lon
dest_lat, dest_lon = geocode(dest_address) or (43.69823, -79.58937)

# Load latest price files
def load_latest(pattern):
    files = glob.glob(os.path.join(PRICES_DIR, pattern))
    return max(files, key=os.path.getctime) if files else None

petro_path = load_latest("petro_prices_*.csv")
esso_path = load_latest("esso_prices_*.csv")

if petro_path: st.success(f"✅ Loaded Petro: {os.path.basename(petro_path)}")
if esso_path: st.success(f"✅ Loaded Esso: {os.path.basename(esso_path)}")

# ====================== LOAD + MATCH DATA ======================
petro_df = pd.DataFrame()
if petro_path:
    petro_df = pd.read_csv(petro_path, skiprows=17, header=0)
    petro_df = petro_df.iloc[:, [0,1,2]].copy()
    petro_df.columns = ["Station_Name", "Province", "Price"]
    petro_df = petro_df.dropna(subset=["Price"]).reset_index(drop=True)
    # SIMPLE matching only - no filters, no word removal
    petro_df["Station_Name"] = petro_df["Station_Name"].astype(str).str.strip().str.upper()
    petro_df["Province"] = petro_df["Province"].astype(str).str.strip().str.upper()
    petro_df["Network"] = "Petro"
    petro_df = petro_df.merge(
        master_petro[["Station_Name", "Address", "Latitude", "Longitude"]],
        on="Station_Name", how="left"
    )

esso_df = pd.DataFrame()
if esso_path:
    esso_prices = pd.read_csv(esso_path)
    esso_prices.columns = [c.strip() for c in esso_prices.columns]
    if "PROVINCE" in esso_prices.columns: esso_prices = esso_prices.rename(columns={"PROVINCE": "Province"})
    if "FUEL PRICE" in esso_prices.columns: esso_prices = esso_prices.rename(columns={"FUEL PRICE": "Price"})
    esso_prices = esso_prices.dropna(subset=["Price"]).reset_index(drop=True)
    esso_prices["Province"] = esso_prices["Province"].astype(str).str.strip().str.upper()
    if esso_prices["Price"].mean() > 10:
        esso_prices["Price"] /= 100
    esso_df = esso_prices.merge(
        master_esso[["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude"]],
        on="SITE NUMBER", how="left"
    )
    esso_df["Network"] = "Esso"

# Combine
network_choice = st.sidebar.radio("🌐 Show network", ["Petro", "Esso", "Both"], index=2, horizontal=True)
if network_choice == "Petro":
    prices_df = petro_df
elif network_choice == "Esso":
    prices_df = esso_df
else:
    prices_df = pd.concat([petro_df, esso_df], ignore_index=True)

# ====================== HEAVY DEBUG SECTION ======================
with st.expander("🔍 FULL DEBUG - Station Matching (click to expand)", expanded=True):
    st.write("**Petro Master rows:**", len(master_petro))
    st.write("**Petro Price rows:**", len(petro_df))
    st.write("**Esso Master rows:**", len(master_esso))
    st.write("**Esso Price rows:**", len(esso_df))
    
    st.write("---")
    st.write("**Petro Price Station_Name samples (first 10):**")
    st.write(petro_df["Station_Name"].head(10).tolist() if not petro_df.empty else "No data")
    st.write("**Petro Master Station_Name samples (first 10):**")
    st.write(master_petro["Station_Name"].head(10).tolist())
    
    st.write("---")
    st.write("**Matched Petro stations (have Address):**", len(petro_df[petro_df["Address"].notna()]))
    st.write("**Unmatched Petro stations (no Address):**")
    unmatched = petro_df[petro_df["Address"].isna()]["Station_Name"].tolist()
    st.write(unmatched[:30])  # first 30 so it doesn't flood the screen

    st.write("**Matched Esso stations:**", len(esso_df[esso_df["Address"].notna()]) if not esso_df.empty else 0)

# ====================== CALCULATIONS & DISPLAY ======================
prices_df["Address"] = prices_df.get("Address", pd.Series(["Address missing"]*len(prices_df))).fillna("Address missing")
prices_df["Latitude"] = pd.to_numeric(prices_df.get("Latitude", pd.Series([0.0]*len(prices_df))), errors="coerce").fillna(0)
prices_df["Longitude"] = pd.to_numeric(prices_df.get("Longitude", pd.Series([0.0]*len(prices_df))), errors="coerce").fillna(0)

prices_df["Sales_Tax_Rate"] = prices_df["Province"].map(PROV_TAX).fillna(0.13)
prices_df["All_In_Price"] = (prices_df["Price"] * (1 + prices_df["Sales_Tax_Rate"])).round(3)

avg_all_in = round(prices_df["All_In_Price"].mean(), 3)

prices_df["Miles_from_Current"] = haversine(current_lat, current_lon, prices_df["Latitude"], prices_df["Longitude"]).round(1)
prices_df["Miles_from_Destination"] = haversine(dest_lat, dest_lon, prices_df["Latitude"], prices_df["Longitude"]).round(1)
prices_df["Miles_from_Yard"] = haversine(43.69823, -79.58937, prices_df["Latitude"], prices_df["Longitude"]).round(1)

prices_df = prices_df[prices_df["Miles_from_Current"] <= max_miles].copy()
prices_df = prices_df.sort_values(by=["All_In_Price", "Miles_from_Current"]).reset_index(drop=True)
prices_df["Savings_per_1000L"] = round((avg_all_in - prices_df["All_In_Price"]) * 1000, 0)

st.subheader(f"🚛 Best options • Current: {current_address or 'GPS'} • Destination: {dest_address or 'None'}")

def highlight_savings(val):
    if pd.isna(val): return ""
    return "background-color: #d1fae5; color: #166534" if val > 0 else "background-color: #fee2e2; color: #991b1b"

display_df = prices_df[["Station_Name","Province","Network","Address","Miles_from_Current","Miles_from_Destination","Miles_from_Yard","All_In_Price","Savings_per_1000L"]].copy().head(20)
display_df.columns = ["Station","Prov","Network","Address","Miles from Current","Miles from Destination","Miles from Yard","ALL-IN Price","Savings per 1,000 L"]

st.dataframe(display_df.style.format({
    "Miles from Current":"{:.1f}","Miles from Destination":"{:.1f}","Miles from Yard":"{:.1f}",
    "ALL-IN Price":"${:.3f}","Savings per 1,000 L":"${:,.0f}"
}).map(highlight_savings, subset=["Savings per 1,000 L"]), width="stretch", hide_index=True)

# Banners below table only
st.success(f"📊 National all-in average: **${avg_all_in:.3f}**/L | Showing {len(prices_df)} stations")
col1, col2 = st.columns(2)
with col1: st.metric("Cheapest for YOU", f"${prices_df['All_In_Price'].iloc[0]:.3f}" if len(prices_df)>0 else "—")
with col2: st.metric("Your best savings", f"${prices_df['Savings_per_1000L'].iloc[0]:,.0f}" if len(prices_df)>0 else "—")

st.download_button("📥 Download this list", prices_df.to_csv(index=False), f"obyr_fuel_v3_{datetime.now().strftime('%Y-%m-%d')}.csv")
st.caption(f"© {datetime.now().year} OBYR Transport Inc. • OBYR Fuel V3.9")