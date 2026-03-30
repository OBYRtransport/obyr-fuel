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
    st.markdown("### Please log in to view fuel prices")

    if os.path.exists(DRIVER_MASTER):
        driver_df = pd.read_csv(DRIVER_MASTER)
    else:
        st.error("❌ driver_master.csv not found in Locations folder")
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
            st.error("❌ Incorrect username or password")

    st.caption("Contact dispatch if you need credentials")
    st.stop()

# ====================== MAIN APP ======================
st.success(f"✅ Logged in as **{st.session_state.driver_name}**")

def haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return np.full_like(lat2, 0.0) if hasattr(lat2, "__len__") else 0.0
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

PRICES_DIR = os.path.join(BASE_DIR, "Prices")

master_petro = pd.read_csv(os.path.join(BASE_DIR, "Locations", "petro_pass_master.csv"), quotechar='"')
master_esso = pd.read_csv(os.path.join(BASE_DIR, "Locations", "esso_cardlock_master.csv"), quotechar='"')

st.set_page_config(page_title="OBYR Fuel V3.8", page_icon="⛽", layout="wide")

st.subheader("Official Dual Network")
st.caption("✅ Auto-loads latest prices • Address search + GPS")

st.sidebar.header("📍 My Current Location")
current_address = st.sidebar.text_input("Current Address", placeholder="Enter address or city")

if st.sidebar.button("📍 Get My Current GPS Location"):
    loc = streamlit_geolocation()
    if loc and loc.get("latitude"):
        st.session_state.current_lat = loc["latitude"]
        st.session_state.current_lon = loc["longitude"]
        st.sidebar.success(f"✅ GPS acquired: {loc['latitude']:.4f}, {loc['longitude']:.4f}")
    else:
        st.sidebar.warning("Tap again or allow location permission")

st.sidebar.header("🏁 Destination")
dest_address = st.sidebar.text_input("Destination Address", placeholder="Enter address or city")

max_miles = st.sidebar.slider("Maximum miles from my current location", 50, 2000, 1000, 50)

# Geocode
@st.cache_resource
def get_geocoder():
    return Nominatim(user_agent="obyr_fuel_app")
geolocator = get_geocoder()

def geocode(address):
    if not address or address.strip() == "":
        return None, None
    try:
        location = geolocator.geocode(address, timeout=5)
        return (location.latitude, location.longitude) if location else (None, None)
    except:
        return None, None

current_lat = st.session_state.get("current_lat", 43.69823)
current_lon = st.session_state.get("current_lon", -79.58937)
if current_address:
    lat, lon = geocode(current_address)
    if lat is not None and lon is not None:
        current_lat, current_lon = lat, lon

dest_lat, dest_lon = geocode(dest_address)
if dest_lat is None or dest_lon is None:
    dest_lat, dest_lon = 43.69823, -79.58937

# Auto-load prices
def load_latest_price_file(pattern):
    files = glob.glob(os.path.join(PRICES_DIR, pattern))
    if not files:
        return None
    return max(files, key=os.path.getctime)

petro_path = load_latest_price_file("petro_prices_*.csv")
esso_path = load_latest_price_file("esso_prices_*.csv")

if petro_path:
    st.success(f"✅ Loaded Petro prices: {os.path.basename(petro_path)}")
if esso_path:
    st.success(f"✅ Loaded Esso prices: {os.path.basename(esso_path)}")

# Load data
if petro_path:
    petro_df = pd.read_csv(petro_path, skiprows=17, header=0)
else:
    petro_df = pd.DataFrame()

if esso_path:
    esso_prices = pd.read_csv(esso_path)
else:
    esso_prices = pd.DataFrame()

if not petro_df.empty:
    petro_df = petro_df.iloc[:, [0, 1, 2]].copy()
    petro_df.columns = ["Station_Name", "Province", "Price"]
    petro_df = petro_df.dropna(subset=['Price']).reset_index(drop=True)
    petro_df["Station_Name"] = petro_df["Station_Name"].astype(str).str.strip().str.upper()
    petro_df["Province"] = petro_df["Province"].astype(str).str.strip().str.upper()
    petro_df["Network"] = "Petro"
    petro_df = petro_df.merge(master_petro[["Station_Name", "Address", "Latitude", "Longitude"]], on="Station_Name", how="left")

if not esso_prices.empty:
    esso_prices.columns = [col.strip() for col in esso_prices.columns]
    if "PROVINCE" in esso_prices.columns:
        esso_prices = esso_prices.rename(columns={"PROVINCE": "Province"})
    if "FUEL PRICE" in esso_prices.columns:
        esso_prices = esso_prices.rename(columns={"FUEL PRICE": "Price"})
    esso_prices = esso_prices.dropna(subset=['Price']).reset_index(drop=True)
    esso_prices["Province"] = esso_prices["Province"].astype(str).str.strip().str.upper()
    if esso_prices["Price"].mean() > 10:
        esso_prices["Price"] = esso_prices["Price"] / 100
    esso_df = esso_prices.merge(master_esso[["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude"]], on="SITE NUMBER", how="left")
    esso_df["Network"] = "Esso"
else:
    esso_df = pd.DataFrame()

if network_choice := st.sidebar.radio("🌐 Show network", ["Petro", "Esso", "Both"], index=2, horizontal=True):
    if network_choice == "Petro":
        prices_df = petro_df
    elif network_choice == "Esso":
        prices_df = esso_df
    else:
        prices_df = pd.concat([petro_df, esso_df], ignore_index=True)

if prices_df.empty:
    st.warning("No price files found. Please run the helpers first.")
    st.stop()

# Calculations
prices_df["Address"] = prices_df.get("Address", pd.Series(["Address missing"] * len(prices_df))).fillna("Address missing")
prices_df["Latitude"] = pd.to_numeric(prices_df.get("Latitude", pd.Series([0.0] * len(prices_df))), errors="coerce").fillna(0)
prices_df["Longitude"] = pd.to_numeric(prices_df.get("Longitude", pd.Series([0.0] * len(prices_df))), errors="coerce").fillna(0)

prices_df["Sales_Tax_Rate"] = prices_df["Province"].map(PROV_TAX).fillna(0.13)
prices_df["All_In_Price"] = (prices_df["Price"] * (1 + prices_df["Sales_Tax_Rate"])).round(3)

avg_all_in = round(prices_df["All_In_Price"].mean(), 3)

prices_df["Miles_from_Current"] = haversine(current_lat, current_lon, prices_df["Latitude"], prices_df["Longitude"]).round(1)
prices_df["Miles_from_Destination"] = haversine(dest_lat, dest_lon, prices_df["Latitude"], prices_df["Longitude"]).round(1)
prices_df["Miles_from_Yard"] = haversine(43.69823, -79.58937, prices_df["Latitude"], prices_df["Longitude"]).round(1)

prices_df = prices_df[prices_df["Miles_from_Current"] <= max_miles]
prices_df = prices_df.sort_values(by=["All_In_Price", "Miles_from_Current"]).reset_index(drop=True)
prices_df["Savings_per_1000L"] = round((avg_all_in - prices_df["All_In_Price"]) * 1000, 0)

st.subheader(f"🚛 Best options • Current: {current_address or 'GPS'} • Destination: {dest_address or 'None'}")

def highlight_savings(val):
    if pd.isna(val):
        return ""
    if val > 0:
        return "background-color: #d1fae5; color: #166534"
    if val < 0:
        return "background-color: #fee2e2; color: #991b1b"
    return ""

display_df = prices_df[["Station_Name", "Province", "Network", "Address", "Miles_from_Current", "Miles_from_Destination", "Miles_from_Yard", "All_In_Price", "Savings_per_1000L"]].copy().head(20)
display_df.columns = ["Station", "Prov", "Network", "Address", "Miles from Current", "Miles from Destination", "Miles from Yard", "ALL-IN Price", "Savings per 1,000 L"]

styled_df = display_df.style.format({
    "Miles from Current": "{:.1f}",
    "Miles from Destination": "{:.1f}",
    "Miles from Yard": "{:.1f}",
    "ALL-IN Price": "${:.3f}",
    "Savings per 1,000 L": "${:,.0f}"
}).map(highlight_savings, subset=["Savings per 1,000 L"])

st.dataframe(styled_df, width="stretch", hide_index=True)

# Banners only below the table
if petro_path:
    st.success(f"✅ Loaded Petro prices: {os.path.basename(petro_path)}")
if esso_path:
    st.success(f"✅ Loaded Esso prices: {os.path.basename(esso_path)}")

st.success(f"📊 National all-in average: **${avg_all_in:.3f}**/L | Showing {len(prices_df)} stations")

col1, col2 = st.columns(2)
with col1: st.metric("Cheapest for YOU", f"${prices_df['All_In_Price'].iloc[0]:.3f}" if len(prices_df) > 0 else "—")
with col2: st.metric("Your best savings", f"${prices_df['Savings_per_1000L'].iloc[0]:,.0f}" if len(prices_df) > 0 else "—")

st.download_button("📥 Download this list", prices_df.to_csv(index=False), f"obyr_fuel_v3_{datetime.now().strftime('%Y-%m-%d')}.csv")

st.markdown("---")
st.caption(f"© {datetime.now().year} OBYR Transport Inc. • OBYR Fuel V3.8")