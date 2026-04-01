cat > obyr_fuel_v3.py << 'EOF'
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim
from streamlit_geolocation import streamlit_geolocation

from fuel_engine import DEFAULT_YARD, build_price_table, get_base_dir, read_driver_master

st.set_page_config(page_title="OBYR Fuel V5", page_icon="⛽", layout="wide")

BASE_DIR = get_base_dir()
LOGO_PATH = BASE_DIR / "obyr_logo.png"


def do_login() -> None:
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.driver_name = ""

    driver_df = read_driver_master()
    if driver_df is None:
        return

    if st.session_state.logged_in:
        return

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=320)
        st.title("OBYR Fuel")
        st.subheader("Driver Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login", type="primary"):
            match = driver_df[
                (driver_df["Username"].astype(str) == username)
                & (driver_df["Password"].astype(str) == password)
            ]
            if not match.empty:
                st.session_state.logged_in = True
                st.session_state.driver_name = username
                st.rerun()
            else:
                st.error("Wrong credentials")
    st.stop()


@st.cache_resource
def get_geocoder() -> Nominatim:
    return Nominatim(user_agent="obyr_fuel_v5")


def geocode(addr: str):
    if not addr or not addr.strip():
        return None, None
    try:
        loc = get_geocoder().geocode(addr, timeout=5)
        if loc:
            return float(loc.latitude), float(loc.longitude)
    except Exception:
        return None, None
    return None, None


def highlight_savings(val):
    if pd.isna(val):
        return ""
    if float(val) > 0:
        return "background-color: #d1fae5; color: #166534"
    if float(val) < 0:
        return "background-color: #fee2e2; color: #991b1b"
    return ""


def main() -> None:
    do_login()

    st.subheader("Official Dual Network")
    st.caption("V5 • fixed station matching • tested locally against your March 31 files")
    if st.session_state.get("logged_in"):
        st.success(f"Logged in as **{st.session_state.driver_name}**")

    st.sidebar.header("Current Location")
    current_address = st.sidebar.text_input("Current Address", placeholder=DEFAULT_YARD["label"])
    if st.sidebar.button("Use my GPS"):
        loc = streamlit_geolocation()
        if loc and loc.get("latitude"):
            st.session_state.current_lat = float(loc["latitude"])
            st.session_state.current_lon = float(loc["longitude"])
            st.sidebar.success("GPS acquired")

    st.sidebar.header("Destination")
    dest_address = st.sidebar.text_input("Destination Address", placeholder="Optional")
    max_miles = st.sidebar.slider("Maximum miles from current location", 50, 2000, 1000, 50)
    network_choice = st.sidebar.radio("Show network", ["Petro", "Esso", "Both"], index=2, horizontal=True)

    current_lat = st.session_state.get("current_lat", DEFAULT_YARD["lat"])
    current_lon = st.session_state.get("current_lon", DEFAULT_YARD["lon"])
    if current_address:
        lat, lon = geocode(current_address)
        if lat is not None:
            current_lat, current_lon = lat, lon

    dest_lat, dest_lon = (None, None)
    if dest_address:
        dest_lat, dest_lon = geocode(dest_address)

    prices_df, meta = build_price_table(
        current_lat=current_lat,
        current_lon=current_lon,
        dest_lat=dest_lat,
        dest_lon=dest_lon,
        network_choice=network_choice,
        max_miles=max_miles,
    )

    top = prices_df.iloc[0] if not prices_df.empty else None
    c1, c2, c3 = st.columns(3)
    c1.metric("National all-in average", f"${meta['avg_all_in']:.3f}/L")
    c2.metric("Cheapest visible stop", f"${top['All_In_Price']:.3f}" if top is not None else "—")
    c3.metric("Best savings / 1,000L", f"${top['Savings_per_1000L']:,.0f}" if top is not None else "—")

    st.subheader(f"Best options • Current: {current_address or DEFAULT_YARD['label']} • Destination: {dest_address or 'None'}")

    display_cols = [
        "Station_Name",
        "Province",
        "Network",
        "Address",
        "Miles_from_Current",
        "Miles_from_Destination",
        "Miles_from_Yard",
        "All_In_Price",
        "Savings_per_1000L",
    ]
    display_df = prices_df[display_cols].copy().head(50)
    display_df.columns = [
        "Station",
        "Prov",
        "Network",
        "Address",
        "Miles from Current",
        "Miles from Destination",
        "Miles from Yard",
        "ALL-IN Price",
        "Savings per 1,000 L",
    ]

    st.dataframe(
        display_df.style.format(
            {
                "Miles from Current": "{:.1f}",
                "Miles from Destination": "{:.1f}",
                "Miles from Yard": "{:.1f}",
                "ALL-IN Price": "${:.3f}",
                "Savings per 1,000 L": "${:,.0f}",
            }
        ).map(highlight_savings, subset=["Savings per 1,000 L"]),
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Debug / data quality", expanded=False):
        st.write(
            {
                "latest_petro_file": Path(meta["latest_petro_file"]).name,
                "latest_esso_file": Path(meta["latest_esso_file"]).name,
                "petro_source_rows": meta["petro_source_rows"],
                "petro_matched_rows": meta["petro_stats"]["matched_rows"],
                "petro_unmatched_rows": meta["petro_stats"]["unmatched_rows"],
                "esso_source_rows": meta["esso_source_rows"],
                "esso_matched_rows": meta["esso_stats"]["matched_rows"],
                "esso_unmatched_rows": meta["esso_stats"]["unmatched_rows"],
                "display_rows": meta["display_rows"],
            }
        )

        unmatched = prices_df[~prices_df["Matched"]][["Station_Name", "Province", "Network", "Address"]].copy()
        if not unmatched.empty:
            st.write("Unmatched rows still shown using price-file address:")
            st.dataframe(unmatched, use_container_width=True, hide_index=True)

    st.download_button(
        "Download ranked fuel list",
        prices_df.to_csv(index=False),
        file_name=f"obyr_fuel_v5_{datetime.now().strftime('%Y-%m-%d')}.csv",
        mime="text/csv",
    )
    st.caption(f"© {datetime.now().year} OBYR Transportation Group Ltd. • OBYR Fuel V5")


if __name__ == "__main__":
    main()
EOF