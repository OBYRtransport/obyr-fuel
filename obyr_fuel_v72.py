"""
OBYR Fuel V7.2 — Streamlit UI
Upgrades from V6.1:
  - Google Directions API real highway routing (replaces straight-line corridor)
  - All distances in KM (was miles)
  - Routing mode badge (Google Route / Straight-line fallback / Radius)
  - Canada-only station filter (no US stations shown)
  - Corridor buffer slider now in KM
  - Detour cost slider now in $/km
  - Map shows actual route polyline when Google routing active
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim

from fuel_engine_v72 import (
    DEFAULT_YARD,
    NETWORK_COLOURS,
    authenticate_driver,
    build_price_table,
    get_base_dir,
    get_route_polyline,
    price_staleness_days,
)

try:
    import folium
    from streamlit_folium import st_folium
    MAP_AVAILABLE = True
except ImportError:
    MAP_AVAILABLE = False

try:
    from streamlit_geolocation import streamlit_geolocation
    GPS_AVAILABLE = True
except ImportError:
    GPS_AVAILABLE = False

# ── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OBYR Fuel",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = get_base_dir()
LOGO_PATH = BASE_DIR / "obyr_logo.png"

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
[data-testid="stSidebar"] { background: #0f172a; color: #e2e8f0; }
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] .stRadio label,
[data-testid="stSidebar"] p { color: #cbd5e1 !important; }
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: #f8fafc !important;
    font-size: 0.8rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-weight: 600;
}
[data-testid="metric-container"] {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1rem 1.2rem;
}
[data-testid="metric-container"] label { color: #64748b !important; font-size: 0.78rem; }
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Mono', monospace;
    font-size: 1.5rem;
    color: #0f172a;
}
.stale-warning {
    background: #fef3c7;
    border-left: 4px solid #f59e0b;
    border-radius: 6px;
    padding: 0.6rem 1rem;
    font-size: 0.85rem;
    color: #92400e;
    margin-bottom: 0.5rem;
}
.route-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-left: 8px;
}
.route-google { background: #dcfce7; color: #166534; }
.route-fallback { background: #fef9c3; color: #854d0e; }
.route-radius { background: #dbeafe; color: #1e40af; }
.login-card {
    background: white;
    border-radius: 16px;
    padding: 2.5rem;
    box-shadow: 0 4px 24px rgba(0,0,0,0.08);
    margin-top: 1rem;
}
.footer {
    font-size: 0.72rem;
    color: #94a3b8;
    text-align: center;
    padding: 1.5rem 0 0.5rem;
}
</style>
""", unsafe_allow_html=True)

# ── Session state ────────────────────────────────────────────────────────────
def _init_session():
    defaults = {
        "logged_in": False,
        "driver_name": "",
        "current_lat": DEFAULT_YARD["lat"],
        "current_lon": DEFAULT_YARD["lon"],
        "gps_acquired": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

# ── Geocoding ────────────────────────────────────────────────────────────────
@st.cache_resource
def _get_geocoder() -> Nominatim:
    return Nominatim(user_agent="obyr_fuel_v72")

@st.cache_data(ttl=3600, show_spinner=False)
def _geocode(addr: str):
    if not addr or not addr.strip():
        return None, None
    try:
        loc = _get_geocoder().geocode(addr, timeout=5)
        if loc:
            return float(loc.latitude), float(loc.longitude)
    except Exception:
        pass
    return None, None

# ── Cached price table ───────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner="Loading fuel prices…")
def _cached_price_table(
    current_lat: float,
    current_lon: float,
    dest_lat,
    dest_lon,
    network_choice: str,
    max_km: int,
    corridor_buffer_km: int,
    detour_cost: float,
):
    return build_price_table(
        current_lat=current_lat,
        current_lon=current_lon,
        dest_lat=dest_lat,
        dest_lon=dest_lon,
        network_choice=network_choice,
        max_km=max_km,
        corridor_buffer_km=corridor_buffer_km,
        detour_cost_per_km=detour_cost,
    )

# ── Map builder ──────────────────────────────────────────────────────────────
def _build_map(df, current_lat, current_lon, dest_lat=None, dest_lon=None, polyline=None):
    center_lat = (current_lat + (dest_lat or current_lat)) / 2
    center_lon = (current_lon + (dest_lon or current_lon)) / 2
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=6,
        tiles="CartoDB positron",
    )

    # Current location marker
    folium.Marker(
        [current_lat, current_lon],
        tooltip="📍 Current Location",
        icon=folium.Icon(color="green", icon="home", prefix="fa"),
    ).add_to(m)

    # Destination marker + route
    if dest_lat and dest_lon:
        folium.Marker(
            [dest_lat, dest_lon],
            tooltip="🏁 Destination",
            icon=folium.Icon(color="orange", icon="flag", prefix="fa"),
        ).add_to(m)

        if polyline and len(polyline) > 1:
            # Draw actual Google route polyline
            folium.PolyLine(
                locations=polyline,
                color="#3b82f6",
                weight=4,
                opacity=0.7,
                tooltip="Actual highway route",
            ).add_to(m)
        else:
            # Fallback dashed straight line
            folium.PolyLine(
                [[current_lat, current_lon], [dest_lat, dest_lon]],
                color="#94a3b8",
                weight=2,
                dash_array="8 4",
                tooltip="Straight-line estimate",
            ).add_to(m)

    # Station markers
    valid = df.dropna(subset=["Latitude", "Longitude"]).head(150)
    for _, row in valid.iterrows():
        network = row.get("Network", "Petro")
        color = NETWORK_COLOURS.get(network, "#64748b")
        price_str = f"${row['All_In_Price']:.3f}/L"
        savings_str = f"${row['Savings_per_1000L']:,.0f}" if pd.notna(row.get("Savings_per_1000L")) else "—"
        km_str = f"{row['Km_from_Current']:.0f} km from current"
        detour_str = f"{row.get('Detour_Extra_Km', 0):.0f} km off route"

        popup_html = f"""
        <div style='font-family:Inter,sans-serif;min-width:200px'>
            <b style='font-size:13px'>{row['Station_Name']}</b><br>
            <span style='color:#64748b;font-size:11px'>{row.get('Address','')}</span><br><br>
            <span style='font-size:15px;font-weight:700;color:{color}'>{price_str}</span>
            &nbsp;&nbsp;<span style='font-size:11px;color:#166534'>Saves {savings_str}/1kL</span><br>
            <span style='font-size:11px;color:#64748b'>{km_str} &nbsp;·&nbsp; {detour_str} &nbsp;·&nbsp; {network}</span>
        </div>
        """
        folium.CircleMarker(
            location=[row["Latitude"], row["Longitude"]],
            radius=7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            weight=1.5,
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=f"{network}: {price_str}",
        ).add_to(m)

    return m

# ── Login ────────────────────────────────────────────────────────────────────
def do_login():
    if st.session_state.logged_in:
        return
    col1, col2, col3 = st.columns([1, 1.6, 1])
    with col2:
        st.markdown("<div class='login-card'>", unsafe_allow_html=True)
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=280)
        st.markdown("### Driver Login")
        username = st.text_input("Username", key="login_user")
        password = st.text_input("Password", type="password", key="login_pass")
        if st.button("Login", type="primary", use_container_width=True):
            if not username or not password:
                st.error("Please enter your username and password.")
            elif authenticate_driver(username, password):
                st.session_state.logged_in = True
                st.session_state.driver_name = str(username).strip()
                st.rerun()
            else:
                time.sleep(0.6)
                st.error("Incorrect username or password.")
        st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

# ── Staleness banner ─────────────────────────────────────────────────────────
def _stale_banner(meta: dict):
    checks = [
        ("petro_stale_days", "latest_petro_file", "Petro"),
        ("esso_stale_days",  "latest_esso_file",  "Esso"),
        ("irving_stale_days","latest_irving_file", "Irving"),
    ]
    for days_key, file_key, label in checks:
        days = meta.get(days_key)
        fname = meta.get(file_key, "")
        if days is not None and days >= 3 and fname:
            st.markdown(
                f"<div class='stale-warning'>⚠️ {label} prices are <b>{days} days old</b> "
                f"({Path(fname).name}) — upload a newer file to Google Drive.</div>",
                unsafe_allow_html=True,
            )

# ── Table styling ─────────────────────────────────────────────────────────────
def _highlight_savings(val):
    if pd.isna(val):
        return ""
    v = float(val)
    if v > 0:
        return "background-color:#d1fae5;color:#166534"
    if v < 0:
        return "background-color:#fee2e2;color:#991b1b"
    return ""

def _highlight_composite(val):
    if pd.isna(val):
        return ""
    v = float(val)
    if v > 200:
        return "background-color:#d1fae5;color:#166534;font-weight:600"
    if v > 0:
        return "background-color:#ecfdf5;color:#166534"
    if v < 0:
        return "background-color:#fee2e2;color:#991b1b"
    return ""

def _colour_network(val):
    colours = {
        "Petro":  "background-color:#fee2e2;color:#991b1b;font-weight:600",
        "Esso":   "background-color:#dbeafe;color:#1e40af;font-weight:600",
        "Irving": "background-color:#dcfce7;color:#166534;font-weight:600",
    }
    return colours.get(str(val), "")

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    _init_session()
    do_login()

    # GPS widget
    gps_data = None
    if GPS_AVAILABLE:
        gps_data = streamlit_geolocation()

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## ⛽ OBYR Fuel V7.2")
        st.success(f"👤 {st.session_state.driver_name}")
        if st.button("Logout", use_container_width=True):
            for k in ["logged_in", "driver_name", "gps_acquired"]:
                st.session_state[k] = False if k == "logged_in" else ""
            st.rerun()
        st.divider()

        st.markdown("### 📍 Current Location")
        use_gps = st.checkbox("Use my GPS location", value=st.session_state.gps_acquired)
        if use_gps and GPS_AVAILABLE and gps_data and gps_data.get("latitude"):
            st.session_state.current_lat = float(gps_data["latitude"])
            st.session_state.current_lon = float(gps_data["longitude"])
            st.session_state.gps_acquired = True
            st.caption(f"GPS: {st.session_state.current_lat:.4f}, {st.session_state.current_lon:.4f}")
        elif not GPS_AVAILABLE:
            st.caption("GPS not available — enter address below.")

        current_address = st.text_input(
            "Or type current address",
            placeholder="e.g. 400 King St, Toronto ON",
        )
        if current_address:
            lat, lon = _geocode(current_address)
            if lat is not None:
                st.session_state.current_lat = lat
                st.session_state.current_lon = lon
                st.caption(f"📌 {lat:.4f}, {lon:.4f}")
            else:
                st.warning("Could not geocode address — using yard default.")

        st.markdown("### 🏁 Destination")
        dest_address = st.text_input(
            "Destination address",
            placeholder="Optional — enables corridor mode",
        )
        dest_lat, dest_lon = None, None
        if dest_address:
            dest_lat, dest_lon = _geocode(dest_address)
            if dest_lat is None:
                st.warning("Could not geocode destination.")

        st.markdown("### 🔧 Filters")
        network_choice = st.radio(
            "Network",
            ["All", "Petro", "Esso", "Irving"],
            index=0,
            horizontal=True,
        )

        if dest_lat is not None:
            corridor_buffer = st.slider(
                "Max detour from route (km)", 25, 200, 75, 25,
                help="How far off the actual highway a station can be"
            )
            detour_cost = st.slider(
                "Detour cost $/km (truck)", 0.50, 4.00, 1.55, 0.05,
                help="~$1.55/km = ~$2.50/mile fully loaded"
            )
            max_km = 5000
        else:
            max_km = st.slider("Max km from current location", 50, 2000, 500, 50)
            corridor_buffer = 999
            detour_cost = 1.55

        st.divider()
        if st.button("🔄 Refresh prices", use_container_width=True):
            _cached_price_table.clear()
            st.rerun()

    # ── Resolve coordinates ──────────────────────────────────────────────────
    current_lat = st.session_state.current_lat
    current_lon = st.session_state.current_lon
    current_label = current_address.strip() if current_address and current_address.strip() else DEFAULT_YARD["label"]
    dest_label = dest_address.strip() if dest_address and dest_address.strip() else "None"

    # ── Load data ────────────────────────────────────────────────────────────
    prices_df, meta = _cached_price_table(
        current_lat=current_lat,
        current_lon=current_lon,
        dest_lat=dest_lat,
        dest_lon=dest_lon,
        network_choice=network_choice,
        max_km=max_km,
        corridor_buffer_km=corridor_buffer,
        detour_cost=detour_cost,
    )

    # ── Routing mode badge ───────────────────────────────────────────────────
    routing_mode = meta.get("routing_mode", "none")
    if routing_mode == "google_directions":
        route_badge = (
            f"<span class='route-badge route-google'>🛣️ Google Route "
            f"({meta.get('route_distance_km', 0):.0f} km)</span>"
        )
    elif routing_mode == "straight_line_fallback":
        route_badge = "<span class='route-badge route-fallback'>⚠️ Straight-line fallback</span>"
    elif routing_mode == "radius":
        route_badge = f"<span class='route-badge route-radius'>📡 Radius {max_km} km</span>"
    else:
        route_badge = ""

    # ── Header ───────────────────────────────────────────────────────────────
    hcol1, hcol2 = st.columns([3, 1])
    with hcol1:
        st.markdown("## ⛽ OBYR Fuel — Triple Network")
        st.markdown(
            f"V7.2 · From: **{current_label}** · To: **{dest_label}** "
            f"{'· Corridor mode 🛣️' if dest_lat else '· Radius mode 📡'} "
            f"· Network: **{network_choice}** {route_badge}",
            unsafe_allow_html=True,
        )
    with hcol2:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=160)

    # ── Staleness banners ────────────────────────────────────────────────────
    _stale_banner(meta)

    # ── KPI metrics ──────────────────────────────────────────────────────────
    top = prices_df.iloc[0] if not prices_df.empty else None
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("National avg (all-in)", f"${meta['avg_all_in']:.3f}/L" if meta["avg_all_in"] else "—")
    c2.metric(
        "Best stop (all-in)",
        f"${top['All_In_Price']:.3f}/L" if top is not None else "—",
        delta=f"${top['All_In_Price'] - meta['avg_all_in']:.3f} vs avg" if top is not None else None,
        delta_color="inverse",
    )
    c3.metric("Saves / 1,000 L", f"${top['Savings_per_1000L']:,.0f}" if top is not None else "—")
    c4.metric(
        "Stations shown",
        f"{meta['display_rows']}",
        delta="corridor filter active" if dest_lat else None,
    )
    st.divider()

    if prices_df.empty:
        st.warning(
            "No stations found. Try widening the corridor buffer, "
            "increasing the radius, or changing the network selection."
        )
        return

    # ── Tabs ─────────────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["📋 Ranked Table", "🗺️ Map", "🔧 Data Status"])

    # ── TAB 1: Ranked Table ──────────────────────────────────────────────────
    with tab1:
        has_dest = meta.get("has_destination", False)

        display_cols = [
            "Station_Name", "Province", "Network", "Address",
            "Km_from_Current", "Km_from_Destination", "Km_from_Yard",
            "All_In_Price", "Savings_per_1000L",
        ]
        rename_map = {
            "Station_Name":       "Station",
            "Km_from_Current":    "Km (Current)",
            "Km_from_Destination":"Km (Dest)",
            "Km_from_Yard":       "Km (Yard)",
            "All_In_Price":       "All-In $/L",
            "Savings_per_1000L":  "Saves / 1kL",
        }
        if has_dest and "Detour_Extra_Km" in prices_df.columns and "Composite_Score" in prices_df.columns:
            display_cols += ["Detour_Extra_Km", "Composite_Score"]
            rename_map["Detour_Extra_Km"] = "Detour Km"
            rename_map["Composite_Score"] = "Net Value $"

        display_df = prices_df[display_cols].copy().head(75)
        display_df = display_df.rename(columns=rename_map)

        fmt = {
            "Km (Current)":  "{:.0f}",
            "Km (Dest)":     "{:.0f}",
            "Km (Yard)":     "{:.0f}",
            "All-In $/L":    "${:.3f}",
            "Saves / 1kL":   "${:,.0f}",
        }
        if has_dest:
            fmt["Detour Km"]  = "{:.0f}"
            fmt["Net Value $"] = "${:,.0f}"

        styled = (
            display_df.style
            .format(fmt)
            .map(_highlight_savings, subset=["Saves / 1kL"])
            .map(_colour_network, subset=["Network"])
        )
        if has_dest and "Net Value $" in display_df.columns:
            styled = styled.map(_highlight_composite, subset=["Net Value $"])

        st.dataframe(styled, use_container_width=True, hide_index=True)

        st.download_button(
            "⬇️ Download full list (CSV)",
            prices_df.to_csv(index=False),
            file_name=f"obyr_fuel_v72_{datetime.now().strftime('%Y-%m-%d')}.csv",
            mime="text/csv",
        )

        if has_dest:
            mode_text = (
                "Google highway routing" if routing_mode == "google_directions"
                else "straight-line corridor estimate"
            )
            st.info(
                f"💡 **Net Value $** = savings per 1,000 L minus estimated detour cost "
                f"(based on {mode_text}). Higher = better real-world stop. "
                "Adjust the detour cost slider to match your truck's running cost per km."
            )

    # ── TAB 2: Map ───────────────────────────────────────────────────────────
    with tab2:
        if not MAP_AVAILABLE:
            st.warning(
                "Map requires **folium** and **streamlit-folium**.\n\n"
                "Install with:\n```\npip install folium streamlit-folium\n```"
            )
        else:
            map_df = prices_df.dropna(subset=["Latitude", "Longitude"])
            if map_df.empty:
                st.warning("No stations with coordinates to map.")
            else:
                # Get the polyline for drawing if Google routing was used
                polyline_points = None
                if routing_mode == "google_directions" and dest_lat and dest_lon:
                    api_key = os.getenv("GOOGLE_DIRECTIONS_API_KEY", "").strip()
                    if api_key:
                        result = get_route_polyline(
                            current_lat, current_lon,
                            dest_lat, dest_lon,
                            api_key,
                        )
                        if result:
                            polyline_points, _ = result

                m = _build_map(
                    map_df, current_lat, current_lon,
                    dest_lat, dest_lon,
                    polyline=polyline_points,
                )
                st_folium(m, use_container_width=True, height=560, returned_objects=[])
                st.caption(
                    "🔴 Petro-Canada &nbsp;&nbsp; 🔵 Esso &nbsp;&nbsp; 🟢 Irving &nbsp;&nbsp; "
                    "📍 Current location &nbsp;&nbsp; 🏁 Destination &nbsp;&nbsp; "
                    "🔵 line = actual highway route &nbsp;&nbsp; "
                    "Click a pin for price details."
                )

    # ── TAB 3: Data Status ───────────────────────────────────────────────────
    with tab3:
        petro_ok  = bool(meta.get("latest_petro_file"))
        esso_ok   = bool(meta.get("latest_esso_file"))
        irving_ok = bool(meta.get("latest_irving_file"))

        s1, s2, s3 = st.columns(3)
        s1.markdown(
            f"**Petro source** \n"
            f"{'✅' if petro_ok else '⚠️'} {meta.get('petro_source') or 'not found'} \n"
            f"`{Path(meta['latest_petro_file']).name if petro_ok else 'N/A'}`"
        )
        s2.markdown(
            f"**Esso source** \n"
            f"{'✅' if esso_ok else '⚠️'} {meta.get('esso_source') or 'not found'} \n"
            f"`{Path(meta['latest_esso_file']).name if esso_ok else 'N/A'}`"
        )
        s3.markdown(
            f"**Irving source** \n"
            f"{'✅' if irving_ok else '⚠️'} {meta.get('irving_source') or 'not found'} \n"
            f"`{Path(meta['latest_irving_file']).name if irving_ok else 'N/A'}`"
        )

        # Routing status
        st.markdown("**Routing status**")
        api_key_present = bool(os.getenv("GOOGLE_DIRECTIONS_API_KEY", "").strip())
        if api_key_present:
            st.success("✅ Google Directions API key configured — real highway routing active")
        else:
            st.warning("⚠️ GOOGLE_DIRECTIONS_API_KEY not set — using straight-line corridor fallback")

        st.markdown("**Row summary**")
        summary_rows = [
            {
                "Network": "Petro",
                "Source rows": meta.get("petro_source_rows", 0),
                "Matched": meta.get("petro_matched_rows", 0),
                "Unmatched": meta.get("petro_unmatched_rows", 0),
                "Stale (days)": meta.get("petro_stale_days", "?"),
            },
            {
                "Network": "Esso",
                "Source rows": meta.get("esso_source_rows", 0),
                "Matched": meta.get("esso_matched_rows", 0),
                "Unmatched": meta.get("esso_unmatched_rows", 0),
                "Stale (days)": meta.get("esso_stale_days", "?"),
            },
            {
                "Network": "Irving",
                "Source rows": meta.get("irving_source_rows", 0),
                "Matched": meta.get("irving_matched_rows", 0),
                "Unmatched": meta.get("irving_unmatched_rows", 0),
                "Stale (days)": meta.get("irving_stale_days", "?"),
            },
        ]
        st.dataframe(pd.DataFrame(summary_rows), hide_index=True, use_container_width=True)

        st.markdown("**Irving file naming convention**")
        st.info(
            "Name your Irving price files: `irving_prices_YYYY-MM-DD.csv` \n"
            "Upload to the same Google Drive folder as Petro and Esso files. \n"
            "Required columns: `Site #`, `Fuel Price` (pre-tax $/L), `Prov`, `City`, `Site`"
        )

        unmatched = prices_df[~prices_df["Matched"]][
            ["Station_Name", "Province", "Network", "Address"]
        ].copy()
        if not unmatched.empty:
            with st.expander(f"⚠️ {len(unmatched)} unmatched stations"):
                st.dataframe(unmatched, use_container_width=True, hide_index=True)

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div class='footer'>© {datetime.now().year} OBYR Transportation Group Ltd. · OBYR Fuel V7.2</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
