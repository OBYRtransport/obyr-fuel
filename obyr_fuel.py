"""
OBYR Fuel — Streamlit UI
Fixes from V7.2:
  1. White box on login — GPS widget only mounts post-login, skeleton hidden
  2. Address entry — server-side Google Places Autocomplete. User types into
     a standard Streamlit text_input, Python calls Places API and returns
     suggestions as a selectbox. No iframes, no JS postMessage, no crashes.
  3. Duplicate Google Directions API call eliminated — polyline cached once
     and reused by map tab.
  4. Streamlit deprecation warnings fixed — use_container_width stays but
     skeletons and GPS crash are resolved.
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests as req
import streamlit as st

from fuel_engine import (
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

# streamlit_geolocation is intentionally NOT imported at the top level.
# Importing it causes the widget to inject a white DOM element on every page,
# including the login screen.  It is imported lazily inside main() only after
# the user has successfully authenticated.
GPS_AVAILABLE = False  # updated at runtime after login gate

st.set_page_config(
    page_title="OBYR Fuel", page_icon="⛽",
    layout="wide", initial_sidebar_state="expanded",
)

BASE_DIR     = get_base_dir()
LOGO_PATH    = BASE_DIR / "obyr_logo.png"
MAPS_API_KEY = os.getenv("GOOGLE_DIRECTIONS_API_KEY", "").strip()

PLACES_AUTOCOMPLETE_URL = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
PLACES_DETAILS_URL      = "https://maps.googleapis.com/maps/api/place/details/json"

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
    color: #f8fafc !important; font-size: 0.8rem;
    letter-spacing: 0.1em; text-transform: uppercase; font-weight: 600;
}
[data-testid="metric-container"] {
    background: #f8fafc; border: 1px solid #e2e8f0;
    border-radius: 10px; padding: 1rem 1.2rem;
}
[data-testid="metric-container"] label { color: #64748b !important; font-size: 0.78rem; }
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Mono', monospace; font-size: 1.5rem; color: #0f172a;
}
.stale-warning {
    background: #fef3c7; border-left: 4px solid #f59e0b;
    border-radius: 6px; padding: 0.6rem 1rem;
    font-size: 0.85rem; color: #92400e; margin-bottom: 0.5rem;
}
.route-badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 0.75rem; font-weight: 600; margin-left: 8px;
}
.route-google   { background: #dcfce7; color: #166534; }
.route-fallback { background: #fef9c3; color: #854d0e; }
.route-radius   { background: #dbeafe; color: #1e40af; }
.login-card {
    background: white; border-radius: 16px; padding: 2.5rem;
    box-shadow: 0 4px 24px rgba(0,0,0,0.08); margin-top: 1rem;
}
.footer { font-size: 0.72rem; color: #94a3b8; text-align: center; padding: 1.5rem 0 0.5rem; }
[data-testid="stSkeleton"] { display: none !important; }
</style>
""", unsafe_allow_html=True)


def _init_session():
    defaults = {
        "logged_in": False, "driver_name": "",
        "current_lat": DEFAULT_YARD["lat"], "current_lon": DEFAULT_YARD["lon"],
        "current_label": DEFAULT_YARD["label"],
        "dest_lat": None, "dest_lon": None, "dest_label": "",
        "gps_acquired": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


@st.cache_data(ttl=300, show_spinner=False)
def _places_suggestions(query: str, api_key: str) -> list:
    if not query or len(query) < 3 or not api_key:
        return []
    try:
        r = req.get(PLACES_AUTOCOMPLETE_URL, params={
            "input": query, "components": "country:ca",
            "types": "geocode|establishment", "key": api_key,
        }, timeout=5)
        data = r.json()
        if data.get("status") == "OK":
            return [{"description": p["description"], "place_id": p["place_id"]}
                    for p in data.get("predictions", [])]
    except Exception:
        pass
    return []


@st.cache_data(ttl=3600, show_spinner=False)
def _place_coords(place_id: str, api_key: str):
    try:
        r = req.get(PLACES_DETAILS_URL, params={
            "place_id": place_id, "fields": "geometry,formatted_address", "key": api_key,
        }, timeout=5)
        data = r.json()
        if data.get("status") == "OK":
            loc  = data["result"]["geometry"]["location"]
            addr = data["result"].get("formatted_address", "")
            return float(loc["lat"]), float(loc["lng"]), addr
    except Exception:
        pass
    return None


def _search_places(query: str, **kwargs) -> list:
    """
    Search function for streamlit-searchbox.
    Returns list of (label, value) tuples where value is place_id.
    Called on every keystroke — results appear instantly in dropdown.
    """
    if not query or len(query) < 2 or not MAPS_API_KEY:
        return []
    suggestions = _places_suggestions(query, MAPS_API_KEY)
    return [s["description"] for s in suggestions]


def places_input(label: str, search_key: str, select_key: str, placeholder: str):
    """
    Instant Google Places autocomplete using streamlit-searchbox.
    As-you-type suggestions appear immediately — no server round-trip delay.
    Falls back to server-side selectbox if searchbox not available.
    """
    try:
        from streamlit_searchbox import st_searchbox

        st.markdown(f"**{label}**")
        chosen = st_searchbox(
            _search_places,
            key=search_key,
            placeholder=placeholder,
            clear_on_submit=False,
            label_visibility="collapsed",
        )

        if not chosen:
            return None

        # chosen is the description string — look up coordinates
        suggestions = _places_suggestions(chosen, MAPS_API_KEY)
        if not suggestions:
            return None

        # Find exact match or use first result
        match = next((s for s in suggestions if s["description"] == chosen), suggestions[0])
        result = _place_coords(match["place_id"], MAPS_API_KEY)
        if result:
            lat, lon, addr = result
            st.caption(f"✓ {lat:.4f}, {lon:.4f}")
            return {"lat": lat, "lon": lon, "address": addr}
        return None

    except ImportError:
        # Fallback: server-side selectbox (reliable but not instant)
        query = st.text_input(label, placeholder=placeholder, key=search_key)
        if not query or len(query) < 3:
            return None

        if not MAPS_API_KEY:
            from geopy.geocoders import Nominatim
            @st.cache_resource
            def _nom():
                return Nominatim(user_agent="obyr_fuel")
            @st.cache_data(ttl=3600, show_spinner=False)
            def _geo(a):
                try:
                    loc = _nom().geocode(a + ", Canada", timeout=5)
                    if loc:
                        return float(loc.latitude), float(loc.longitude)
                except Exception:
                    pass
                return None, None
            lat, lon = _geo(query)
            if lat:
                st.caption(f"📌 {lat:.4f}, {lon:.4f}")
                return {"lat": lat, "lon": lon, "address": query}
            st.caption("Could not locate — try adding city and province")
            return None

        suggestions = _places_suggestions(query, MAPS_API_KEY)
        if not suggestions:
            st.caption("No suggestions — try a different search")
            return None

        options = ["— select a result —"] + [s["description"] for s in suggestions]
        chosen  = st.selectbox("", options=options, key=select_key, label_visibility="collapsed")
        if chosen == "— select a result —":
            return None

        place_id = next((s["place_id"] for s in suggestions if s["description"] == chosen), None)
        if not place_id:
            return None

        result = _place_coords(place_id, MAPS_API_KEY)
        if result:
            lat, lon, addr = result
            st.caption(f"✓ {lat:.4f}, {lon:.4f}")
            return {"lat": lat, "lon": lon, "address": addr}
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def _cached_polyline(clat, clon, dlat, dlon):
    if not MAPS_API_KEY:
        return None, 0.0
    result = get_route_polyline(clat, clon, dlat, dlon, MAPS_API_KEY)
    return result if result else (None, 0.0)


@st.cache_data(ttl=3600, show_spinner="Loading fuel prices…")
def _cached_price_table(clat, clon, dlat, dlon, network, max_km, buffer_km, detour_cost):
    return build_price_table(
        current_lat=clat, current_lon=clon,
        dest_lat=dlat, dest_lon=dlon,
        network_choice=network, max_km=max_km,
        corridor_buffer_km=buffer_km, detour_cost_per_km=detour_cost,
    )


def _build_map(df, clat, clon, dlat=None, dlon=None, polyline=None):
    m = folium.Map(
        location=[(clat + (dlat or clat)) / 2, (clon + (dlon or clon)) / 2],
        zoom_start=6, tiles="CartoDB positron",
    )
    folium.Marker([clat, clon], tooltip="📍 Current",
                  icon=folium.Icon(color="green", icon="home", prefix="fa")).add_to(m)
    if dlat and dlon:
        folium.Marker([dlat, dlon], tooltip="🏁 Destination",
                      icon=folium.Icon(color="orange", icon="flag", prefix="fa")).add_to(m)
        if polyline and len(polyline) > 1:
            folium.PolyLine(polyline, color="#3b82f6", weight=4, opacity=0.75,
                            tooltip="Actual highway route").add_to(m)
        else:
            folium.PolyLine([[clat, clon], [dlat, dlon]], color="#94a3b8",
                            weight=2, dash_array="8 4").add_to(m)

    for _, row in df.dropna(subset=["Latitude", "Longitude"]).head(150).iterrows():
        net   = row.get("Network", "Petro")
        color = NETWORK_COLOURS.get(net, "#64748b")
        price = f"${row['All_In_Price']:.3f}/L"
        saves = f"${row['Savings_per_1000L']:,.0f}" if pd.notna(row.get("Savings_per_1000L")) else "—"
        html  = (f"<div style='font-family:Inter,sans-serif;min-width:200px'>"
                 f"<b>{row['Station_Name']}</b><br>"
                 f"<span style='color:#64748b;font-size:11px'>{row.get('Address','')}</span><br><br>"
                 f"<span style='font-size:15px;font-weight:700;color:{color}'>{price}</span>"
                 f"&nbsp;&nbsp;<span style='font-size:11px;color:#166534'>Saves {saves}/1kL</span><br>"
                 f"<span style='font-size:11px;color:#64748b'>"
                 f"{row['Km_from_Current']:.0f} km · {row.get('Detour_Extra_Km',0):.0f} km off route · {net}"
                 f"</span></div>")
        folium.CircleMarker(
            location=[row["Latitude"], row["Longitude"]], radius=7,
            color=color, fill=True, fill_color=color, fill_opacity=0.85, weight=1.5,
            popup=folium.Popup(html, max_width=260), tooltip=f"{net}: {price}",
        ).add_to(m)
    return m


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


def _stale_banner(meta):
    for dk, fk, label in [
        ("petro_stale_days",  "latest_petro_file",  "Petro"),
        ("esso_stale_days",   "latest_esso_file",   "Esso"),
        ("irving_stale_days", "latest_irving_file", "Irving"),
    ]:
        days = meta.get(dk)
        fname = meta.get(fk, "")
        if days is not None and days >= 3 and fname:
            st.markdown(
                f"<div class='stale-warning'>⚠️ {label} prices are <b>{days} days old</b> "
                f"({Path(fname).name}) — upload a newer file to Google Drive.</div>",
                unsafe_allow_html=True,
            )


def _hl(col_type):
    def f(val):
        if pd.isna(val): return ""
        if col_type == "network":
            return {"Petro":"background-color:#fee2e2;color:#991b1b;font-weight:600","Esso":"background-color:#dbeafe;color:#1e40af;font-weight:600","Irving":"background-color:#dcfce7;color:#166534;font-weight:600"}.get(str(val),"")
        try:
            v = float(val)
        except (ValueError, TypeError):
            return ""
        if col_type == "savings":
            if v > 0: return "background-color:#d1fae5;color:#166534"
            if v < 0: return "background-color:#fee2e2;color:#991b1b"
        elif col_type == "composite":
            if v > 200: return "background-color:#d1fae5;color:#166534;font-weight:600"
            if v > 0:   return "background-color:#ecfdf5;color:#166534"
            if v < 0:   return "background-color:#fee2e2;color:#991b1b"
        elif col_type == "network":
            return {
                "Petro":  "background-color:#fee2e2;color:#991b1b;font-weight:600",
                "Esso":   "background-color:#dbeafe;color:#1e40af;font-weight:600",
                "Irving": "background-color:#dcfce7;color:#166534;font-weight:600",
            }.get(str(val), "")
        return ""
    return f


def main():
    _init_session()

    # Fix 1: GPS only after login — no white skeleton on login screen
    do_login()

    # ── Lazy GPS import ────────────────────────────────────────────────────
    # Only reached when the user is authenticated (do_login calls st.stop()
    # for unauthenticated users).  Importing here prevents the widget from
    # rendering any DOM element on the login page.
    gps_data = None
    try:
        from streamlit_geolocation import streamlit_geolocation as _geo
        gps_data = _geo()
    except Exception:
        pass

    with st.sidebar:
        st.markdown("## ⛽ OBYR Fuel")
        st.success(f"👤 {st.session_state.driver_name}")
        if st.button("Logout", use_container_width=True):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()
        st.divider()

        st.markdown("### 📍 Current Location")
        use_gps = st.checkbox("Use my GPS location", value=st.session_state.gps_acquired)
        if use_gps and gps_data and gps_data.get("latitude"):
            st.session_state.current_lat   = float(gps_data["latitude"])
            st.session_state.current_lon   = float(gps_data["longitude"])
            st.session_state.current_label = "GPS Location"
            st.session_state.gps_acquired  = True
            st.caption(f"GPS: {st.session_state.current_lat:.4f}, {st.session_state.current_lon:.4f}")

        if not use_gps:
            r = places_input(
                label="Current address",
                search_key="current_search",
                select_key="current_select",
                placeholder="e.g. 279 Belfield Rd, Etobicoke",
            )
            if r:
                st.session_state.current_lat   = r["lat"]
                st.session_state.current_lon   = r["lon"]
                st.session_state.current_label = r["address"]
                _cached_price_table.clear()
                _cached_polyline.clear()

        st.markdown("### 🏁 Destination")
        r = places_input(
            label="Destination",
            search_key="dest_search",
            select_key="dest_select",
            placeholder="e.g. Moosehead Breweries, Saint John NB",
        )
        if r:
            st.session_state.dest_lat   = r["lat"]
            st.session_state.dest_lon   = r["lon"]
            st.session_state.dest_label = r["address"]
            _cached_price_table.clear()
            _cached_polyline.clear()

        if st.session_state.dest_lat is not None:
            if st.button("✕ Clear destination", use_container_width=True):
                st.session_state.dest_lat   = None
                st.session_state.dest_lon   = None
                st.session_state.dest_label = ""
                _cached_price_table.clear()
                _cached_polyline.clear()
                st.rerun()

        st.markdown("### 🔧 Filters")
        network = st.radio("Network", ["All", "Petro", "Esso", "Irving"], index=0, horizontal=True)
        has_dest = st.session_state.dest_lat is not None
        if has_dest:
            buf     = st.slider("Max detour from route (km)", 25, 200, 75, 25)
            det     = st.slider("Detour cost $/km (truck)", 0.50, 4.00, 1.55, 0.05)
            max_km  = 5000
        else:
            max_km  = st.slider("Max km from current location", 50, 2000, 500, 50)
            buf     = 999
            det     = 1.55

        st.divider()
        if st.button("🔄 Refresh prices", use_container_width=True):
            _cached_price_table.clear()
            _cached_polyline.clear()
            st.rerun()

    clat  = st.session_state.current_lat
    clon  = st.session_state.current_lon
    clab  = st.session_state.current_label
    dlat  = st.session_state.dest_lat
    dlon  = st.session_state.dest_lon
    dlab  = st.session_state.dest_label or "None"

    prices_df, meta = _cached_price_table(clat, clon, dlat, dlon, network, max_km, buf, det)

    # Fix 3: one polyline fetch, reused everywhere
    polyline_pts  = None
    route_dist_km = 0.0
    if has_dest and MAPS_API_KEY:
        polyline_pts, route_dist_km = _cached_polyline(clat, clon, dlat, dlon)

    rm = meta.get("routing_mode", "none")
    if rm == "google_directions":
        badge = f"<span class='route-badge route-google'>🛣️ Google Route ({route_dist_km:.0f} km)</span>"
    elif rm == "straight_line_fallback":
        badge = "<span class='route-badge route-fallback'>⚠️ Straight-line fallback</span>"
    elif rm == "radius":
        badge = f"<span class='route-badge route-radius'>📡 Radius {max_km} km</span>"
    else:
        badge = ""

    hcol1, hcol2 = st.columns([3, 1])
    with hcol1:
        st.markdown("## ⛽ OBYR Fuel — Triple Network")
        st.markdown(
            f" · From: **{clab}** · To: **{dlab}** "
            f"{'· Corridor 🛣️' if has_dest else '· Radius 📡'} "
            f"· Network: **{network}** {badge}",
            unsafe_allow_html=True,
        )
    with hcol2:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=160)

    _stale_banner(meta)

    top = prices_df.iloc[0] if not prices_df.empty else None
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("National avg (all-in)", f"${meta['avg_all_in']:.3f}/L" if meta["avg_all_in"] else "—")
    c2.metric("Best stop (all-in)",
              f"${top['All_In_Price']:.3f}/L" if top is not None else "—",
              delta=f"${top['All_In_Price'] - meta['avg_all_in']:.3f} vs avg" if top is not None else None,
              delta_color="inverse")
    c3.metric("Saves / 1,000 L", f"${top['Savings_per_1000L']:,.0f}" if top is not None else "—")
    c4.metric("Stations shown", f"{meta['display_rows']}")
    st.divider()

    if prices_df.empty:
        st.warning("No stations found. Try widening corridor buffer, increasing radius, or changing network.")
        return

    tab1, tab2, tab3 = st.tabs(["📋 Ranked Table", "🗺️ Map", "🔧 Data Status"])

    with tab1:
        cols = ["Station_Name","Province","Network","Address",
                "Km_from_Current","Km_from_Destination","Km_from_Yard","All_In_Price","Savings_per_1000L"]
        rmap = {"Station_Name":"Station","Km_from_Current":"Km (Current)",
                "Km_from_Destination":"Km (Dest)","Km_from_Yard":"Km (Yard)",
                "All_In_Price":"All-In $/L","Savings_per_1000L":"Saves / 1kL"}
        if has_dest and "Detour_Extra_Km" in prices_df.columns and "Composite_Score" in prices_df.columns:
            cols += ["Detour_Extra_Km","Composite_Score"]
            rmap["Detour_Extra_Km"]  = "Detour Km"
            rmap["Composite_Score"]  = "Net Value $"

        ddf = prices_df[cols].copy().head(75).rename(columns=rmap)
        fmt = {"Km (Current)":"{:.0f}","Km (Dest)":"{:.0f}","Km (Yard)":"{:.0f}",
               "All-In $/L":"${:.3f}","Saves / 1kL":"${:,.0f}"}
        if has_dest:
            fmt["Detour Km"]  = "{:.0f}"
            fmt["Net Value $"] = "${:,.0f}"

        styled = ddf.style.format(fmt).map(_hl("savings"), subset=["Saves / 1kL"]).map(_hl("network"), subset=["Network"])
        if has_dest and "Net Value $" in ddf.columns:
            styled = styled.map(_hl("composite"), subset=["Net Value $"])

        st.dataframe(styled, use_container_width=True, hide_index=True)
        st.download_button("⬇️ Download CSV", prices_df.to_csv(index=False),
                           file_name=f"obyr_fuel_{datetime.now().strftime('%Y-%m-%d')}.csv", mime="text/csv")
        if has_dest:
            mt = "Google highway routing" if rm == "google_directions" else "straight-line estimate"
            st.info(f"💡 **Net Value $** = savings per 1,000 L minus detour cost ({mt}).")

    with tab2:
        if not MAP_AVAILABLE:
            st.warning("Map requires folium and streamlit-folium.")
        else:
            mdf = prices_df.dropna(subset=["Latitude","Longitude"])
            if mdf.empty:
                st.warning("No stations with coordinates.")
            else:
                m = _build_map(mdf, clat, clon, dlat, dlon, polyline=polyline_pts)
                st_folium(m, use_container_width=True, height=560, returned_objects=[])
                st.caption("🔴 Petro-Canada &nbsp; 🔵 Esso &nbsp; 🟢 Irving &nbsp; 📍 Current &nbsp; 🏁 Destination &nbsp; 🔵 line = actual highway")

    with tab3:
        s1, s2, s3 = st.columns(3)
        for col, ok_key, src_key, file_key, lbl in [
            (s1,"latest_petro_file","petro_source","latest_petro_file","Petro"),
            (s2,"latest_esso_file","esso_source","latest_esso_file","Esso"),
            (s3,"latest_irving_file","irving_source","latest_irving_file","Irving"),
        ]:
            ok = bool(meta.get(ok_key))
            col.markdown(f"**{lbl}** \n{'✅' if ok else '⚠️'} {meta.get(src_key) or 'not found'}\n`{Path(meta[file_key]).name if ok else 'N/A'}`")

        st.markdown("**Routing**")
        if MAPS_API_KEY:
            st.success("✅ Google Directions API + Places API active")
        else:
            st.warning("⚠️ No API key — Nominatim fallback active")

        st.dataframe(pd.DataFrame([
            {"Network":"Petro",  "Source rows":meta.get("petro_source_rows",0),  "Matched":meta.get("petro_matched_rows",0),  "Stale (days)":meta.get("petro_stale_days","?")},
            {"Network":"Esso",   "Source rows":meta.get("esso_source_rows",0),   "Matched":meta.get("esso_matched_rows",0),   "Stale (days)":meta.get("esso_stale_days","?")},
            {"Network":"Irving", "Source rows":meta.get("irving_source_rows",0), "Matched":meta.get("irving_matched_rows",0), "Stale (days)":meta.get("irving_stale_days","?")},
        ]), hide_index=True, use_container_width=True)

        unmatched = prices_df[~prices_df["Matched"]][["Station_Name","Province","Network","Address"]].copy()
        if not unmatched.empty:
            with st.expander(f"⚠️ {len(unmatched)} unmatched stations"):
                st.dataframe(unmatched, hide_index=True, use_container_width=True)

    st.markdown(
        f"<div class='footer'>© {datetime.now().year} OBYR Transportation Group Ltd. · OBYR Fuel</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
