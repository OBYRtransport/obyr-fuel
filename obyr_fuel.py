"""
OBYR Fuel — Streamlit UI

V7.5 (Debug fixes):
  1. Sidebar toggle fixed — login-page CSS no longer hides the global
     stExpandSidebarButton / collapsedControl so the toggle works after
     login. Sidebar is only visually hidden pre-login.
  2. Map & searchbox rendering fixed — the GPS iframe hide selector was
     nuking ALL custom components (folium map, streamlit-searchbox).
     Now scoped to a unique wrapper so only the GPS widget is hidden.
  3. Theme — locked to Streamlit light theme via .streamlit/config.toml.
     Custom CSS only layers on top (sidebar, header, cards, badges);
     Streamlit's own components use its native theme for legibility.
  4. Directions fixed — dropped overly aggressive avoid=ferries|tolls
     that was failing routes into PEI and on toll-road corridors; kept
     Canadian waypoint biasing so routes stay in Canada.
  5. Fresh prices on every session start — st.cache_data.clear() now
     fires once per user session BEFORE any data is fetched, not only on
     explicit login, so refreshes and stale sessions always pull live
     Drive data.
  6. Analytics: Drive log writes now pass supportsAllDrives=True so the
     usage_log.csv survives on shared drives too.
  7. Modernised visuals — gradient header, card-style metrics, softer
     shadows, better type scale.
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
    get_driver_full_name,
    get_driver_role,
    build_price_table,
    get_base_dir,
    get_route_polyline,
    price_staleness_days,
    log_event,
    read_analytics,
)

try:
    from geotab_engine import get_fleet_snapshot, fuel_window, TANK_CAPACITY_L
    GEOTAB_AVAILABLE = True
except Exception:
    GEOTAB_AVAILABLE = False

try:
    import folium
    from streamlit_folium import st_folium
    MAP_AVAILABLE = True
except ImportError:
    MAP_AVAILABLE = False

# streamlit_geolocation imported lazily in main() after login gate only.

st.set_page_config(
    page_title="OBYR Fuel", page_icon="⛽",
    layout="wide", initial_sidebar_state="expanded",
)

BASE_DIR     = get_base_dir()

# ── Fresh-data guarantee ───────────────────────────────────────────────────
# Clear all cached data exactly once per Streamlit session. This runs BEFORE
# any price-table or Drive fetch, so every time the site is opened (new
# login, refreshed tab, previous logout, whatever) the very first fetch
# goes straight to Google Drive and pulls the newest price files.
if not st.session_state.get("_session_cache_cleared", False):
    try:
        st.cache_data.clear()
    except Exception:
        pass
    st.session_state["_session_cache_cleared"] = True

# On the login page only, hide the sidebar *body* but leave the toggle
# buttons alone so the toggle keeps working after login.
if not st.session_state.get("logged_in", False):
    st.markdown(
        "<style>"
        "[data-testid='stSidebar']{display:none!important;}"
        "</style>",
        unsafe_allow_html=True,
    )
LOGO_PATH    = BASE_DIR / "obyr_logo.png"
MAPS_API_KEY = os.getenv("GOOGLE_DIRECTIONS_API_KEY", "").strip()

PLACES_AUTOCOMPLETE_URL = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
PLACES_DETAILS_URL      = "https://maps.googleapis.com/maps/api/place/details/json"

def _inject_theme_css(mode: str = "system"):
    """
    Inject cosmetic CSS that layers on top of Streamlit's built-in light
    theme (set via .streamlit/config.toml). Only styles elements we
    directly control — sidebar, header banner, metric cards, warnings,
    badges, and the login heading. Streamlit's own components (dataframes,
    tabs, inputs) are left to its native theme engine for legibility.
    """
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=DM+Mono:wght@400;500&display=swap');

/* ── Base font only — let Streamlit handle bg & text colours ───────── */
html, body, [class*="css"], .stApp {
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
}

/* ── Sidebar — dark navy ───────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #0f172a !important;
    color: #e2e8f0 !important;
    border-right: 1px solid #1e293b;
}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] .stRadio label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div {
    color: #e2e8f0 !important;
}
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] h4 {
    color: #f8fafc !important;
    font-size: 0.8rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-weight: 700;
}
/* Sidebar buttons (Logout, Refresh Prices) — visible on dark bg */
[data-testid="stSidebar"] button {
    color: #e2e8f0 !important;
    border: 1px solid #334155 !important;
    background: rgba(255,255,255,0.05) !important;
}
[data-testid="stSidebar"] button:hover {
    background: rgba(255,255,255,0.15) !important;
    border-color: #60a5fa !important;
    color: #ffffff !important;
}

/* ── Metrics — card style ──────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    padding: 1.1rem 1.3rem;
    box-shadow: 0 1px 3px rgba(15,23,42,0.06), 0 1px 2px rgba(15,23,42,0.04);
    transition: box-shadow 0.2s ease, transform 0.2s ease;
}
[data-testid="metric-container"]:hover {
    box-shadow: 0 4px 12px rgba(15,23,42,0.08), 0 2px 4px rgba(15,23,42,0.04);
}
[data-testid="metric-container"] label {
    color: #64748b !important;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 600;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Mono', monospace;
    font-size: 1.6rem;
    color: #0f172a;
    font-weight: 600;
}

/* ── Header banner ─────────────────────────────────────────────────── */
.header-banner {
    background: linear-gradient(135deg, #1d4ed8 0%, #2563eb 100%);
    color: #ffffff;
    padding: 1.4rem 1.8rem;
    border-radius: 16px;
    margin-bottom: 1.2rem;
    box-shadow: 0 4px 14px rgba(37, 99, 235, 0.25);
}
.header-banner h1 {
    color: #ffffff !important;
    margin: 0;
    font-size: 1.75rem;
    font-weight: 800;
    letter-spacing: -0.02em;
}
.header-banner .sub {
    color: rgba(255,255,255,0.9);
    font-size: 0.92rem;
    margin-top: 0.35rem;
}

/* ── Tables & tabs ─────────────────────────────────────────────────── */
.stDataFrame, [data-testid="stTable"] {
    border-radius: 12px;
    overflow: hidden;
}
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 10px 10px 0 0;
    padding: 0.55rem 1rem;
    font-weight: 600;
}

/* ── Warnings & badges ─────────────────────────────────────────────── */
.stale-warning {
    background: #fef3c7;
    border-left: 4px solid #f59e0b;
    border-radius: 8px;
    padding: 0.7rem 1.1rem;
    font-size: 0.88rem;
    color: #92400e;
    margin-bottom: 0.6rem;
}
.route-badge {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-left: 8px;
}
.route-google   { background: #dcfce7; color: #166534; }
.route-fallback { background: #fef9c3; color: #854d0e; }
.route-radius   { background: #dbeafe; color: #1e40af; }

.login-heading  { font-size: 1.5rem; font-weight: 700; margin-bottom: 1rem; }
.footer {
    font-size: 0.72rem;
    color: #64748b;
    text-align: center;
    padding: 1.5rem 0 0.5rem;
}

/* Hide Streamlit chrome — but KEEP the header visible so the sidebar
   expand/collapse toggle (which lives inside stHeader) stays usable. */
[data-testid="stSkeleton"] { display: none !important; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
[data-testid="stHeader"] {
    background: transparent !important;
    height: 3rem;
}
[data-testid="stHeader"] [data-testid="stDecoration"],
[data-testid="stHeader"] [data-testid="stStatusWidget"],
[data-testid="stToolbar"] {
    visibility: hidden;
}
/* Make absolutely sure the sidebar collapse/expand controls are visible */
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"],
[data-testid="stExpandSidebarButton"],
[data-testid="stSidebarCollapseButton"] {
    visibility: visible !important;
    display: flex !important;
    opacity: 1 !important;
    z-index: 999999 !important;
}

/* ── Geolocation widget hide ───────────────────────────────────────
   Target the geolocation iframe by its title attribute. The wrapper
   div approach doesn't work because Streamlit renders each component
   as a sibling, not inside the markdown div. */
[data-testid="stSidebar"] iframe[title="streamlit_geolocation.streamlit_geolocation"] {
    display: none !important;
    height: 0 !important;
}
[data-testid="stSidebar"] iframe[title="streamlit_geolocation.streamlit_geolocation"] ~ * {
    /* no-op placeholder */
}
/* Also hide the stCustomComponentV1 wrapper that holds the geolocation widget */
[data-testid="stSidebar"] [data-testid="stCustomComponentV1"]:has(iframe[title="streamlit_geolocation.streamlit_geolocation"]) {
    display: none !important;
    height: 0 !important;
    overflow: hidden !important;
}
</style>
""", unsafe_allow_html=True)


# Apply the current theme immediately (before any UI is rendered). The
# actual radio control lives in the sidebar and triggers a rerun which
# re-injects this CSS with the new mode.
_inject_theme_css()


def _init_session():
    defaults = {
        "logged_in": False, "driver_name": "", "driver_full_name": "",
        "driver_role": "driver",
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


def _price_cache_window() -> str:
    """15-minute cache bust key — forces fresh Drive read every 15 min."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime
        now = datetime.now(ZoneInfo("America/Toronto"))
    except Exception:
        from datetime import datetime
        now = datetime.utcnow()
    window_minute = (now.minute // 15) * 15
    return f"{now.date()}-{now.hour:02d}-{window_minute:02d}"


@st.cache_data(ttl=900, show_spinner="Loading fuel prices…")
def _cached_price_table(clat, clon, dlat, dlon, network, max_km, buffer_km, detour_cost, price_window):
    # price_window changes every 15 min — forces fresh Drive read automatically.
    # Must NOT start with _ or Streamlit ignores it as a cache key.
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
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=280)
        st.markdown("<p class='login-heading'>Driver Login</p>", unsafe_allow_html=True)
        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("E-mail", key="login_user",
                                     placeholder="your@email.com")
            password = st.text_input("Password", type="password", key="login_pass")
            submitted = st.form_submit_button("Login", type="primary",
                                              use_container_width=True)
        if submitted:
            if not username or not password:
                st.error("Please enter your e-mail and password.")
            elif authenticate_driver(username, password):
                st.cache_data.clear()
                st.session_state.logged_in = True
                st.session_state.driver_name = str(username).strip()
                st.session_state.driver_full_name = get_driver_full_name(username)
                st.session_state.driver_role = get_driver_role(username)
                if st.session_state.driver_role != "admin":
                    log_event(
                        username=str(username).strip(),
                        full_name=st.session_state.driver_full_name,
                        event="login",
                    )
                st.rerun()
            else:
                time.sleep(0.6)
                st.error("Incorrect e-mail or password.")
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

    do_login()
    # Only authenticated users reach this point

    with st.sidebar:
        # GPS widget is wrapped in .gps-hidden so the CSS scoped selector
        # hides only this iframe — the folium map and searchbox stay visible.
        gps_data = None
        st.markdown("<div class='gps-hidden'>", unsafe_allow_html=True)
        try:
            from streamlit_geolocation import streamlit_geolocation
            gps_data = streamlit_geolocation()
        except Exception:
            pass
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("## ⛽ OBYR Fuel")
        full_name = st.session_state.get("driver_full_name", "")
        email     = st.session_state.driver_name
        if full_name:
            st.markdown(
                f"<div style='line-height:1.6;margin-bottom:0.5rem'>"
                f"<span style='font-weight:700;color:#f8fafc;font-size:0.95rem'>👤 {full_name}</span><br>"
                f"<span style='color:#94a3b8;font-size:0.78rem'>{email}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            st.success(f"👤 {email}")
        if st.button("Logout", use_container_width=True, key="btn_logout"):
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
            if st.button("✕ Clear destination", use_container_width=True, key="btn_clear_dest"):
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
        if st.button("🔄 Refresh prices", use_container_width=True, key="btn_refresh_prices"):
            _cached_price_table.clear()
            _cached_polyline.clear()
            st.rerun()

    clat  = st.session_state.current_lat
    clon  = st.session_state.current_lon
    clab  = st.session_state.current_label
    dlat  = st.session_state.dest_lat
    dlon  = st.session_state.dest_lon
    dlab  = st.session_state.dest_label or "None"

    prices_df, meta = _cached_price_table(clat, clon, dlat, dlon, network, max_km, buf, det, price_window=_price_cache_window())

    # Log a search once per unique combination (session-keyed so it doesn't
    # fire on every Streamlit rerun, only when the inputs actually change)
    _log_key = f"_logged_{clat}_{clon}_{dlat}_{dlon}_{network}"
    if _log_key not in st.session_state:
        st.session_state[_log_key] = True
        if st.session_state.get("driver_role") != "admin":
            log_event(
                username=st.session_state.driver_name,
                full_name=st.session_state.get("driver_full_name", ""),
                event="search",
                origin_label=clab,
                dest_label=dlab if dlat else "",
                network=network,
                route_km=meta.get("route_distance_km", 0.0),
            )

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

    st.markdown(
        f"""
        <div class='header-banner'>
            <h1>⛽ OBYR Fuel — Triple Network</h1>
            <div class='sub'>
                From: <b>{clab}</b> &nbsp;·&nbsp; To: <b>{dlab}</b>
                &nbsp;·&nbsp; {'Corridor 🛣️' if has_dest else 'Radius 📡'}
                &nbsp;·&nbsp; Network: <b>{network}</b> {badge}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

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

    is_admin = st.session_state.get("driver_role") == "admin"

    if is_admin:
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Ranked Table", "🗺️ Map", "🔧 Data Status", "🚛 Fleet", "📊 Analytics"])
    else:
        tab1, tab2, tab3 = st.tabs(["📋 Ranked Table", "🗺️ Map", "🔧 Data Status"])
        tab4 = None
        tab5 = None

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
        for col, ok_key, src_key, file_key, lbl, subfolder in [
            (s1,"latest_petro_file","petro_source","latest_petro_file","Petro","Petro/"),
            (s2,"latest_esso_file","esso_source","latest_esso_file","Esso","Esso/"),
            (s3,"latest_irving_file","irving_source","latest_irving_file","Irving","Irving/"),
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

    if tab4 is not None:
        with tab4:
            _render_fleet()

    if tab5 is not None:
        with tab5:
            _render_admin_analytics()

    st.markdown(
        f"<div class='footer'>© {datetime.now().year} OBYR Transportation Group Ltd. · OBYR Fuel</div>",
        unsafe_allow_html=True,
    )


def _render_fleet():
    """Admin-only Fleet tab — live map with truck icons, fuel levels, and smart stop recommendations."""

    st.markdown("## 🚛 Live Fleet — OBYR Highway")

    if not GEOTAB_AVAILABLE:
        st.error("geotab_engine.py not loaded. Ensure it is deployed alongside this file.")
        return

    creds_ok = bool(os.getenv("GEOTAB_USERNAME")) and bool(os.getenv("GEOTAB_PASSWORD"))
    if not creds_ok:
        st.error("Geotab credentials missing — add GEOTAB_USERNAME, GEOTAB_PASSWORD, GEOTAB_DATABASE in Render environment variables.")
        return

    # ── Refresh control — manual only, no automatic API calls on rerun ─────────
    from geotab_engine import _FLEET_CACHE, FLEET_CACHE_TTL
    import time as _time

    fetched_at   = _FLEET_CACHE.get("fetched_at", 0.0)
    age_secs     = _time.time() - fetched_at
    next_refresh = max(0, int(FLEET_CACHE_TTL - age_secs))
    last_str     = (datetime.fromtimestamp(fetched_at).strftime("%Y-%m-%d %H:%M:%S")
                    if fetched_at > 0 else "Not loaded yet")

    col_refresh, col_ts = st.columns([1, 3])
    with col_refresh:
        force = st.button("🔄 Refresh fleet data", use_container_width=True, key="btn_refresh_fleet")
    with col_ts:
        st.caption(
            f"Geotab last queried: **{last_str}** · "
            f"Next earliest refresh in: **{next_refresh}s** · "
            f"Data refreshes at most once every 5 minutes to protect the Geotab account."
        )

    with st.spinner("Loading fleet data…"):
        fleet = get_fleet_snapshot(force=force)

    if fleet.empty:
        st.warning("No fleet data returned. Geotab may not have matched any device names to your unit numbers (017, 019, 020, 024, 025, 027, 028). Check device names in MyGeotab.")
        return

    if "error" in fleet.columns:
        st.error(f"Geotab API error: {fleet['error'].iloc[0]}")
        st.info("Check that your Geotab credentials in Render are correct and that fuel@obyrtransport.com has read access.")
        return

    # ── KPI strip ─────────────────────────────────────────────────────────────
    total        = len(fleet)
    low_fuel     = int((fleet["status"] == "🔴 Low Fuel").sum())
    fuel_soon    = int((fleet["status"] == "⚠️ Fuel Soon").sum())
    ok           = int((fleet["status"] == "✅ OK").sum())
    no_data      = int((fleet["status"] == "❓ No data").sum())
    avg_fuel_pct = fleet["fuel_pct"].dropna().mean()

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Trucks tracked",  total)
    k2.metric("✅ OK",           ok)
    k3.metric("⚠️ Fuel Soon",   fuel_soon)
    k4.metric("🔴 Low Fuel",    low_fuel)
    k5.metric("Avg fuel level",  f"{avg_fuel_pct:.0f}%" if pd.notna(avg_fuel_pct) else "—")

    st.divider()

    # ── Live fleet map ────────────────────────────────────────────────────────
    st.markdown("#### 📍 Live positions")

    if not MAP_AVAILABLE:
        st.warning("Map requires folium and streamlit-folium.")
    else:
        mapped = fleet[fleet["lat"].notna() & fleet["lon"].notna()].copy()

        if mapped.empty:
            st.warning("No trucks have GPS coordinates yet — Geotab connected but no position data returned.")
        else:
            # Centre map on fleet centroid
            centre_lat = mapped["lat"].mean()
            centre_lon = mapped["lon"].mean()

            fleet_map = folium.Map(
                location=[centre_lat, centre_lon],
                zoom_start=5,
                tiles="CartoDB positron",
            )

            # Colour by fuel status
            STATUS_COLOUR = {
                "✅ OK":        "#16a34a",   # green
                "⚠️ Fuel Soon": "#f59e0b",   # amber
                "🔴 Low Fuel":  "#dc2626",   # red
                "❓ No data":   "#94a3b8",   # grey
            }

            for _, row in mapped.iterrows():
                colour  = STATUS_COLOUR.get(row["status"], "#94a3b8")
                fuel_pct  = f"{row['fuel_pct']:.0f}%" if pd.notna(row.get("fuel_pct")) else "—"
                fuel_l    = f"{row['fuel_litres']:.0f} L" if pd.notna(row.get("fuel_litres")) else "—"
                range_km  = f"{row['range_km']:.0f} km" if pd.notna(row.get("range_km")) else "—"
                speed     = f"{row['speed_kmh']:.0f} km/h" if pd.notna(row.get("speed_kmh")) else "—"

                # Fuel gauge bar (simple HTML)
                pct_val   = float(row["fuel_pct"]) if pd.notna(row.get("fuel_pct")) else 0
                bar_fill  = colour
                bar_html  = (
                    f"<div style='background:#e2e8f0;border-radius:4px;height:8px;width:100%;margin:4px 0'>"
                    f"<div style='background:{bar_fill};width:{pct_val}%;height:8px;border-radius:4px'></div>"
                    f"</div>"
                )

                popup_html = (
                    f"<div style='font-family:Inter,sans-serif;min-width:220px;padding:4px'>"
                    f"<b style='font-size:14px'>Unit {row['unit']} — {row['truck_name']}</b><br>"
                    f"<span style='color:#64748b;font-size:11px'>{row['vehicle']}</span><br>"
                    f"<span style='color:#64748b;font-size:11px'>{row['engine']} · {row['hp']} HP</span><br><br>"
                    f"<b>Driver:</b> {row['driver']}<br>"
                    f"<b>Status:</b> <span style='color:{colour};font-weight:700'>{row['status']}</span><br>"
                    f"{bar_html}"
                    f"<b>Fuel:</b> {fuel_pct} &nbsp;({fuel_l})<br>"
                    f"<b>Est. range:</b> {range_km}<br>"
                    f"<b>Economy:</b> {row['economy_l100km']:.1f} L/100km<br>"
                    f"<b>Speed:</b> {speed}<br>"
                    f"</div>"
                )

                # Truck icon — circle with unit number label
                folium.CircleMarker(
                    location=[float(row["lat"]), float(row["lon"])],
                    radius=14,
                    color=colour,
                    fill=True,
                    fill_color=colour,
                    fill_opacity=0.9,
                    weight=2.5,
                    popup=folium.Popup(popup_html, max_width=260),
                    tooltip=f"Unit {row['unit']} · {row['driver']} · Fuel: {fuel_pct} · {row['status']}",
                ).add_to(fleet_map)

                # Unit number label on top of circle
                folium.Marker(
                    location=[float(row["lat"]), float(row["lon"])],
                    icon=folium.DivIcon(
                        html=f"<div style='font-family:Inter,sans-serif;font-weight:700;font-size:10px;"
                             f"color:#fff;text-align:center;margin-top:-6px;text-shadow:0 1px 2px rgba(0,0,0,0.6)'>"
                             f"{row['unit']}</div>",
                        icon_size=(30, 20),
                        icon_anchor=(15, 10),
                    ),
                ).add_to(fleet_map)

            st_folium(fleet_map, use_container_width=True, height=520, returned_objects=[])
            st.caption(
                "🟢 OK &nbsp; 🟡 Fuel Soon (&lt;300 km range) &nbsp; 🔴 Low Fuel (&lt;150 km range) &nbsp; "
                "⚪ No data &nbsp; · Click any truck for details"
            )

    st.divider()

    # ── Fleet roster table ────────────────────────────────────────────────────
    st.markdown("#### Fleet roster")
    display_cols = ["status", "unit", "driver", "vehicle", "engine",
                    "fuel_pct", "fuel_litres", "range_km", "economy_l100km", "speed_kmh"]
    display_cols = [c for c in display_cols if c in fleet.columns]
    rename = {
        "status":         "Status",
        "unit":           "Unit",
        "driver":         "Driver",
        "vehicle":        "Vehicle",
        "engine":         "Engine",
        "fuel_pct":       "Fuel %",
        "fuel_litres":    "Fuel (L)",
        "range_km":       "Range km",
        "economy_l100km": "L/100km",
        "speed_kmh":      "Speed km/h",
    }
    fmt = {
        "Fuel %":    "{:.0f}",
        "Fuel (L)":  "{:.0f}",
        "Range km":  "{:.0f}",
        "L/100km":   "{:.1f}",
        "Speed km/h":"{:.0f}",
    }
    roster_display = fleet[display_cols].rename(columns=rename)
    st.dataframe(
        roster_display.style.format(fmt, na_rep="—"),
        hide_index=True, use_container_width=True,
    )

    st.divider()

    # ── Per-truck fuel stop recommendation ───────────────────────────────────
    st.markdown("#### ⛽ Fuel stop recommendation")
    st.caption("Select a truck and destination to get a route-aware recommendation based on live fuel level.")

    has_gps  = fleet["lat"].notna() & fleet["lon"].notna()
    eligible = fleet[has_gps & fleet["fuel_litres"].notna()].copy()

    if eligible.empty:
        st.info("No trucks with live GPS and fuel data available right now.")
        return

    truck_options = {
        f"Unit {row['unit']} — {row['driver']}": row
        for _, row in eligible.iterrows()
    }

    selected_name = st.selectbox("Select truck", list(truck_options.keys()), key="fleet_truck_select")
    truck = truck_options[selected_name]

    dest_result = places_input(
        label="Destination",
        search_key="fleet_dest_search",
        select_key="fleet_dest_select",
        placeholder="e.g. Moosehead Breweries, Saint John NB",
    )

    if dest_result is None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Current fuel",  f"{truck['fuel_pct']:.0f}%  ({truck['fuel_litres']:.0f} L)")
        c2.metric("Est. range",    f"{truck['range_km']:.0f} km" if pd.notna(truck['range_km']) else "—")
        c3.metric("Economy",       f"{truck['economy_l100km']:.1f} L/100km")
        st.info("Enter a destination above to get a specific fuel stop recommendation for this truck.")
        return

    dlat = dest_result["lat"]
    dlon = dest_result["lon"]
    dlab = dest_result["address"]
    clat = float(truck["lat"])
    clon = float(truck["lon"])

    with st.spinner("Calculating optimal fuel window…"):
        api_key  = os.getenv("GOOGLE_DIRECTIONS_API_KEY", "").strip()
        route_km = 0.0
        if api_key:
            route_result = get_route_polyline(clat, clon, dlat, dlon, api_key)
            if route_result:
                _, route_km = route_result

        window = fuel_window(
            current_lat=clat, current_lon=clon,
            dest_lat=dlat, dest_lon=dlon,
            fuel_litres=float(truck["fuel_litres"]),
            economy_l100km=float(truck["economy_l100km"]),
            route_km=route_km,
        )

    st.markdown(f"**Unit {truck['unit']} — {truck['driver']}** → **{dlab}**")
    w1, w2, w3, w4 = st.columns(4)
    w1.metric("Route distance",  f"{route_km:.0f} km" if route_km else "—")
    w2.metric("Usable range",    f"{window['usable_range_km']:.0f} km")
    w3.metric("Must fuel by",    f"{window['must_fuel_by_km']:.0f} km")
    w4.metric("Will make it?",   "✅ Yes" if window["will_make_it"] else "⛽ Needs stop")

    if window["will_make_it"]:
        st.success("This truck can complete the full route on current fuel with reserve to spare.")
    else:
        st.warning(
            f"Must fuel before **{window['must_fuel_by_km']:.0f} km**. "
            f"Optimal stop window: **{window['optimal_from_km']:.0f} – {window['must_fuel_by_km']:.0f} km** along the route."
        )

    st.markdown("#### Recommended stops along this route")
    with st.spinner("Loading fuel prices…"):
        prices_df, meta = build_price_table(
            current_lat=clat, current_lon=clon,
            dest_lat=dlat, dest_lon=dlon,
            network_choice="All",
            corridor_buffer_km=75,
            detour_cost_per_km=1.55,
        )

    if prices_df.empty:
        st.warning("No stations found along this route.")
        return

    if not window["will_make_it"] and "Km_from_Current" in prices_df.columns:
        in_window = prices_df[
            (prices_df["Km_from_Current"] >= window["optimal_from_km"]) &
            (prices_df["Km_from_Current"] <= window["must_fuel_by_km"])
        ].copy()
        if in_window.empty:
            in_window = prices_df.copy()
    else:
        in_window = prices_df.copy()

    cols = ["Station_Name", "Province", "Network", "Address",
            "Km_from_Current", "All_In_Price", "Savings_per_1000L", "Detour_Extra_Km", "Composite_Score"]
    cols  = [c for c in cols if c in in_window.columns]
    rmap  = {
        "Station_Name":      "Station",
        "Km_from_Current":   "Km from Truck",
        "All_In_Price":      "All-In $/L",
        "Savings_per_1000L": "Saves / 1kL",
        "Detour_Extra_Km":   "Detour km",
        "Composite_Score":   "Net Value $",
    }
    fmt2 = {
        "Km from Truck": "{:.0f}",
        "All-In $/L":    "${:.3f}",
        "Saves / 1kL":   "${:,.0f}",
        "Detour km":     "{:.0f}",
        "Net Value $":   "${:,.0f}",
    }
    st.dataframe(
        in_window[cols].head(20).rename(columns=rmap).style.format(fmt2),
        hide_index=True, use_container_width=True,
    )


def _render_admin_analytics():
    """Full utilisation dashboard — only reachable when driver_role == 'admin'."""
    from datetime import timedelta

    st.markdown("## 📊 OBYR Fuel — Utilisation Dashboard")
    st.caption("Every login and search is recorded automatically. Refreshes on each page load.")

    log = read_analytics()

    if log.empty:
        st.info("No activity recorded yet. This dashboard will populate as drivers log in and run searches.")
        return

    # ── time window ──────────────────────────────────────────────────────────
    window = st.radio(
        "Time window",
        ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
        index=1, horizontal=True, key="adm_window",
    )
    days_back = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90, "All time": None}[window]
    if days_back:
        view = log[log["timestamp"] >= pd.Timestamp.now() - timedelta(days=days_back)].copy()
    else:
        view = log.copy()

    logins   = view[view["event"] == "login"]
    searches = view[view["event"] == "search"]

    # ── KPI strip ────────────────────────────────────────────────────────────
    st.divider()
    k1, k2, k3, k4, k5 = st.columns(5)
    unique_active = logins["username"].nunique()
    k1.metric("Logins",          len(logins))
    k2.metric("Searches",        len(searches))
    k3.metric("Unique drivers",  unique_active)
    k4.metric("Avg searches / driver", round(len(searches) / unique_active, 1) if unique_active else 0)
    most_active = (
        searches.groupby("full_name").size().idxmax()
        if not searches.empty else "—"
    )
    k5.metric("Most active",     most_active)
    st.divider()

    # ── Row 1: daily chart + searches per driver ──────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("#### Daily logins & searches")
        daily_l = logins.groupby("date").size().rename("Logins")
        daily_s = searches.groupby("date").size().rename("Searches")
        daily = pd.concat([daily_l, daily_s], axis=1).fillna(0).astype(int)
        daily.index = pd.to_datetime(daily.index)
        st.bar_chart(daily.sort_index())

    with col_b:
        st.markdown("#### Searches per driver")
        if searches.empty:
            st.caption("No searches yet.")
        else:
            by_driver = (
                searches.groupby("full_name").size()
                .reset_index(name="Searches").rename(columns={"full_name": "Driver"})
                .sort_values("Searches", ascending=False)
            )
            st.bar_chart(by_driver.set_index("Driver")["Searches"])

    # ── Row 2: hour-of-day + network preference ───────────────────────────────
    col_c, col_d = st.columns(2)
    with col_c:
        st.markdown("#### Hour of day (searches)")
        if "hour" in searches.columns and not searches.empty:
            hc = (
                searches["hour"].value_counts()
                .reindex(range(24), fill_value=0)
                .sort_index()
                .reset_index()
            )
            hc.columns = ["Hour", "Searches"]
            hc["Hour"] = hc["Hour"].apply(lambda h: f"{h:02d}:00")
            st.bar_chart(hc.set_index("Hour")["Searches"])
        else:
            st.caption("No data.")

    with col_d:
        st.markdown("#### Network preference")
        if not searches.empty and "network" in searches.columns:
            nc = (
                searches[searches["network"].astype(str).str.strip() != ""]
                .groupby("network").size()
                .reset_index(name="Searches").rename(columns={"network": "Network"})
                .sort_values("Searches", ascending=False)
            )
            if not nc.empty:
                st.bar_chart(nc.set_index("Network")["Searches"])
            else:
                st.caption("No network data.")
        else:
            st.caption("No data.")

    st.divider()

    # ── Driver roster ─────────────────────────────────────────────────────────
    st.markdown("#### Driver roster — activity summary")
    login_agg  = logins.groupby("username").agg(
        Logins=("event", "count"), Last_Login=("timestamp", "max")
    ).reset_index()
    search_agg = searches.groupby("username").agg(
        Searches=("event", "count")
    ).reset_index()

    # Pull all known drivers from master so you see who has NEVER logged in
    try:
        dm = read_driver_master()
        dm.columns = [c.strip() for c in dm.columns]
        roster = dm[dm.get("Role", pd.Series(["driver"] * len(dm))) != "admin"].copy()
        roster = roster.rename(columns={"Username": "username"})
        roster = (
            roster.merge(login_agg,  on="username", how="left")
                  .merge(search_agg, on="username", how="left")
        )
    except Exception:
        roster = login_agg.merge(search_agg, on="username", how="left")

    roster["Logins"]   = roster["Logins"].fillna(0).astype(int)
    roster["Searches"] = roster["Searches"].fillna(0).astype(int)
    roster["Last Login"] = roster["Last_Login"].apply(
        lambda t: pd.Timestamp(t).strftime("%Y-%m-%d %H:%M") if pd.notna(t) else "—  Never"
    ) if "Last_Login" in roster.columns else "—"
    roster["Status"] = roster.get("Last_Login", pd.Series([pd.NaT] * len(roster))).apply(
        lambda t: "✅ Active" if pd.notna(t) else "⚠️ Never logged in"
    )

    show_cols = [c for c in ["First Name", "Last Name", "username", "Logins", "Searches", "Last Login", "Status"] if c in roster.columns]
    st.dataframe(
        roster[show_cols].rename(columns={"username": "E-mail", "First Name": "First", "Last Name": "Last"})
                         .sort_values("Logins", ascending=False),
        hide_index=True, use_container_width=True,
    )

    st.divider()

    # ── Top routes ────────────────────────────────────────────────────────────
    st.markdown("#### Top searched routes")
    route_rows = searches[
        searches["dest_label"].astype(str).str.strip().isin(["", "None"]) == False
    ].copy()
    if route_rows.empty:
        st.caption("No corridor searches yet — all searches have been radius mode.")
    else:
        top_routes = (
            route_rows.groupby(["origin_label", "dest_label"])
            .agg(Count=("event", "count"), Avg_km=("route_km", "mean"))
            .reset_index()
            .rename(columns={"origin_label": "From", "dest_label": "To",
                              "Count": "Searches", "Avg_km": "Avg Route km"})
            .sort_values("Searches", ascending=False)
            .head(20)
        )
        top_routes["Avg Route km"] = top_routes["Avg Route km"].round(0).astype("Int64")
        st.dataframe(top_routes, hide_index=True, use_container_width=True)

    # ── Raw log ───────────────────────────────────────────────────────────────
    with st.expander("🔍 Full raw event log"):
        raw = (
            view[["timestamp", "full_name", "username", "event",
                  "origin_label", "dest_label", "network", "route_km"]]
            .rename(columns={"timestamp": "Time", "full_name": "Driver",
                              "username": "E-mail", "event": "Event",
                              "origin_label": "From", "dest_label": "To",
                              "network": "Network", "route_km": "Route km"})
            .sort_values("Time", ascending=False)
        )
        raw["Time"] = raw["Time"].dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(raw, hide_index=True, use_container_width=True)

    st.download_button(
        "⬇️ Export full log (CSV)",
        data=log.to_csv(index=False),
        file_name=f"obyr_usage_log_{datetime.now().strftime('%Y-%m-%d')}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()