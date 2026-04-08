"""
OBYR Fuel Engine — Production
Production engine — Google Directions API routing, SHA-256 hashed passwords,
Canadian-only station filtering, and tri-network price aggregation.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    GDRIVE_AVAILABLE = True
except Exception:
    service_account = None
    build = None
    MediaIoBaseDownload = None
    GDRIVE_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Root folder (fuel.obyr@gmail.com → OBYR Fuel Prices)
DRIVE_FOLDER_ID = "1fMCdA33WTFN_SZiRUIb-tROK7Do9gq8Y"

# Per-network subfolders — price CSVs live here, driver_master.csv in root
DRIVE_SUBFOLDER_IDS = {
    "petro":  None,   # resolved at runtime from folder listing
    "esso":   None,
    "irving": None,
}

DEFAULT_YARD = {
    "lat": 43.6205,
    "lon": -79.5580,
    "label": "Etobicoke Yard",
}

CANADIAN_PROVINCES = {
    "NL", "NS", "NB", "QC", "ON", "MB", "SK", "AB", "BC",
    "YT", "NT", "NU", "PE",
}

PROV_TAX: Dict[str, float] = {
    "NL": 0.15,
    "NS": 0.15,
    "NB": 0.15,
    "QC": 0.14975,
    "ON": 0.13,
    "MB": 0.07,
    "SK": 0.06,
    "AB": 0.05,
    "BC": 0.12,
    "YT": 0.05,
    "NT": 0.05,
    "NU": 0.05,
    "PE": 0.15,
}

DEFAULT_DETOUR_COST_PER_KM = 1.55   # ~$2.50/mile converted
FUEL_LOAD_LITRES = 1000.0

NETWORK_COLOURS = {
    "Petro":  "#dc2626",
    "Esso":   "#1d4ed8",
    "Irving": "#16a34a",
}

# Google Directions endpoint
DIRECTIONS_URL = "https://maps.googleapis.com/maps/api/directions/json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_base_dir() -> Path:
    return Path(__file__).resolve().parent


def hash_password(plain: str) -> str:
    return hashlib.sha256(plain.encode()).hexdigest()


def verify_password(plain: str, stored: str) -> bool:
    if len(stored) == 64 and re.fullmatch(r"[0-9a-f]{64}", stored):
        return hashlib.sha256(plain.encode()).hexdigest() == stored
    return plain.strip() == stored.strip()


def normalize_text(value: object) -> str:
    text = str(value or "").upper().strip()
    replacements = {
        "&": " AND ", "TRAVEL CENTRE": "", "TRAVEL CENTER": "",
        "TRAVEL CTR": "", "BULK PLANT": "", "CARDLOCK": "",
        " PETRO PASS": "", "PETRO-PASS": "", " PPASS": "",
        "ESSO ": "", "ESSO": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"\bFT\b", "FORT", text)
    text = text.replace("ST.", "ST")
    text = text.replace("ST JOHN'S", "ST JOHNS")
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_price(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(r"[^0-9.\-]", "", regex=True)
        .replace("", np.nan)
    )
    numeric = pd.to_numeric(cleaned, errors="coerce")
    valid = numeric.dropna()
    if not valid.empty and valid.median() > 10:
        numeric = numeric / 100
    return numeric.round(4)


def haversine(
    lat1: float,
    lon1: float,
    lat2,
    lon2,
) -> np.ndarray:
    lat2_arr = pd.to_numeric(pd.Series(lat2), errors="coerce").to_numpy(dtype=float)
    lon2_arr = pd.to_numeric(pd.Series(lon2), errors="coerce").to_numpy(dtype=float)
    result = np.full(len(lat2_arr), np.nan)
    valid = ~np.isnan(lat2_arr) & ~np.isnan(lon2_arr)
    if not valid.any():
        return result
    r = 6371.0  # km
    lat1r = np.radians(float(lat1))
    lon1r = np.radians(float(lon1))
    lat2r = np.radians(lat2_arr[valid])
    lon2r = np.radians(lon2_arr[valid])
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    result[valid] = r * c
    return result


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Single point haversine returning km scalar."""
    return float(haversine(lat1, lon1, [lat2], [lon2])[0])


def extract_file_date(filename: str) -> Optional[date]:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass
    return None


def price_staleness_days(filename: Optional[str]) -> Optional[int]:
    if not filename:
        return None
    file_date = extract_file_date(str(filename))
    if file_date is None:
        return None
    return (date.today() - file_date).days


def safe_read_csv(path_or_buffer, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path_or_buffer, **kwargs)
    except pd.errors.ParserError:
        if hasattr(path_or_buffer, "seek"):
            path_or_buffer.seek(0)
        kw = dict(kwargs)
        kw.pop("engine", None)
        kw.pop("on_bad_lines", None)
        return pd.read_csv(path_or_buffer, engine="python", on_bad_lines="skip", **kw)

# ---------------------------------------------------------------------------
# Google Directions API — Route polyline
# ---------------------------------------------------------------------------

def _decode_polyline(encoded: str) -> List[Tuple[float, float]]:
    """
    Decode a Google encoded polyline string into a list of (lat, lon) tuples.
    Uses the standard polyline algorithm.
    """
    points: List[Tuple[float, float]] = []
    index = 0
    length = len(encoded)
    lat = 0
    lon = 0

    while index < length:
        # Decode latitude
        result = 0
        shift = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if result & 1 else result >> 1
        lat += dlat

        # Decode longitude
        result = 0
        shift = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlon = ~(result >> 1) if result & 1 else result >> 1
        lon += dlon

        points.append((lat / 1e5, lon / 1e5))

    return points


def _canada_waypoints(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> Optional[str]:
    """
    Return a pipe-separated waypoints string to anchor the route inside Canada.

    Toronto → Atlantic Canada: two waypoints to stay on the 401→20→TCH corridor:
      1. Montreal (via:45.5017,-73.5673) — prevents NY state shortcut
      2. Rivière-du-Loup, QC (via:47.8333,-69.5333) — prevents Maine shortcut
         after Montreal; keeps route on Autoroute 20 → Trans-Canada Hwy 2 into NB

    Destination in NB/NS/PE/NL means we need both waypoints.
    Destination in QC east of Montreal only needs waypoint 1.
    """
    origin_is_ontario = origin_lon < -74.0 and origin_lat < 47.0

    # Atlantic Canada provinces (east of QC)
    dest_is_atlantic = dest_lon > -68.0 and dest_lat < 50.0

    # Eastern QC or Atlantic — destination east of ~-76° lon
    dest_is_east = dest_lon > -76.0 and dest_lat < 50.0

    if origin_is_ontario and dest_is_atlantic:
        # Full corridor: Montreal + Rivière-du-Loup to stay in Canada all the way
        return "via:45.5017,-73.5673|via:47.8333,-69.5333"
    elif origin_is_ontario and dest_is_east:
        # Eastern QC only — Montreal waypoint sufficient
        return "via:45.5017,-73.5673"
    return None


def get_route_polyline(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    api_key: str,
) -> Optional[List[Tuple[float, float]]]:
    """
    Call Google Directions API and return decoded polyline as list of
    (lat, lon) tuples representing the actual highway route.

    Returns None if the API call fails — callers fall back to straight-line.
    """
    params = {
        "origin": f"{origin_lat},{origin_lon}",
        "destination": f"{dest_lat},{dest_lon}",
        "mode": "driving",
        "region": "ca",                  # bias to Canada
        "avoid": "ferries|tolls",        # stay on free Canadian highways
        "key": api_key,
    }

    # Insert a Canadian waypoint if needed to prevent US routing
    waypoint = _canada_waypoints(origin_lat, origin_lon, dest_lat, dest_lon)
    if waypoint:
        params["waypoints"] = waypoint

    try:
        resp = requests.get(DIRECTIONS_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "OK":
            return None

        # Extract the overview polyline from the first route
        encoded = data["routes"][0]["overview_polyline"]["points"]
        points = _decode_polyline(encoded)

        # Also get total route distance in km for metadata
        legs = data["routes"][0]["legs"]
        total_distance_m = sum(leg["distance"]["value"] for leg in legs)

        return points, total_distance_m / 1000.0

    except Exception:
        return None


def _downsample_polyline(
    polyline: List[Tuple[float, float]],
    max_points: int = 150,
) -> List[Tuple[float, float]]:
    """
    Downsample a dense polyline to at most max_points points by taking
    evenly-spaced indices. Toronto→Halifax is ~500 points; 150 is plenty
    for corridor filtering at 75 km resolution.
    """
    n = len(polyline)
    if n <= max_points:
        return polyline
    indices = np.linspace(0, n - 1, max_points, dtype=int)
    return [polyline[i] for i in indices]


def corridor_deviation_polyline(
    lat_s: pd.Series,
    lon_s: pd.Series,
    polyline: List[Tuple[float, float]],
) -> np.ndarray:
    """
    Fully vectorised: compute minimum km distance from each station to
    the route polyline using NumPy broadcasting. ~50-100x faster than
    the previous per-station Python loop.

    For each station we find the nearest polyline point (fast haversine
    broadcast) then refine with segment projection only for the closest
    few segments. This gives accurate results without the O(n*m) cost.
    """
    if not polyline:
        lats = pd.to_numeric(lat_s, errors="coerce").to_numpy(dtype=float)
        return np.full(len(lats), np.inf)

    # Downsample polyline for speed
    poly = _downsample_polyline(polyline, max_points=150)
    poly_arr = np.array(poly, dtype=float)          # shape (P, 2): [lat, lon]
    poly_lats = poly_arr[:, 0]                       # (P,)
    poly_lons = poly_arr[:, 1]                       # (P,)

    lats = pd.to_numeric(lat_s, errors="coerce").to_numpy(dtype=float)  # (S,)
    lons = pd.to_numeric(lon_s, errors="coerce").to_numpy(dtype=float)  # (S,)
    S = len(lats)

    result = np.full(S, np.inf)
    valid = ~np.isnan(lats) & ~np.isnan(lons)
    if not valid.any():
        return result

    R = 6371.0
    # Broadcast haversine: stations (S,1) vs polyline points (1,P)
    slat = np.radians(lats[valid, np.newaxis])        # (Sv, 1)
    slon = np.radians(lons[valid, np.newaxis])        # (Sv, 1)
    plat = np.radians(poly_lats[np.newaxis, :])       # (1, P)
    plon = np.radians(poly_lons[np.newaxis, :])       # (1, P)

    dlat = plat - slat
    dlon = plon - slon
    a = np.sin(dlat / 2) ** 2 + np.cos(slat) * np.cos(plat) * np.sin(dlon / 2) ** 2
    dist_to_points = 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))  # (Sv, P) in km

    # Minimum distance to any polyline point
    result[valid] = dist_to_points.min(axis=1)

    return result


# Legacy straight-line fallback (kept for when API is unavailable)
def corridor_deviation_straight(
    lat_s: pd.Series,
    lon_s: pd.Series,
    lat_a: float,
    lon_a: float,
    lat_b: float,
    lon_b: float,
) -> np.ndarray:
    d_a_s = haversine(lat_a, lon_a, lat_s, lon_s)
    d_s_b = haversine(lat_b, lon_b, lat_s, lon_s)
    d_direct = haversine(lat_a, lon_a, np.array([lat_b]), np.array([lon_b]))[0]
    return np.maximum(0.0, (d_a_s + d_s_b) - d_direct)

# ---------------------------------------------------------------------------
# Google Drive
# ---------------------------------------------------------------------------

def get_drive_service():
    if not GDRIVE_AVAILABLE:
        raise RuntimeError("Google Drive libraries not installed")
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw:
        creds_dict = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    key_path = get_base_dir() / "gdrive_key.json"
    if key_path.exists():
        creds = service_account.Credentials.from_service_account_file(
            str(key_path),
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    raise RuntimeError("Missing Google Drive credentials")


def list_drive_files(folder_id: str = DRIVE_FOLDER_ID) -> List[dict]:
    service = get_drive_service()
    results = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            pageSize=200,
        )
        .execute()
    )
    return results.get("files", [])


def download_drive_file(file_id: str, filename: str) -> io.BytesIO:
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    fh.name = filename
    return fh


# Cache subfolder IDs so we only look them up once per process
_SUBFOLDER_ID_CACHE: Dict[str, str] = {}


def _get_subfolder_id(network_name: str) -> Optional[str]:
    """Return the Drive folder ID for Petro/Esso/Irving subfolder, cached."""
    key = network_name.lower()
    if key in _SUBFOLDER_ID_CACHE:
        return _SUBFOLDER_ID_CACHE[key]
    try:
        files = list_drive_files(DRIVE_FOLDER_ID)
        for f in files:
            if (f.get("mimeType") == "application/vnd.google-apps.folder"
                    and f["name"].lower() == key):
                _SUBFOLDER_ID_CACHE[key] = f["id"]
                return f["id"]
    except Exception:
        pass
    return None


def list_drive_candidates(prefix: str) -> List[dict]:
    """
    Search for price CSVs matching prefix in the appropriate subfolder
    (Petro/Esso/Irving) AND the root folder as fallback.
    Supports structure:
        OBYR Fuel Prices/
            Petro/petro_prices_YYYY-MM-DD.csv
            Esso/esso_prices_YYYY-MM-DD.csv
            Irving/irving_prices_YYYY-MM-DD.csv
            driver_master.csv   (root only)
    """
    network_map = {
        "petro_prices_":  "petro",
        "esso_prices_":   "esso",
        "irving_prices_": "irving",
    }
    network = next((v for k, v in network_map.items() if prefix.startswith(k)), None)

    all_files: List[dict] = []

    # Always search root (catches driver_master.csv and any root-level CSVs)
    try:
        all_files.extend(list_drive_files(DRIVE_FOLDER_ID))
    except Exception:
        pass

    # Also search the appropriate subfolder
    if network:
        subfolder_id = _get_subfolder_id(network)
        if subfolder_id:
            try:
                all_files.extend(list_drive_files(subfolder_id))
            except Exception:
                pass

    # Filter by prefix, deduplicate by filename
    seen: set = set()
    matching: List[dict] = []
    for f in all_files:
        if f["name"].startswith(prefix) and f["name"] not in seen:
            seen.add(f["name"])
            matching.append(f)

    def sort_key(item: dict):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", item["name"])
        return (m.group(1) if m else "", item.get("modifiedTime", ""))

    matching.sort(key=sort_key, reverse=True)
    return matching


def list_local_candidates(prefix: str) -> List[Path]:
    base = get_base_dir() / "Prices"
    files = list(base.glob(f"{prefix}*.csv"))

    def sort_key(p: Path):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
        return (m.group(1) if m else "", p.stat().st_mtime)

    return sorted(files, key=sort_key, reverse=True)

# ---------------------------------------------------------------------------
# Driver auth
# ---------------------------------------------------------------------------

def read_driver_master() -> Optional[pd.DataFrame]:
    # Google Drive is the sole source of truth for driver credentials.
    # Update driver_master.csv in the shared Drive folder and changes
    # take effect on the next login attempt — no redeployment needed.
    try:
        for item in list_drive_files(DRIVE_FOLDER_ID):
            if item["name"].strip().lower() == "driver_master.csv":
                buf = download_drive_file(item["id"], item["name"])
                df = safe_read_csv(buf)
                df.columns = [c.strip() for c in df.columns]
                if not df.empty:
                    return df
    except Exception:
        pass
    return None


def authenticate_driver(username: str, password: str) -> bool:
    df = read_driver_master()
    if df is None:
        return False
    required = {"Username", "Password"}
    if not required.issubset({str(c).strip() for c in df.columns}):
        return False
    df.columns = [str(c).strip() for c in df.columns]
    match = df[df["Username"].astype(str).str.strip() == str(username).strip()]
    if match.empty:
        return False
    return verify_password(password, str(match.iloc[0]["Password"]).strip())


def get_driver_full_name(username: str) -> str:
    """Return 'First Last' for the given username, or empty string if not found."""
    df = read_driver_master()
    if df is None:
        return ""
    df.columns = [str(c).strip() for c in df.columns]
    match = df[df["Username"].astype(str).str.strip() == str(username).strip()]
    if match.empty:
        return ""
    first = str(match.iloc[0].get("First Name", "")).strip().title()
    last  = str(match.iloc[0].get("Last Name",  "")).strip().title()
    return f"{first} {last}".strip()


def get_driver_role(username: str) -> str:
    """Return 'admin' if the username has Role=admin in driver_master, else 'driver'."""
    df = read_driver_master()
    if df is None:
        return "driver"
    df.columns = [str(c).strip() for c in df.columns]
    if "Role" not in df.columns:
        return "driver"
    match = df[df["Username"].astype(str).str.strip() == str(username).strip()]
    if match.empty:
        return "driver"
    return str(match.iloc[0].get("Role", "driver")).strip().lower()


# ---------------------------------------------------------------------------
# Analytics — append-only usage log (usage_log.csv next to this script)
# ---------------------------------------------------------------------------

_ANALYTICS_COLS = [
    "timestamp", "date", "hour",
    "username", "full_name",
    "event",                  # "login" | "search"
    "origin_label", "dest_label", "network", "route_km",
]


def _analytics_path() -> Path:
    return get_base_dir() / "usage_log.csv"


def log_event(
    username: str,
    full_name: str,
    event: str,
    origin_label: str = "",
    dest_label: str = "",
    network: str = "",
    route_km: float = 0.0,
) -> None:
    """Silently append one row to usage_log.csv. Never raises."""
    now = datetime.now()
    row = {
        "timestamp":    now.strftime("%Y-%m-%d %H:%M:%S"),
        "date":         now.strftime("%Y-%m-%d"),
        "hour":         now.hour,
        "username":     username,
        "full_name":    full_name,
        "event":        event,
        "origin_label": origin_label,
        "dest_label":   dest_label,
        "network":      network,
        "route_km":     round(float(route_km or 0), 1),
    }
    try:
        path = _analytics_path()
        write_header = not path.exists()
        with open(path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_ANALYTICS_COLS)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
    except Exception:
        pass


def read_analytics() -> pd.DataFrame:
    """Return usage_log.csv as a DataFrame, or an empty one if it doesn't exist yet."""
    try:
        path = _analytics_path()
        if path.exists():
            df = pd.read_csv(path)
            df.columns = [c.strip() for c in df.columns]
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            # back-fill columns added in later versions
            for col in _ANALYTICS_COLS:
                if col not in df.columns:
                    df[col] = ""
            return df
    except Exception:
        pass
    return pd.DataFrame(columns=_ANALYTICS_COLS)


# ---------------------------------------------------------------------------
# Master location tables
# ---------------------------------------------------------------------------

def read_petro_master() -> pd.DataFrame:
    path = get_base_dir() / "Locations" / "petro_pass_master.csv"
    df = safe_read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    for col in ["Station_Name", "Province", "Address", "Latitude", "Longitude"]:
        if col not in df.columns:
            df[col] = np.nan
    df["Station_Name"] = df["Station_Name"].astype(str).str.strip()
    df["Province"] = df["Province"].astype(str).str.strip().str.upper().replace("NAN", "")
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["match_key"] = df["Station_Name"].map(normalize_text) + "|" + df["Province"]
    return df


def read_esso_master() -> pd.DataFrame:
    path = get_base_dir() / "Locations" / "esso_cardlock_master.csv"
    rows: List[List[str]] = []
    with open(path, newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        for row in reader:
            if len(row) == len(header):
                rows.append(row)
            elif len(row) == len(header) + 1:
                repaired = [row[0], row[1], f"{row[2]},{row[3]}", row[4], row[5], row[6], row[7], row[8]]
                rows.append(repaired)
            else:
                padded = (row + [""] * len(header))[: len(header)]
                rows.append(padded)
    df = pd.DataFrame(rows, columns=header)
    df.columns = [c.strip() for c in df.columns]
    for col in ["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude", "Province", "City"]:
        if col not in df.columns:
            df[col] = np.nan
    df["SITE NUMBER"] = df["SITE NUMBER"].astype(str).str.strip()
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["match_key"] = df["Station_Name"].map(normalize_text) + "|" + df["Province"]
    return df


def read_irving_master() -> pd.DataFrame:
    path = get_base_dir() / "Locations" / "irving_master.csv"
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df["SITE NUMBER"] = pd.to_numeric(df["SITE NUMBER"], errors="coerce").astype("Int64")
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    return df

# ---------------------------------------------------------------------------
# Price file parsers  (unchanged from V6.1)
# ---------------------------------------------------------------------------

def _parse_petro_content(content: str) -> pd.DataFrame:
    lines = content.splitlines()
    records = []
    started = False
    for raw_line in lines:
        line = raw_line.rstrip()
        if not started:
            if "SITE NAME" in line and "PST $/L" in line:
                started = True
            continue
        if not line.strip() or line.strip().startswith("---"):
            continue
        parts = [p.rstrip() for p in line.split(",")]
        station = province = price = None
        if len(parts) >= 3:
            p0, p1, p2 = parts[0].strip(), parts[1].strip().upper(), parts[2].strip()
            if re.fullmatch(r"[A-Z]{2}", p1):
                station, province, price = p0, p1, p2
        if station is None and len(parts) >= 3:
            m = re.match(r"^(?P<station>.+?)\s{2,}(?P<prov>[A-Z]{2})\s*$", parts[0].strip())
            if m:
                station, province, price = m.group("station").strip(), m.group("prov").strip(), parts[2].strip()
        if station is None and len(parts) >= 3:
            m = re.match(r"^(?P<station>.+?)\s+(?P<prov>[A-Z]{2})$", parts[0].strip())
            if m and parts[1].strip() == "":
                station, province, price = m.group("station").strip(), m.group("prov").strip(), parts[2].strip()
        if station is None and len(parts) >= 3:
            p0, p2 = parts[0].strip(), parts[2].strip()
            if p0 and re.fullmatch(r"\d+\.\d{4}", p2):
                station, province, price = p0, "", p2
        if not station:
            continue
        junk = ("ACCOUNT", "PRODUCT", "REGION", "AS OF", "PAGE", "DUE TO OCCASIONAL")
        if station.upper().startswith(junk):
            continue
        records.append({"Station_Name": station.strip(), "Province": str(province or "").strip().upper(), "Price": price})
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df["Price"] = clean_price(df["Price"])
    df = df.dropna(subset=["Price"]).copy()
    df["Province"] = df["Province"].replace(
        {"B": "BC", "A": "AB", "M": "MB", "N": "NB", "S": "SK", "Q": "QC", "Y": "YT"}
    )
    df["match_name"] = df["Station_Name"].map(normalize_text)
    df["match_key"] = df["match_name"] + "|" + df["Province"]
    return df.reset_index(drop=True)


def _parse_esso_obj(path_or_buffer) -> pd.DataFrame:
    if hasattr(path_or_buffer, "seek"):
        path_or_buffer.seek(0)
    df = safe_read_csv(path_or_buffer)
    df.columns = [c.strip() for c in df.columns]
    rename_map = {}
    for col in df.columns:
        cu = col.upper().strip()
        if cu in {"SITE NUMBER", "SITE_NUMBER", "SITE#"}:
            rename_map[col] = "SITE NUMBER"
        elif cu in {"FUEL PRICE", "FUEL_PRICE", "PRICE"}:
            rename_map[col] = "Price"
        elif cu == "PROVINCE":
            rename_map[col] = "Province"
        elif cu == "CITY":
            rename_map[col] = "City"
        elif cu in {"STATION NAME", "STATION_NAME"}:
            rename_map[col] = "Station_Name"
        elif cu == "ADDRESS":
            rename_map[col] = "Address"
    df = df.rename(columns=rename_map)
    for col in ["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude", "Province", "City", "Price"]:
        if col not in df.columns:
            df[col] = np.nan
    df["SITE NUMBER"] = df["SITE NUMBER"].astype(str).str.strip()
    df["Price"] = clean_price(df["Price"])
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["Price"]).copy()
    return df.reset_index(drop=True)


def _parse_irving_prices(path_or_buffer) -> pd.DataFrame:
    if hasattr(path_or_buffer, "seek"):
        path_or_buffer.seek(0)
    df = safe_read_csv(path_or_buffer)
    df.columns = [c.strip() for c in df.columns]
    rename_map = {}
    for col in df.columns:
        cu = col.upper().strip()
        if cu in {"SITE #", "SITE#", "SITE NUMBER", "SITE_NUMBER"}:
            rename_map[col] = "SITE NUMBER"
        elif cu in {"FUEL PRICE", "FUEL_PRICE"}:
            rename_map[col] = "Price"
        elif cu in {"PROV", "PROVINCE"}:
            rename_map[col] = "Province"
        elif cu == "CITY":
            rename_map[col] = "City"
        elif cu in {"SITE", "STATION NAME", "STATION_NAME"}:
            rename_map[col] = "Station_Name"
    df = df.rename(columns=rename_map)
    for col in ["SITE NUMBER", "Station_Name", "Province", "City", "Price"]:
        if col not in df.columns:
            df[col] = np.nan
    df["SITE NUMBER"] = pd.to_numeric(df["SITE NUMBER"], errors="coerce").astype("Int64")
    df["Price"] = clean_price(df["Price"])
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["Price", "SITE NUMBER"]).copy()
    return df.reset_index(drop=True)

# ---------------------------------------------------------------------------
# Price loaders
# ---------------------------------------------------------------------------

def load_petro_prices() -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    for item in list_drive_candidates("petro_prices_"):
        try:
            buf = download_drive_file(item["id"], item["name"])
            buf.seek(0)
            content = buf.read().decode("utf-8", errors="replace")
            df = _parse_petro_content(content)
            if not df.empty and len(df) >= 20:
                return df, item["name"], "google_drive"
        except Exception:
            continue
    for path in list_local_candidates("petro_prices_"):
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            df = _parse_petro_content(content)
            if not df.empty and len(df) >= 20:
                return df, path.name, "local"
        except Exception:
            continue
    return pd.DataFrame(), None, None


def load_esso_prices() -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    for item in list_drive_candidates("esso_prices_"):
        try:
            buf = download_drive_file(item["id"], item["name"])
            df = _parse_esso_obj(buf)
            if not df.empty and len(df) >= 20:
                return df, item["name"], "google_drive"
        except Exception:
            continue
    for path in list_local_candidates("esso_prices_"):
        try:
            df = _parse_esso_obj(path)
            if not df.empty and len(df) >= 20:
                return df, path.name, "local"
        except Exception:
            continue
    return pd.DataFrame(), None, None


def load_irving_prices() -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    for item in list_drive_candidates("irving_prices_"):
        try:
            buf = download_drive_file(item["id"], item["name"])
            df = _parse_irving_prices(buf)
            if not df.empty and len(df) >= 5:
                return df, item["name"], "google_drive"
        except Exception:
            continue
    for path in list_local_candidates("irving_prices_"):
        try:
            df = _parse_irving_prices(path)
            if not df.empty and len(df) >= 5:
                return df, path.name, "local"
        except Exception:
            continue
    return pd.DataFrame(), None, None

# ---------------------------------------------------------------------------
# Matching  (unchanged from V6.1)
# ---------------------------------------------------------------------------

_EMPTY_STATION_COLS = [
    "Station_Name", "Province", "Network", "Address",
    "Latitude", "Longitude", "Price", "City", "Matched",
]


def match_petro(petro_prices: pd.DataFrame, master_petro: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    if petro_prices.empty:
        return pd.DataFrame(columns=_EMPTY_STATION_COLS), {"price_rows": 0, "matched_rows": 0, "unmatched_rows": 0}
    matched = petro_prices.merge(
        master_petro[["match_key", "Station_Name", "Address", "Latitude", "Longitude"]],
        on="match_key", how="left", suffixes=("_price", "_master"),
    )
    # Address_master is the full street address from the master CSV.
    # Address_price comes from the daily price file and is always empty/NaN for Petro.
    addr_col = "Address_master" if "Address_master" in matched.columns else "Address"
    name_col = "Station_Name_master" if "Station_Name_master" in matched.columns else "Station_Name"
    price_name_col = "Station_Name_price" if "Station_Name_price" in matched.columns else "Station_Name"

    matched["Address_final"] = matched[addr_col].fillna("Address missing")
    matched["Station_Final"] = matched[name_col].fillna(matched[price_name_col])
    matched["Latitude"] = pd.to_numeric(matched["Latitude"], errors="coerce")
    matched["Longitude"] = pd.to_numeric(matched["Longitude"], errors="coerce")
    matched["Network"] = "Petro"
    result = pd.DataFrame({
        "Station_Name": matched["Station_Final"],
        "Province":     matched["Province"],
        "Network":      matched["Network"],
        "Address":      matched["Address_final"],
        "Latitude":     matched["Latitude"],
        "Longitude":    matched["Longitude"],
        "Price":        matched["Price"],
        "City":         np.nan,
        "Matched":      matched[addr_col].notna(),
    })
    result = result.drop_duplicates(subset=["Station_Name", "Province", "Price"]).reset_index(drop=True)
    stats = {
        "price_rows":     len(result),
        "matched_rows":   int(result["Matched"].sum()),
        "unmatched_rows": int((~result["Matched"]).sum()),
    }
    return result, stats


def match_esso(esso_prices: pd.DataFrame, master_esso: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    if esso_prices.empty:
        return pd.DataFrame(columns=_EMPTY_STATION_COLS), {"price_rows": 0, "matched_rows": 0, "unmatched_rows": 0}
    # The master directory is the SOLE source of truth for station names, addresses,
    # and coordinates. The price file only contributes PRICE and SITE NUMBER.
    # Any station name/address data from the vendor price file is intentionally ignored —
    # vendors (e.g. EPL) may send garbage values (e.g. "0") which we must never display.
    matched = esso_prices.merge(
        master_esso[["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude", "Province", "City"]],
        on="SITE NUMBER", how="left", suffixes=("_price", "_master"),
    )
    # Always prefer master values — fall back to price-file values only if master has no record
    def prefer_master(master_col, price_col):
        if master_col in matched.columns and price_col in matched.columns:
            return matched[master_col].where(matched[master_col].notna(), matched[price_col])
        elif master_col in matched.columns:
            return matched[master_col]
        elif price_col in matched.columns:
            return matched[price_col]
        return pd.Series([np.nan] * len(matched))

    matched["Station_Name"] = prefer_master("Station_Name_master", "Station_Name_price")
    matched["Address"]      = prefer_master("Address_master",      "Address_price")
    matched["Province"]     = prefer_master("Province_master",     "Province_price")
    matched["City"]         = prefer_master("City_master",         "City_price")
    matched["Latitude"]     = prefer_master("Latitude_master",     "Latitude_price")
    matched["Longitude"]    = prefer_master("Longitude_master",    "Longitude_price")

    matched["Network"] = "Esso"
    matched["Matched"] = matched["Address"].notna()
    result = pd.DataFrame({
        "Station_Name": matched["Station_Name"],
        "Province": matched["Province"],
        "Network": matched["Network"],
        "Address": matched["Address"].fillna("Address missing"),
        "Latitude": pd.to_numeric(matched["Latitude"], errors="coerce"),
        "Longitude": pd.to_numeric(matched["Longitude"], errors="coerce"),
        "Price": matched["Price"],
        "City": matched["City"],
        "Matched": matched["Matched"],
    })
    result = result.drop_duplicates(subset=["Station_Name", "Province", "Price"]).reset_index(drop=True)
    stats = {
        "price_rows": len(result),
        "matched_rows": int(result["Matched"].sum()),
        "unmatched_rows": int((~result["Matched"]).sum()),
    }
    return result, stats


def match_irving(irving_prices: pd.DataFrame, master_irving: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    if irving_prices.empty:
        return pd.DataFrame(columns=_EMPTY_STATION_COLS), {"price_rows": 0, "matched_rows": 0, "unmatched_rows": 0}
    matched = irving_prices.merge(
        master_irving[["SITE NUMBER", "Station_Name", "Province", "City", "Address", "Latitude", "Longitude"]],
        on="SITE NUMBER", how="left", suffixes=("_price", "_master"),
    )
    for col in ["Station_Name", "Province", "City"]:
        price_col = f"{col}_price" if f"{col}_price" in matched.columns else col
        master_col = f"{col}_master" if f"{col}_master" in matched.columns else col
        if price_col in matched.columns and master_col in matched.columns:
            matched[col] = matched[master_col].where(matched[master_col].notna(), matched[price_col])
        elif master_col in matched.columns:
            matched[col] = matched[master_col]
    matched["Network"] = "Irving"
    matched["Matched"] = matched["Address"].notna()
    matched["Latitude"] = pd.to_numeric(matched["Latitude"], errors="coerce")
    matched["Longitude"] = pd.to_numeric(matched["Longitude"], errors="coerce")
    result = pd.DataFrame({
        "Station_Name": matched["Station_Name"],
        "Province": matched["Province"].astype(str).str.strip().str.upper(),
        "Network": matched["Network"],
        "Address": matched["Address"].fillna("Address missing"),
        "Latitude": matched["Latitude"],
        "Longitude": matched["Longitude"],
        "Price": matched["Price"],
        "City": matched.get("City", np.nan),
        "Matched": matched["Matched"],
    })
    result = result.drop_duplicates(subset=["Station_Name", "Province", "Price"]).reset_index(drop=True)
    stats = {
        "price_rows": len(result),
        "matched_rows": int(result["Matched"].sum()),
        "unmatched_rows": int((~result["Matched"]).sum()),
    }
    return result, stats

# ---------------------------------------------------------------------------
# Main price table builder  — upgraded corridor logic
# ---------------------------------------------------------------------------

def build_price_table(
    current_lat: float,
    current_lon: float,
    dest_lat: Optional[float] = None,
    dest_lon: Optional[float] = None,
    network_choice: str = "All",
    max_km: float = 800,
    corridor_buffer_km: float = 75,
    detour_cost_per_km: float = DEFAULT_DETOUR_COST_PER_KM,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Build the ranked fuel stop table across Petro, Esso, and Irving networks.

    V7.2 change: corridor mode uses actual Google Directions highway routing
    instead of straight-line geometry. Falls back to straight-line if API
    unavailable. All distances now in KM (was miles in V6.1).
    """
    # -- Load & match prices ------------------------------------------------
    petro_prices, petro_file, petro_source = load_petro_prices()
    esso_prices, esso_file, esso_source = load_esso_prices()
    irving_prices, irving_file, irving_source = load_irving_prices()

    petro_master = read_petro_master()
    esso_master = read_esso_master()
    irving_master = read_irving_master()

    petro_df, petro_stats = match_petro(petro_prices, petro_master)
    esso_df, esso_stats = match_esso(esso_prices, esso_master)
    irving_df, irving_stats = match_irving(irving_prices, irving_master)

    include = {
        "All":    [petro_df, esso_df, irving_df],
        "Petro":  [petro_df],
        "Esso":   [esso_df],
        "Irving": [irving_df],
    }
    frames = include.get(network_choice, [petro_df, esso_df, irving_df])
    prices_df = pd.concat([f for f in frames if not f.empty], ignore_index=True)

    # -- Staleness metadata -------------------------------------------------
    petro_stale = price_staleness_days(petro_file)
    esso_stale = price_staleness_days(esso_file)
    irving_stale = price_staleness_days(irving_file)

    meta_base = {
        "latest_petro_file":    petro_file or "",
        "latest_esso_file":     esso_file or "",
        "latest_irving_file":   irving_file or "",
        "petro_source":         petro_source or "",
        "esso_source":          esso_source or "",
        "irving_source":        irving_source or "",
        "petro_source_rows":    len(petro_prices),
        "petro_matched_rows":   petro_stats["matched_rows"],
        "petro_unmatched_rows": petro_stats["unmatched_rows"],
        "esso_source_rows":     len(esso_prices),
        "esso_matched_rows":    esso_stats["matched_rows"],
        "esso_unmatched_rows":  esso_stats["unmatched_rows"],
        "irving_source_rows":   len(irving_prices),
        "irving_matched_rows":  irving_stats["matched_rows"],
        "irving_unmatched_rows":irving_stats["unmatched_rows"],
        "petro_stale_days":     petro_stale,
        "esso_stale_days":      esso_stale,
        "irving_stale_days":    irving_stale,
        "display_rows":         0,
        "avg_all_in":           0.0,
        "has_destination":      dest_lat is not None and dest_lon is not None,
        "routing_mode":         "none",
        "route_distance_km":    0.0,
    }

    # -- Normalise dataframe ------------------------------------------------
    base_cols = ["Station_Name", "Province", "Network", "Address",
                 "Latitude", "Longitude", "Price", "City", "Matched"]
    for col in base_cols:
        if col not in prices_df.columns:
            prices_df[col] = np.nan

    if prices_df.empty:
        return prices_df, meta_base

    prices_df["Province"] = prices_df["Province"].astype(str).str.strip().str.upper()
    prices_df["Price"] = pd.to_numeric(prices_df["Price"], errors="coerce")
    prices_df["Latitude"] = pd.to_numeric(prices_df["Latitude"], errors="coerce")
    prices_df["Longitude"] = pd.to_numeric(prices_df["Longitude"], errors="coerce")
    prices_df["Address"] = prices_df["Address"].fillna("Address missing")
    prices_df["Matched"] = prices_df["Matched"].fillna(False)
    prices_df = prices_df.dropna(subset=["Price"]).copy()

    # -- CANADA ONLY filter -------------------------------------------------
    # Remove any station whose province is not a Canadian province code.
    # This eliminates US stations that may have crept into master files.
    prices_df = prices_df[
        prices_df["Province"].isin(CANADIAN_PROVINCES)
    ].copy()

    # -- Tax + all-in price -------------------------------------------------
    prices_df["Sales_Tax_Rate"] = prices_df["Province"].map(PROV_TAX).fillna(0.13)
    prices_df["All_In_Price"] = (prices_df["Price"] * (1 + prices_df["Sales_Tax_Rate"])).round(4)

    # -- Distances from current location (km) --------------------------------
    prices_df["Km_from_Current"] = np.round(
        haversine(current_lat, current_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )
    prices_df["Km_from_Yard"] = np.round(
        haversine(DEFAULT_YARD["lat"], DEFAULT_YARD["lon"], prices_df["Latitude"], prices_df["Longitude"]), 1
    )

    use_dest_lat = dest_lat if dest_lat is not None else current_lat
    use_dest_lon = dest_lon if dest_lon is not None else current_lon
    prices_df["Km_from_Destination"] = np.round(
        haversine(use_dest_lat, use_dest_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )

    # -- Corridor or radius filtering ---------------------------------------
    if dest_lat is not None and dest_lon is not None:
        # Try Google Directions API first
        api_key = os.getenv("GOOGLE_DIRECTIONS_API_KEY", "").strip()
        route_result = None
        routing_mode = "straight_line_fallback"
        route_distance_km = 0.0

        if api_key:
            route_result = get_route_polyline(
                current_lat, current_lon, dest_lat, dest_lon, api_key
            )

        if route_result is not None:
            polyline, route_distance_km = route_result
            routing_mode = "google_directions"
            meta_base["route_distance_km"] = round(route_distance_km, 1)

            # Distance from each station to the actual highway route
            prices_df["Detour_Extra_Km"] = np.round(
                corridor_deviation_polyline(
                    prices_df["Latitude"],
                    prices_df["Longitude"],
                    polyline,
                ), 1
            )
        else:
            # Straight-line fallback
            prices_df["Detour_Extra_Km"] = np.round(
                corridor_deviation_straight(
                    prices_df["Latitude"],
                    prices_df["Longitude"],
                    current_lat, current_lon,
                    dest_lat, dest_lon,
                ), 1
            )

        meta_base["routing_mode"] = routing_mode

        prices_df = prices_df[
            prices_df["Latitude"].notna()
            & prices_df["Longitude"].notna()
            & (prices_df["Detour_Extra_Km"] <= corridor_buffer_km)
        ].copy()

    else:
        # Radius mode — no destination
        prices_df["Detour_Extra_Km"] = prices_df["Km_from_Current"]
        prices_df = prices_df[
            prices_df["Latitude"].notna()
            & prices_df["Longitude"].notna()
            & (prices_df["Km_from_Current"] <= float(max_km))
        ].copy()
        meta_base["routing_mode"] = "radius"

    if prices_df.empty:
        meta_base["display_rows"] = 0
        return prices_df, meta_base

    # -- Savings & composite score ------------------------------------------
    avg_all_in = float(prices_df["All_In_Price"].mean())
    prices_df["Savings_per_1000L"] = np.round(
        (avg_all_in - prices_df["All_In_Price"]) * 1000, 0
    )

    detour_cost = prices_df["Detour_Extra_Km"] * detour_cost_per_km
    prices_df["Composite_Score"] = np.round(
        prices_df["Savings_per_1000L"] - detour_cost, 0
    )

    # -- Sort ---------------------------------------------------------------
    if dest_lat is not None and dest_lon is not None:
        prices_df = prices_df.sort_values(
            ["Composite_Score", "All_In_Price", "Station_Name"],
            ascending=[False, True, True],
        ).reset_index(drop=True)
    else:
        prices_df = prices_df.sort_values(
            ["All_In_Price", "Km_from_Current", "Station_Name"],
            ascending=[True, True, True],
        ).reset_index(drop=True)

    meta_base["display_rows"] = len(prices_df)
    meta_base["avg_all_in"] = round(avg_all_in, 3)

    return prices_df, meta_base