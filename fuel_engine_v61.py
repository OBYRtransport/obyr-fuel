"""
OBYR Fuel Engine V6.1
Adds full Irving Oil network support alongside Petro-Canada and Esso.
Irving matching uses Site # (integer join) — 100% reliable, no fuzzy text needed.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

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

DRIVE_FOLDER_ID = "18Cqpj-pVLDk5Esx2r3Cj_IR6Bd7lubCT"

DEFAULT_YARD = {
    "lat": 43.6205,
    "lon": -79.5580,
    "label": "Etobicoke Yard",
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

DEFAULT_DETOUR_COST_PER_MILE = 2.50
FUEL_LOAD_LITRES = 1000.0

# Network display colours (used by UI)
NETWORK_COLOURS = {
    "Petro":  "#dc2626",   # red
    "Esso":   "#1d4ed8",   # blue
    "Irving": "#16a34a",   # green
}

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
        "&": " AND ",
        "TRAVEL CENTRE": "",
        "TRAVEL CENTER": "",
        "TRAVEL CTR": "",
        "BULK PLANT": "",
        "CARDLOCK": "",
        " PETRO PASS": "",
        "PETRO-PASS": "",
        " PPASS": "",
        "ESSO ": "",
        "ESSO": "",
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
    r = 3958.8
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


def corridor_deviation(
    lat_s: pd.Series,
    lon_s: pd.Series,
    lat_a: float, lon_a: float,
    lat_b: float, lon_b: float,
) -> np.ndarray:
    d_a_s = haversine(lat_a, lon_a, lat_s, lon_s)
    d_s_b = haversine(lat_b, lon_b, lat_s, lon_s)
    d_direct = haversine(lat_a, lon_a, np.array([lat_b]), np.array([lon_b]))[0]
    return np.maximum(0.0, (d_a_s + d_s_b) - d_direct)


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


def list_drive_files() -> List[dict]:
    service = get_drive_service()
    results = (
        service.files()
        .list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and trashed = false",
            fields="files(id, name, modifiedTime)",
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


def list_drive_candidates(prefix: str) -> List[dict]:
    try:
        files = list_drive_files()
    except Exception:
        return []
    matching = [f for f in files if f["name"].startswith(prefix)]

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
    try:
        for item in list_drive_files():
            if item["name"].strip().lower() == "driver_master.csv":
                buf = download_drive_file(item["id"], item["name"])
                df = safe_read_csv(buf)
                df.columns = [c.strip() for c in df.columns]
                return df
    except Exception:
        pass
    path = get_base_dir() / "Locations" / "driver_master.csv"
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception:
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
    """
    Irving master uses integer Site # as join key — simple and 100% reliable.
    Columns: SITE NUMBER, Station_Name, Province, City, Address, Latitude, Longitude
    """
    path = get_base_dir() / "Locations" / "irving_master.csv"
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df["SITE NUMBER"] = pd.to_numeric(df["SITE NUMBER"], errors="coerce").astype("Int64")
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Price file parsers
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
    """
    Parse Irving price CSV.
    Expected columns: Site #, Site, City, Prov, Fuel Price (pre-tax $/L)
    Site # is an integer — used directly as join key to irving_master.
    Fuel Price may be stored as cents (e.g. 154.3) or dollars (e.g. 1.543).
    clean_price() handles both via median detection.
    """
    if hasattr(path_or_buffer, "seek"):
        path_or_buffer.seek(0)

    df = safe_read_csv(path_or_buffer)
    df.columns = [c.strip() for c in df.columns]

    # Normalise column names
    rename_map = {}
    for col in df.columns:
        cu = col.upper().strip()
        if cu in {"SITE #", "SITE#", "SITE NUMBER", "SITE_NUMBER"}:
            rename_map[col] = "SITE NUMBER"
        elif cu in {"FUEL PRICE", "FUEL_PRICE", "FUEL PRICE"}:
            rename_map[col] = "Price"
        elif cu in {"PROV", "PROVINCE"}:
            rename_map[col] = "Province"
        elif cu in {"CITY"}:
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
    """
    Looks for files named irving_prices_YYYY-MM-DD.csv on Drive then locally.
    Falls back gracefully — Irving being absent never breaks Petro/Esso display.
    """
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
# Matching
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
        on="match_key", how="left", suffixes=("", "_master"),
    )
    matched["Address_final"] = matched["Address"].fillna("Address missing")
    matched["Station_Final"] = matched["Station_Name_master"].fillna(matched["Station_Name"])
    matched["Latitude"] = pd.to_numeric(matched["Latitude"], errors="coerce")
    matched["Longitude"] = pd.to_numeric(matched["Longitude"], errors="coerce")
    matched["Network"] = "Petro"

    result = pd.DataFrame({
        "Station_Name": matched["Station_Final"],
        "Province": matched["Province"],
        "Network": matched["Network"],
        "Address": matched["Address_final"],
        "Latitude": matched["Latitude"],
        "Longitude": matched["Longitude"],
        "Price": matched["Price"],
        "City": np.nan,
        "Matched": matched["Address"].notna(),
    })
    result = result.drop_duplicates(subset=["Station_Name", "Province", "Price"]).reset_index(drop=True)
    stats = {
        "price_rows": len(result),
        "matched_rows": int(result["Matched"].sum()),
        "unmatched_rows": int((~result["Matched"]).sum()),
    }
    return result, stats


def match_esso(esso_prices: pd.DataFrame, master_esso: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    if esso_prices.empty:
        return pd.DataFrame(columns=_EMPTY_STATION_COLS), {"price_rows": 0, "matched_rows": 0, "unmatched_rows": 0}

    matched = esso_prices.merge(
        master_esso[["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude", "Province", "City"]],
        on="SITE NUMBER", how="left", suffixes=("", "_master"),
    )
    for col in ["Station_Name", "Address", "Latitude", "Longitude", "Province", "City"]:
        mc = f"{col}_master"
        if mc in matched.columns:
            matched[col] = matched[col].where(matched[col].notna(), matched[mc])

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
    """
    Irving uses integer Site # join — no fuzzy matching needed.
    Every site in the price file that exists in irving_master.csv gets
    full address + coordinates. Sites not in the master still appear
    but without coordinates (will be filtered out by distance calc).
    """
    if irving_prices.empty:
        return pd.DataFrame(columns=_EMPTY_STATION_COLS), {"price_rows": 0, "matched_rows": 0, "unmatched_rows": 0}

    matched = irving_prices.merge(
        master_irving[["SITE NUMBER", "Station_Name", "Province", "City", "Address", "Latitude", "Longitude"]],
        on="SITE NUMBER", how="left", suffixes=("_price", "_master"),
    )

    # Prefer master values; fall back to price-file values
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
# Main price table builder
# ---------------------------------------------------------------------------

def build_price_table(
    current_lat: float,
    current_lon: float,
    dest_lat: Optional[float] = None,
    dest_lon: Optional[float] = None,
    network_choice: str = "All",
    max_miles: float = 500,
    corridor_buffer_miles: float = 150,
    detour_cost_per_mile: float = DEFAULT_DETOUR_COST_PER_MILE,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Build the ranked fuel stop table across Petro, Esso, and Irving networks.
    network_choice: "Petro" | "Esso" | "Irving" | "All"
    """
    # Load prices for all three networks
    petro_prices, petro_file, petro_source = load_petro_prices()
    esso_prices, esso_file, esso_source = load_esso_prices()
    irving_prices, irving_file, irving_source = load_irving_prices()

    # Load masters
    petro_master = read_petro_master()
    esso_master = read_esso_master()
    irving_master = read_irving_master()

    # Match each network
    petro_df, petro_stats = match_petro(petro_prices, petro_master)
    esso_df, esso_stats = match_esso(esso_prices, esso_master)
    irving_df, irving_stats = match_irving(irving_prices, irving_master)

    # Filter by network selection
    include = {
        "All":    [petro_df, esso_df, irving_df],
        "Petro":  [petro_df],
        "Esso":   [esso_df],
        "Irving": [irving_df],
    }
    frames = include.get(network_choice, [petro_df, esso_df, irving_df])
    prices_df = pd.concat([f for f in frames if not f.empty], ignore_index=True)

    # Staleness
    petro_stale = price_staleness_days(petro_file)
    esso_stale = price_staleness_days(esso_file)
    irving_stale = price_staleness_days(irving_file)

    meta_base = {
        "latest_petro_file":   petro_file or "",
        "latest_esso_file":    esso_file or "",
        "latest_irving_file":  irving_file or "",
        "petro_source":        petro_source or "",
        "esso_source":         esso_source or "",
        "irving_source":       irving_source or "",
        "petro_source_rows":   len(petro_prices),
        "petro_matched_rows":  petro_stats["matched_rows"],
        "petro_unmatched_rows":petro_stats["unmatched_rows"],
        "esso_source_rows":    len(esso_prices),
        "esso_matched_rows":   esso_stats["matched_rows"],
        "esso_unmatched_rows": esso_stats["unmatched_rows"],
        "irving_source_rows":  len(irving_prices),
        "irving_matched_rows": irving_stats["matched_rows"],
        "irving_unmatched_rows":irving_stats["unmatched_rows"],
        "petro_stale_days":    petro_stale,
        "esso_stale_days":     esso_stale,
        "irving_stale_days":   irving_stale,
        "display_rows":        0,
        "avg_all_in":          0.0,
        "has_destination":     dest_lat is not None and dest_lon is not None,
    }

    base_cols = ["Station_Name", "Province", "Network", "Address", "Latitude", "Longitude", "Price", "City", "Matched"]
    for col in base_cols:
        if col not in prices_df.columns:
            prices_df[col] = np.nan

    if prices_df.empty:
        meta_base["display_rows"] = 0
        return prices_df, meta_base

    prices_df["Province"] = prices_df["Province"].astype(str).str.strip().str.upper()
    prices_df["Price"] = pd.to_numeric(prices_df["Price"], errors="coerce")
    prices_df["Latitude"] = pd.to_numeric(prices_df["Latitude"], errors="coerce")
    prices_df["Longitude"] = pd.to_numeric(prices_df["Longitude"], errors="coerce")
    prices_df["Address"] = prices_df["Address"].fillna("Address missing")
    prices_df["Matched"] = prices_df["Matched"].fillna(False)
    prices_df = prices_df.dropna(subset=["Price"]).copy()

    # Tax + all-in price
    prices_df["Sales_Tax_Rate"] = prices_df["Province"].map(PROV_TAX).fillna(0.13)
    prices_df["All_In_Price"] = (prices_df["Price"] * (1 + prices_df["Sales_Tax_Rate"])).round(4)

    # Distances
    prices_df["Miles_from_Current"] = np.round(
        haversine(current_lat, current_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )
    prices_df["Miles_from_Yard"] = np.round(
        haversine(DEFAULT_YARD["lat"], DEFAULT_YARD["lon"], prices_df["Latitude"], prices_df["Longitude"]), 1
    )
    use_dest_lat = dest_lat if dest_lat is not None else current_lat
    use_dest_lon = dest_lon if dest_lon is not None else current_lon
    prices_df["Miles_from_Destination"] = np.round(
        haversine(use_dest_lat, use_dest_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )

    # Filter: corridor (with dest) or radius (no dest)
    if dest_lat is not None and dest_lon is not None:
        prices_df["Detour_Extra_Miles"] = np.round(
            corridor_deviation(
                prices_df["Latitude"], prices_df["Longitude"],
                current_lat, current_lon,
                dest_lat, dest_lon,
            ), 1
        )
        prices_df = prices_df[
            prices_df["Latitude"].notna()
            & prices_df["Longitude"].notna()
            & (prices_df["Detour_Extra_Miles"] <= corridor_buffer_miles)
        ].copy()
    else:
        prices_df["Detour_Extra_Miles"] = prices_df["Miles_from_Current"]
        prices_df = prices_df[
            prices_df["Latitude"].notna()
            & prices_df["Longitude"].notna()
            & (prices_df["Miles_from_Current"] <= float(max_miles))
        ].copy()

    if prices_df.empty:
        meta_base["display_rows"] = 0
        return prices_df, meta_base

    avg_all_in = float(prices_df["All_In_Price"].mean())
    prices_df["Savings_per_1000L"] = np.round((avg_all_in - prices_df["All_In_Price"]) * 1000, 0)

    # Composite score: real value after detour cost
    detour_cost = prices_df["Detour_Extra_Miles"] * detour_cost_per_mile
    prices_df["Composite_Score"] = np.round(prices_df["Savings_per_1000L"] - detour_cost, 0)

    # Sort
    if dest_lat is not None and dest_lon is not None:
        prices_df = prices_df.sort_values(
            ["Composite_Score", "All_In_Price", "Station_Name"],
            ascending=[False, True, True],
        ).reset_index(drop=True)
    else:
        prices_df = prices_df.sort_values(
            ["All_In_Price", "Miles_from_Current", "Station_Name"],
            ascending=[True, True, True],
        ).reset_index(drop=True)

    meta_base["display_rows"] = len(prices_df)
    meta_base["avg_all_in"] = round(avg_all_in, 3)
    return prices_df, meta_base
