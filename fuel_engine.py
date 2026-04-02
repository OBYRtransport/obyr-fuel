
from __future__ import annotations

import csv
import io
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
except Exception:  # pragma: no cover
    service_account = None
    build = None
    MediaIoBaseDownload = None

DRIVE_FOLDER_ID = "18Cqpj-pVLDk5Esx2r3Cj_IR6Bd7lubCT"

DEFAULT_YARD = {
    "lat": 43.69823,
    "lon": -79.58937,
    "label": "Mississauga Yard",
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
}

LAST_FILE_INFO = {
    "petro": {"name": "", "source": "", "status": ""},
    "esso": {"name": "", "source": "", "status": ""},
}


def get_base_dir() -> Path:
    return Path(__file__).resolve().parent


def read_driver_master() -> Optional[pd.DataFrame]:
    path = get_base_dir() / "Locations" / "driver_master.csv"
    if path.exists():
        return pd.read_csv(path)
    return None


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


def haversine(lat1: float, lon1: float, lat2: pd.Series, lon2: pd.Series) -> np.ndarray:
    lat2_arr = pd.to_numeric(lat2, errors="coerce").to_numpy(dtype=float)
    lon2_arr = pd.to_numeric(lon2, errors="coerce").to_numpy(dtype=float)
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


# ---------- Google Drive access ----------

def _get_drive_creds():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw:
        return json.loads(raw)
    key_path = get_base_dir() / "gdrive_key.json"
    if key_path.exists():
        with open(key_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return None


def get_drive_service():
    creds_dict = _get_drive_creds()
    if not creds_dict or service_account is None or build is None:
        raise RuntimeError("Google Drive unavailable")
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_drive_candidates(prefix: str) -> List[dict]:
    try:
        service = get_drive_service()
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and trashed = false",
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=200,
        ).execute()
        files = results.get("files", [])
        matching = [f for f in files if f["name"].startswith(prefix)]

        def sort_key(item: dict):
            m = re.search(r"(\d{4}-\d{2}-\d{2})", item["name"])
            return (m.group(1) if m else "", item.get("modifiedTime", ""))

        matching.sort(key=sort_key, reverse=True)
        return matching
    except Exception:
        return []


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


def list_local_candidates(prefix: str) -> List[Path]:
    base = get_base_dir() / "Prices"
    files = sorted(base.glob(f"{prefix}*.csv"))

    def sort_key(p: Path):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
        return (m.group(1) if m else "", p.stat().st_mtime)

    return sorted(files, key=sort_key, reverse=True)


# ---------- Master files ----------

def read_petro_master() -> pd.DataFrame:
    path = get_base_dir() / "Locations" / "petro_pass_master.csv"
    df = safe_read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    for col in ["Station_Name", "Province", "Address", "Latitude", "Longitude"]:
        if col not in df.columns:
            df[col] = np.nan
    df["Station_Name"] = df["Station_Name"].astype(str).str.strip()
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
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


# ---------- Parsing supplier files ----------

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
            p0 = parts[0].strip()
            p1 = parts[1].strip().upper()
            p2 = parts[2].strip()
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
            p0 = parts[0].strip()
            p2 = parts[2].strip()
            if p0 and re.fullmatch(r"\d+\.\d{4}", p2):
                station, province, price = p0, "", p2

        if not station:
            continue

        junk_prefixes = ("ACCOUNT", "PRODUCT", "REGION", "AS OF", "PAGE", "DUE TO OCCASIONAL")
        if station.upper().startswith(junk_prefixes):
            continue

        records.append({"Station_Name": station.strip(), "Province": str(province or "").strip().upper(), "Price": price})

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["Price"] = clean_price(df["Price"])
    df = df.dropna(subset=["Price"]).copy()
    df["Province"] = df["Province"].replace({"B": "BC", "A": "AB", "M": "MB", "N": "NB", "S": "SK", "Q": "QC", "Y": "YT"})
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


def load_petro_prices() -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    for item in list_drive_candidates("petro_prices_"):
        try:
            buf = download_drive_file(item["id"], item["name"])
            buf.seek(0)
            content = buf.read().decode("utf-8", errors="replace")
            df = _parse_petro_content(content)
            if not df.empty and len(df) >= 20:
                LAST_FILE_INFO["petro"] = {"name": item["name"], "source": "google_drive", "status": "ok"}
                return df, item["name"], "google_drive"
        except Exception:
            continue

    for path in list_local_candidates("petro_prices_"):
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            df = _parse_petro_content(content)
            if not df.empty and len(df) >= 20:
                LAST_FILE_INFO["petro"] = {"name": path.name, "source": "local", "status": "ok"}
                return df, path.name, "local"
        except Exception:
            continue

    LAST_FILE_INFO["petro"] = {"name": "", "source": "", "status": "failed"}
    return pd.DataFrame(), None, None


def load_esso_prices() -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    for item in list_drive_candidates("esso_prices_"):
        try:
            buf = download_drive_file(item["id"], item["name"])
            df = _parse_esso_obj(buf)
            if not df.empty and len(df) >= 20:
                LAST_FILE_INFO["esso"] = {"name": item["name"], "source": "google_drive", "status": "ok"}
                return df, item["name"], "google_drive"
        except Exception:
            continue

    for path in list_local_candidates("esso_prices_"):
        try:
            df = _parse_esso_obj(path)
            if not df.empty and len(df) >= 20:
                LAST_FILE_INFO["esso"] = {"name": path.name, "source": "local", "status": "ok"}
                return df, path.name, "local"
        except Exception:
            continue

    LAST_FILE_INFO["esso"] = {"name": "", "source": "", "status": "failed"}
    return pd.DataFrame(), None, None


def match_petro(petro_prices: pd.DataFrame, master_petro: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if petro_prices.empty:
        return pd.DataFrame(), {"price_rows": 0, "matched_rows": 0, "unmatched_rows": 0}

    matched = petro_prices.merge(
        master_petro[["match_key", "Station_Name", "Address", "Latitude", "Longitude"]],
        on="match_key",
        how="left",
        suffixes=("", "_master"),
    )

    still_unmatched = matched[matched["Address"].isna()].copy()
    if not still_unmatched.empty:
        master_station = master_petro.copy()
        master_station["match_name_only"] = master_station["Station_Name"].map(normalize_text)
        unique_names = (
            master_station.groupby("match_name_only")
            .size()
            .reset_index(name="cnt")
            .query("cnt == 1")["match_name_only"]
        )
        master_station = master_station[master_station["match_name_only"].isin(unique_names)].copy()

        fallback = still_unmatched.copy()
        fallback["match_name_only"] = fallback["Station_Name"].map(normalize_text)
        fallback = fallback.merge(
            master_station[["match_name_only", "Station_Name", "Province", "Address", "Latitude", "Longitude"]],
            on="match_name_only",
            how="left",
            suffixes=("", "_fallback"),
        )
        fallback.index = still_unmatched.index

        for target_col, fallback_col in [
            ("Address", "Address_fallback"),
            ("Latitude", "Latitude_fallback"),
            ("Longitude", "Longitude_fallback"),
            ("Station_Name_master", "Station_Name_fallback"),
        ]:
            if fallback_col in fallback.columns:
                matched[target_col] = matched[target_col].where(matched[target_col].notna(), fallback[fallback_col])

        if "Province_fallback" in fallback.columns:
            blank_province_mask = matched["Province"].fillna("").eq("") & matched.index.isin(fallback.index)
            matched.loc[blank_province_mask, "Province"] = fallback.loc[blank_province_mask, "Province_fallback"]

    matched["Address_final"] = matched["Address"].fillna("Address missing")
    matched["Station_Final"] = matched["Station_Name_master"].fillna(matched["Station_Name"])
    matched["Latitude"] = pd.to_numeric(matched["Latitude"], errors="coerce")
    matched["Longitude"] = pd.to_numeric(matched["Longitude"], errors="coerce")
    matched["Network"] = "Petro"

    result = pd.DataFrame(
        {
            "Station_Name": matched["Station_Final"],
            "Province": matched["Province"],
            "Network": matched["Network"],
            "Address": matched["Address_final"],
            "Latitude": matched["Latitude"],
            "Longitude": matched["Longitude"],
            "Price": matched["Price"],
            "City": np.nan,
            "Matched": matched["Address"].notna(),
        }
    )
    result = result.drop_duplicates(subset=["Station_Name", "Province", "Price"]).reset_index(drop=True)
    stats = {
        "price_rows": len(result),
        "matched_rows": int(result["Matched"].sum()),
        "unmatched_rows": int((~result["Matched"]).sum()),
    }
    return result, stats


def match_esso(esso_prices: pd.DataFrame, master_esso: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if esso_prices.empty:
        return pd.DataFrame(), {"price_rows": 0, "matched_rows": 0, "unmatched_rows": 0}

    matched = esso_prices.merge(
        master_esso[["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude", "Province", "City"]],
        on="SITE NUMBER",
        how="left",
        suffixes=("", "_master"),
    )

    for col in ["Station_Name", "Address", "Latitude", "Longitude", "Province", "City"]:
        master_col = f"{col}_master"
        if master_col in matched.columns:
            matched[col] = matched[col].where(matched[col].notna(), matched[master_col])

    matched["Network"] = "Esso"
    matched["Matched"] = matched["Address"].notna()

    result = pd.DataFrame(
        {
            "Station_Name": matched["Station_Name"],
            "Province": matched["Province"],
            "Network": matched["Network"],
            "Address": matched["Address"].fillna("Address missing"),
            "Latitude": pd.to_numeric(matched["Latitude"], errors="coerce"),
            "Longitude": pd.to_numeric(matched["Longitude"], errors="coerce"),
            "Price": matched["Price"],
            "City": matched["City"],
            "Matched": matched["Matched"],
        }
    )
    result = result.drop_duplicates(subset=["Station_Name", "Province", "Price"]).reset_index(drop=True)
    stats = {
        "price_rows": len(result),
        "matched_rows": int(result["Matched"].sum()),
        "unmatched_rows": int((~result["Matched"]).sum()),
    }
    return result, stats


def build_price_table(
    current_lat: float,
    current_lon: float,
    dest_lat: Optional[float] = None,
    dest_lon: Optional[float] = None,
    network_choice: str = "Both",
    max_miles: float = 1000,
):
    petro_prices, petro_file, petro_source = load_petro_prices()
    esso_prices, esso_file, esso_source = load_esso_prices()

    petro_master = read_petro_master()
    esso_master = read_esso_master()

    petro_df, petro_stats = match_petro(petro_prices, petro_master)
    esso_df, esso_stats = match_esso(esso_prices, esso_master)

    if network_choice == "Petro":
        prices_df = petro_df.copy()
    elif network_choice == "Esso":
        prices_df = esso_df.copy()
    else:
        prices_df = pd.concat([petro_df, esso_df], ignore_index=True)

    if prices_df.empty:
        meta = {
            "latest_petro_file": petro_file or "",
            "latest_esso_file": esso_file or "",
            "petro_source": petro_source or "",
            "esso_source": esso_source or "",
            "petro_stats": petro_stats,
            "esso_stats": esso_stats,
            "petro_source_rows": len(petro_prices),
            "petro_matched_rows": petro_stats["matched_rows"],
            "petro_unmatched_rows": petro_stats["unmatched_rows"],
            "esso_source_rows": len(esso_prices),
            "esso_matched_rows": esso_stats["matched_rows"],
            "esso_unmatched_rows": esso_stats["unmatched_rows"],
            "display_rows": 0,
            "avg_all_in": 0.0,
        }
        return prices_df, meta

    prices_df["Province"] = prices_df["Province"].astype(str).str.strip().str.upper()
    prices_df["Price"] = pd.to_numeric(prices_df["Price"], errors="coerce")
    prices_df["Latitude"] = pd.to_numeric(prices_df["Latitude"], errors="coerce")
    prices_df["Longitude"] = pd.to_numeric(prices_df["Longitude"], errors="coerce")
    prices_df["Address"] = prices_df["Address"].fillna("Address missing")
    prices_df["Matched"] = prices_df["Matched"].fillna(False)

    prices_df = prices_df.dropna(subset=["Price"]).copy()

    prices_df["Sales_Tax_Rate"] = prices_df["Province"].map(PROV_TAX).fillna(0.13)
    prices_df["All_In_Price"] = (prices_df["Price"] * (1 + prices_df["Sales_Tax_Rate"])).round(3)

    use_dest_lat = current_lat if dest_lat is None else dest_lat
    use_dest_lon = current_lon if dest_lon is None else dest_lon

    prices_df["Miles_from_Current"] = np.round(
        haversine(current_lat, current_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )
    prices_df["Miles_from_Destination"] = np.round(
        haversine(use_dest_lat, use_dest_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )
    prices_df["Miles_from_Yard"] = np.round(
        haversine(DEFAULT_YARD["lat"], DEFAULT_YARD["lon"], prices_df["Latitude"], prices_df["Longitude"]), 1
    )

    prices_df = prices_df[
        prices_df["Latitude"].notna()
        & prices_df["Longitude"].notna()
        & (prices_df["Miles_from_Current"] <= float(max_miles))
    ].copy()

    avg_all_in = float(prices_df["All_In_Price"].mean()) if not prices_df.empty else 0.0
    prices_df["Savings_per_1000L"] = np.round((avg_all_in - prices_df["All_In_Price"]) * 1000, 0)

    prices_df = prices_df.sort_values(
        ["All_In_Price", "Miles_from_Current", "Station_Name"],
        ascending=[True, True, True],
    ).reset_index(drop=True)

    meta = {
        "latest_petro_file": petro_file or "",
        "latest_esso_file": esso_file or "",
        "petro_source": petro_source or "",
        "esso_source": esso_source or "",
        "petro_stats": petro_stats,
        "esso_stats": esso_stats,
        "petro_source_rows": len(petro_prices),
        "petro_matched_rows": petro_stats["matched_rows"],
        "petro_unmatched_rows": petro_stats["unmatched_rows"],
        "esso_source_rows": len(esso_prices),
        "esso_matched_rows": esso_stats["matched_rows"],
        "esso_unmatched_rows": esso_stats["unmatched_rows"],
        "display_rows": len(prices_df),
        "avg_all_in": round(avg_all_in, 3),
    }

    return prices_df, meta
