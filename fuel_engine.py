print("🔥 USING GOOGLE DRIVE VERSION OF FUEL ENGINE")
from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

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

DEFAULT_YARD = {"lat": 43.69823, "lon": -79.58937, "label": "Mississauga Yard"}

DRIVE_FOLDER_ID = "18Cqpj-pVLDk5Esx2r3Cj_IR6Bd7lubCT"
SERVICE_ACCOUNT_FILENAME = "gdrive_key.json"

LAST_FILE_INFO = {
    "petro": {"name": "", "source": "", "status": ""},
    "esso": {"name": "", "source": "", "status": ""},
}


def get_base_dir() -> Path:
    return Path(__file__).resolve().parent


def resolve_path(*candidates: str) -> Path:
    base = get_base_dir()
    for candidate in candidates:
        path = base / candidate
        if path.exists():
            return path
    raise FileNotFoundError(f"Could not find any of: {candidates}")


def get_service_account_path() -> Path:
    return get_base_dir() / SERVICE_ACCOUNT_FILENAME


def get_drive_service():
    key_path = get_service_account_path()
    if not key_path.exists():
        raise FileNotFoundError(f"Missing Google Drive key file: {key_path}")

    creds = service_account.Credentials.from_service_account_file(
        str(key_path),
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def glob_to_regex(pattern: str) -> str:
    escaped = re.escape(pattern).replace(r"\*", ".*")
    return f"^{escaped}$"


def list_drive_candidates(pattern: str) -> List[dict]:
    service = get_drive_service()
    results = service.files().list(
        q=f"'{DRIVE_FOLDER_ID}' in parents and trashed = false",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=200,
    ).execute()

    files = results.get("files", [])
    regex = re.compile(glob_to_regex(pattern), re.IGNORECASE)
    candidates = [f for f in files if regex.match(f["name"])]

    def sort_key(item: dict):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", item["name"])
        return (m.group(1) if m else "", item.get("modifiedTime", ""))

    candidates.sort(key=sort_key, reverse=True)
    return candidates


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


def list_local_candidates(pattern: str, prices_dir: Optional[Path] = None) -> List[Path]:
    prices_dir = prices_dir or resolve_path("Prices")
    files = sorted(prices_dir.glob(pattern))

    def sort_key(p: Path) -> Tuple[str, float]:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
        return (m.group(1) if m else "", p.stat().st_mtime)

    return sorted(files, key=sort_key, reverse=True)


def get_candidate_files(pattern: str, prices_dir: Optional[Path] = None) -> List[dict]:
    candidates: List[dict] = []

    try:
        for item in list_drive_candidates(pattern):
            candidates.append(
                {
                    "source": "google_drive",
                    "name": item["name"],
                    "id": item["id"],
                    "modifiedTime": item.get("modifiedTime", ""),
                }
            )
    except Exception:
        pass

    try:
        for path in list_local_candidates(pattern, prices_dir=prices_dir):
            candidates.append(
                {
                    "source": "local",
                    "name": path.name,
                    "path": path,
                }
            )
    except Exception:
        pass

    return candidates


def open_candidate(candidate: dict):
    if candidate["source"] == "google_drive":
        return download_drive_file(candidate["id"], candidate["name"])
    return candidate["path"]


def safe_read_csv(path_or_buffer, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(path_or_buffer, **kwargs)
    except pd.errors.ParserError:
        fallback_kwargs = dict(kwargs)
        fallback_kwargs.pop("engine", None)
        fallback_kwargs.pop("on_bad_lines", None)

        if hasattr(path_or_buffer, "seek"):
            path_or_buffer.seek(0)

        return pd.read_csv(path_or_buffer, engine="python", on_bad_lines="skip", **fallback_kwargs)


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


def haversine(lat1: float, lon1: float, lat2: pd.Series, lon2: pd.Series) -> np.ndarray:
    lat2_arr = pd.to_numeric(lat2, errors="coerce").fillna(0).to_numpy(dtype=float)
    lon2_arr = pd.to_numeric(lon2, errors="coerce").fillna(0).to_numpy(dtype=float)
    if lat1 is None or lon1 is None:
        return np.zeros(len(lat2_arr))

    r = 3958.8
    lat1r = np.radians(float(lat1))
    lon1r = np.radians(float(lon1))
    lat2r = np.radians(lat2_arr)
    lon2r = np.radians(lon2_arr)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return r * c


def read_petro_master() -> pd.DataFrame:
    path = resolve_path("Locations/petro_pass_master.csv", "petro_pass_master.csv")
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
    path = resolve_path("Locations/esso_cardlock_master.csv", "esso_cardlock_master.csv")
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

    if "SITE NUMBER" not in df.columns:
        df["SITE NUMBER"] = ""
    if "Province" not in df.columns:
        df["Province"] = ""
    if "Latitude" not in df.columns:
        df["Latitude"] = np.nan
    if "Longitude" not in df.columns:
        df["Longitude"] = np.nan
    if "Station_Name" not in df.columns:
        df["Station_Name"] = ""

    df["SITE NUMBER"] = df["SITE NUMBER"].astype(str).str.strip()
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["match_key"] = df["Station_Name"].map(normalize_text) + "|" + df["Province"]
    return df


def read_driver_master() -> Optional[pd.DataFrame]:
    try:
        path = resolve_path("Locations/driver_master.csv")
    except FileNotFoundError:
        return None
    df = safe_read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


def _parse_petro_from_content(content: str) -> pd.DataFrame:
    lines = content.splitlines()

    records = []
    started = False

    for raw_line in lines:
        line = raw_line.rstrip()

        if not started:
            if "SITE NAME" in line and "PST $/L" in line:
                started = True
            continue

        if not line.strip():
            continue
        if line.strip().startswith("---"):
            continue

        parts = [p.rstrip() for p in line.split(",")]

        station = None
        province = None
        price = None

        if len(parts) >= 3:
            p0 = parts[0].strip()
            p1 = parts[1].strip().upper()
            p2 = parts[2].strip()

            if re.fullmatch(r"[A-Z]{2}", p1):
                station = p0
                province = p1
                price = p2

        if station is None and len(parts) >= 3:
            m = re.match(r"^(?P<station>.+?)\s{2,}(?P<prov>[A-Z]{2})\s*$", parts[0].strip())
            if m:
                station = m.group("station").strip()
                province = m.group("prov").strip()
                price = parts[2].strip()

        if station is None and len(parts) >= 3:
            m = re.match(r"^(?P<station>.+?)\s+(?P<prov>[A-Z]{2})$", parts[0].strip())
            if m and parts[1].strip() == "":
                station = m.group("station").strip()
                province = m.group("prov").strip()
                price = parts[2].strip()

        if station is None and len(parts) >= 3:
            p0 = parts[0].strip()
            p2 = parts[2].strip()
            if p0 and re.fullmatch(r"\d+\.\d{4}", p2):
                station = p0
                province = ""
                price = p2

        if not station:
            continue

        junk_prefixes = (
            "ACCOUNT",
            "PRODUCT",
            "REGION",
            "AS OF",
            "PAGE",
            "DUE TO OCCASIONAL",
        )
        if station.upper().startswith(junk_prefixes):
            continue

        records.append(
            {
                "Station_Name": station.strip(),
                "Province": str(province or "").strip().upper(),
                "Price": price,
            }
        )

    df = pd.DataFrame(records)

    if df.empty:
        return df

    df["Price"] = clean_price(df["Price"])
    df = df.dropna(subset=["Price"]).copy()

    df["Province"] = df["Province"].replace(
        {
            "B": "BC",
            "A": "AB",
            "M": "MB",
            "N": "NB",
            "S": "SK",
            "Q": "QC",
            "Y": "YT",
        }
    )

    df["match_name"] = df["Station_Name"].map(normalize_text)
    df["match_key"] = df["match_name"] + "|" + df["Province"]

    return df.reset_index(drop=True)


def load_petro_prices(path: Optional[Path] = None) -> pd.DataFrame:
    if path is not None:
        if hasattr(path, "read"):
            path.seek(0)
            content = path.read().decode("utf-8", errors="replace")
        else:
            content = path.read_text(encoding="utf-8", errors="replace")
        return _parse_petro_from_content(content)

    candidates = get_candidate_files("petro_prices_*.csv")

    for candidate in candidates:
        try:
            obj = open_candidate(candidate)
            if hasattr(obj, "seek"):
                obj.seek(0)
                content = obj.read().decode("utf-8", errors="replace")
            else:
                content = obj.read_text(encoding="utf-8", errors="replace")

            df = _parse_petro_from_content(content)

            if not df.empty and len(df) >= 20:
                LAST_FILE_INFO["petro"] = {
                    "name": candidate["name"],
                    "source": candidate["source"],
                    "status": "ok",
                }
                return df
        except Exception:
            continue

    LAST_FILE_INFO["petro"] = {"name": "", "source": "", "status": "failed"}
    return pd.DataFrame()


def _parse_esso_from_obj(path_or_buffer) -> pd.DataFrame:
    if hasattr(path_or_buffer, "seek"):
        path_or_buffer.seek(0)

    df = safe_read_csv(path_or_buffer)
    df.columns = [c.strip() for c in df.columns]

    rename_map = {}
    for col in df.columns:
        upper = col.upper().strip()
        if upper in {"PROVINCE", "PROV"}:
            rename_map[col] = "Province"
        elif upper == "CITY":
            rename_map[col] = "City"
        elif upper in {"FUEL PRICE", "FUEL_PRICE", "PRICE"}:
            rename_map[col] = "Price"
        elif upper in {"SITE NUMBER", "SITE_NUMBER", "SITE#"}:
            rename_map[col] = "SITE_NUMBER"
        elif upper == "STATION NAME":
            rename_map[col] = "Station_Name"
        elif upper == "ADDRESS":
            rename_map[col] = "Address"

    df = df.rename(columns=rename_map)

    for col in ["SITE_NUMBER", "Station_Name", "Province", "City", "Address", "Price"]:
        if col not in df.columns:
            df[col] = np.nan

    df["SITE_NUMBER"] = df["SITE_NUMBER"].astype(str).str.strip()
    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df["Price"] = clean_price(df["Price"])
    df["match_key"] = df["Station_Name"].map(normalize_text) + "|" + df["Province"]
    df = df.dropna(subset=["Price"]).copy()

    return df.reset_index(drop=True)


def load_esso_prices(path: Optional[Path] = None) -> pd.DataFrame:
    if path is not None:
        return _parse_esso_from_obj(path)

    candidates = get_candidate_files("esso_prices_*.csv")

    for candidate in candidates:
        try:
            obj = open_candidate(candidate)
            df = _parse_esso_from_obj(obj)

            if not df.empty and len(df) >= 20:
                LAST_FILE_INFO["esso"] = {
                    "name": candidate["name"],
                    "source": candidate["source"],
                    "status": "ok",
                }
                return df
        except Exception:
            continue

    LAST_FILE_INFO["esso"] = {"name": "", "source": "", "status": "failed"}
    return pd.DataFrame()


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

        if "Address_fallback" in fallback.columns:
            matched["Address"] = matched["Address"].where(matched["Address"].notna(), fallback["Address_fallback"])

        if "Latitude_fallback" in fallback.columns:
            matched["Latitude"] = matched["Latitude"].where(matched["Latitude"].notna(), fallback["Latitude_fallback"])

        if "Longitude_fallback" in fallback.columns:
            matched["Longitude"] = matched["Longitude"].where(matched["Longitude"].notna(), fallback["Longitude_fallback"])

        if "Station_Name_fallback" in fallback.columns:
            matched["Station_Name_master"] = matched["Station_Name_master"].where(
                matched["Station_Name_master"].notna(),
                fallback["Station_Name_fallback"],
            )

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
            "Source_Station_Name": matched["Station_Name"],
            "Source_Site_Number": np.nan,
            "Matched": matched["Address"].notna(),
        }
    )

    result = result.drop_duplicates(subset=["Source_Station_Name", "Province", "Price"]).reset_index(drop=True)
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
        master_esso[["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude", "match_key"]],
        left_on="SITE_NUMBER",
        right_on="SITE NUMBER",
        how="left",
        suffixes=("", "_master"),
    )

    still_unmatched = matched[matched["Address_master"].isna()].copy()
    if not still_unmatched.empty:
        fallback = still_unmatched.drop(
            columns=["Station_Name_master", "Address_master", "Latitude", "Longitude", "match_key_master", "SITE NUMBER"],
            errors="ignore",
        ).copy()
        fallback = fallback.merge(
            master_esso[["match_key", "Station_Name", "Address", "Latitude", "Longitude"]],
            on="match_key",
            how="left",
            suffixes=("", "_master"),
        )
        for col in ["Station_Name_master", "Address_master", "Latitude", "Longitude"]:
            if col in fallback.columns:
                matched.loc[still_unmatched.index, col] = fallback[col].values

    matched["Address_final"] = matched["Address_master"].fillna(matched["Address"])
    matched["Station_Final"] = matched["Station_Name_master"].fillna(matched["Station_Name"])
    matched["Latitude"] = pd.to_numeric(matched["Latitude"], errors="coerce")
    matched["Longitude"] = pd.to_numeric(matched["Longitude"], errors="coerce")
    matched["Network"] = "Esso"

    result = pd.DataFrame(
        {
            "Station_Name": matched["Station_Final"],
            "Province": matched["Province"],
            "Network": matched["Network"],
            "Address": matched["Address_final"],
            "Latitude": matched["Latitude"],
            "Longitude": matched["Longitude"],
            "Price": matched["Price"],
            "City": matched["City"],
            "Source_Station_Name": matched["Station_Name"],
            "Source_Site_Number": matched["SITE_NUMBER"],
            "Matched": matched["Address_master"].notna(),
        }
    )

    result = result.drop_duplicates(subset=["Source_Site_Number", "Province", "Price"]).reset_index(drop=True)
    stats = {
        "price_rows": len(result),
        "matched_rows": int(result["Matched"].sum()),
        "unmatched_rows": int((~result["Matched"]).sum()),
    }
    return result, stats


def build_price_table(
    current_lat: float = DEFAULT_YARD["lat"],
    current_lon: float = DEFAULT_YARD["lon"],
    dest_lat: Optional[float] = None,
    dest_lon: Optional[float] = None,
    network_choice: str = "Both",
    max_miles: float = 1000,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    master_petro = read_petro_master()
    master_esso = read_esso_master()
    petro_prices = load_petro_prices()
    esso_prices = load_esso_prices()

    petro_df, petro_stats = match_petro(petro_prices, master_petro)
    esso_df, esso_stats = match_esso(esso_prices, master_esso)

    if network_choice == "Petro":
        prices_df = petro_df.copy()
    elif network_choice == "Esso":
        prices_df = esso_df.copy()
    else:
        prices_df = pd.concat([petro_df, esso_df], ignore_index=True)

    if prices_df.empty:
        meta = {
            "latest_petro_file": LAST_FILE_INFO["petro"]["name"],
            "latest_esso_file": LAST_FILE_INFO["esso"]["name"],
            "petro_source": LAST_FILE_INFO["petro"]["source"],
            "esso_source": LAST_FILE_INFO["esso"]["source"],
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

    prices_df["Address"] = prices_df["Address"].fillna("Address missing")
    prices_df["Latitude"] = pd.to_numeric(prices_df["Latitude"], errors="coerce")
    prices_df["Longitude"] = pd.to_numeric(prices_df["Longitude"], errors="coerce")
    prices_df["Price"] = clean_price(prices_df["Price"])
    prices_df = prices_df.dropna(subset=["Price"]).copy()

    prices_df["Sales_Tax_Rate"] = prices_df["Province"].map(PROV_TAX).fillna(0.13)
    prices_df["All_In_Price"] = (prices_df["Price"] * (1 + prices_df["Sales_Tax_Rate"])).round(3)
    avg_all_in = float(prices_df["All_In_Price"].mean()) if not prices_df.empty else 0.0

    dest_lat = current_lat if dest_lat is None else dest_lat
    dest_lon = current_lon if dest_lon is None else dest_lon

    prices_df["Miles_from_Current"] = np.round(
        haversine(current_lat, current_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )
    prices_df["Miles_from_Destination"] = np.round(
        haversine(dest_lat, dest_lon, prices_df["Latitude"], prices_df["Longitude"]), 1
    )
    prices_df["Miles_from_Yard"] = np.round(
        haversine(DEFAULT_YARD["lat"], DEFAULT_YARD["lon"], prices_df["Latitude"], prices_df["Longitude"]), 1
    )

    prices_df = prices_df[
        prices_df["Latitude"].notna() &
        prices_df["Longitude"].notna() &
        (prices_df["Miles_from_Current"] <= float(max_miles))
    ].copy()

    prices_df["Savings_per_1000L"] = np.round((avg_all_in - prices_df["All_In_Price"]) * 1000, 0)
    prices_df = prices_df.sort_values(["All_In_Price", "Miles_from_Current", "Station_Name"]).reset_index(drop=True)

    meta = {
        "latest_petro_file": LAST_FILE_INFO["petro"]["name"],
        "latest_esso_file": LAST_FILE_INFO["esso"]["name"],
        "petro_source": LAST_FILE_INFO["petro"]["source"],
        "esso_source": LAST_FILE_INFO["esso"]["source"],
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