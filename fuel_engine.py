from __future__ import annotations

import csv
import io
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

print("🔥 USING GOOGLE DRIVE VERSION OF FUEL ENGINE")

DRIVE_FOLDER_ID = "18Cqpj-pVLDk5Esx2r3Cj_IR6Bd7lubCT"

DEFAULT_YARD = {
    "lat": 43.69823,
    "lon": -79.58937,
    "label": "Mississauga Yard",
}

PROV_TAX = {
    "ON": 0.13,
    "QC": 0.14975,
    "NB": 0.15,
    "NS": 0.15,
    "NL": 0.15,
    "MB": 0.07,
    "SK": 0.06,
    "AB": 0.05,
    "BC": 0.12,
}

LAST_FILE_INFO = {
    "petro": {"name": "", "source": "", "status": ""},
    "esso": {"name": "", "source": "", "status": ""},
}


def get_base_dir() -> Path:
    return Path(__file__).resolve().parent


def read_driver_master():
    try:
        path = get_base_dir() / "Locations" / "driver_master.csv"
        return pd.read_csv(path)
    except Exception:
        return None


def get_drive_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise Exception("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    creds_dict = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_drive_files():
    service = get_drive_service()
    results = service.files().list(
        q=f"'{DRIVE_FOLDER_ID}' in parents and trashed = false",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=200,
    ).execute()
    return results.get("files", [])


def download_drive_file(file_id: str) -> io.BytesIO:
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh


def load_latest_from_drive(prefix: str):
    try:
        files = list_drive_files()
        matching = [f for f in files if f["name"].startswith(prefix)]

        for file in matching:
            try:
                content = download_drive_file(file["id"])
                return content, file["name"], "google_drive"
            except Exception:
                continue
    except Exception:
        pass

    return None, None, None


def load_latest_local(prefix: str):
    base = get_base_dir() / "Prices"
    try:
        files = sorted(base.glob(f"{prefix}*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        for f in files:
            return f, f.name, "local"
    except Exception:
        pass

    return None, None, None


def safe_read_csv(obj):
    try:
        return pd.read_csv(obj)
    except Exception:
        if hasattr(obj, "seek"):
            obj.seek(0)
        return pd.read_csv(obj, engine="python", on_bad_lines="skip")


def clean_price(series):
    cleaned = (
        series.astype(str)
        .str.replace(r"[^0-9.]", "", regex=True)
        .replace("", np.nan)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def haversine(lat1, lon1, lat2, lon2):
    lat2 = pd.to_numeric(lat2, errors="coerce")
    lon2 = pd.to_numeric(lon2, errors="coerce")

    valid = lat2.notna() & lon2.notna()

    result = np.full(len(lat2), np.nan)

    if valid.any():
        R = 3958.8
        lat1r = np.radians(lat1)
        lon1r = np.radians(lon1)
        lat2r = np.radians(lat2[valid].astype(float))
        lon2r = np.radians(lon2[valid].astype(float))

        dlat = lat2r - lat1r
        dlon = lon2r - lon1r

        a = np.sin(dlat / 2) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2) ** 2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

        result[valid] = R * c

    return result


def read_petro_master():
    path = get_base_dir() / "Locations" / "petro_pass_master.csv"
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    for col in ["Station_Name", "Province", "Address", "Latitude", "Longitude"]:
        if col not in df.columns:
            df[col] = np.nan

    return df


def read_esso_master():
    path = get_base_dir() / "Locations" / "esso_cardlock_master.csv"

    rows = []
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

    return df


def normalize_site_number(series):
    return series.astype(str).str.strip()


def normalize_price_df(df):
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
        elif cu == "LATITUDE":
            rename_map[col] = "Latitude"
        elif cu == "LONGITUDE":
            rename_map[col] = "Longitude"

    df = df.rename(columns=rename_map)

    for col in ["SITE NUMBER", "Station_Name", "Address", "Latitude", "Longitude", "Province", "City", "Price"]:
        if col not in df.columns:
            df[col] = np.nan

    df["SITE NUMBER"] = normalize_site_number(df["SITE NUMBER"])
    df["Price"] = clean_price(df["Price"])
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    return df


def load_petro_prices():
    obj, name, source = load_latest_from_drive("petro_prices_")

    if obj is None:
        obj, name, source = load_latest_local("petro_prices_")

    if obj is None:
        LAST_FILE_INFO["petro"] = {"name": "", "source": "", "status": "missing"}
        return pd.DataFrame(), None, None

    df = safe_read_csv(obj)
    df = normalize_price_df(df)
    df = df.dropna(subset=["Price"])

    LAST_FILE_INFO["petro"] = {"name": name or "", "source": source or "", "status": "ok"}
    return df, name, source


def load_esso_prices():
    obj, name, source = load_latest_from_drive("esso_prices_")

    if obj is None:
        obj, name, source = load_latest_local("esso_prices_")

    if obj is None:
        LAST_FILE_INFO["esso"] = {"name": "", "source": "", "status": "missing"}
        return pd.DataFrame(), None, None

    df = safe_read_csv(obj)
    df = normalize_price_df(df)
    df = df.dropna(subset=["Price"])

    LAST_FILE_INFO["esso"] = {"name": name or "", "source": source or "", "status": "ok"}
    return df, name, source


def enrich_with_master(price_df, master_df):
    if price_df.empty:
        return price_df

    master = master_df.copy()
    master["SITE NUMBER"] = normalize_site_number(master["SITE NUMBER"])

    merged = price_df.merge(
        master[
            [
                "SITE NUMBER",
                "Station_Name",
                "Address",
                "Latitude",
                "Longitude",
                "Province",
                "City",
            ]
        ],
        on="SITE NUMBER",
        how="left",
        suffixes=("", "_master"),
    )

    for col in ["Station_Name", "Address", "Latitude", "Longitude", "Province", "City"]:
        master_col = f"{col}_master"
        if master_col in merged.columns:
            merged[col] = merged[col].where(merged[col].notna(), merged[master_col])

    return merged


def build_price_table(current_lat, current_lon, dest_lat=None, dest_lon=None, network_choice="Both", max_miles=1000):
    petro_prices, petro_file, petro_source = load_petro_prices()
    esso_prices, esso_file, esso_source = load_esso_prices()

    petro_master = read_petro_master()
    esso_master = read_esso_master()

    petro_df = enrich_with_master(petro_prices, petro_master)
    esso_df = enrich_with_master(esso_prices, esso_master)

    petro_df["Network"] = "Petro"
    esso_df["Network"] = "Esso"

    petro_stats = {
        "matched_rows": int(petro_df["Address"].notna().sum()) if not petro_df.empty else 0,
        "unmatched_rows": int(petro_df["Address"].isna().sum()) if not petro_df.empty else 0,
    }
    esso_stats = {
        "matched_rows": int(esso_df["Address"].notna().sum()) if not esso_df.empty else 0,
        "unmatched_rows": int(esso_df["Address"].isna().sum()) if not esso_df.empty else 0,
    }

    if network_choice == "Petro":
        df = petro_df.copy()
    elif network_choice == "Esso":
        df = esso_df.copy()
    else:
        df = pd.concat([petro_df, esso_df], ignore_index=True)

    if df.empty:
        meta = {
            "latest_petro_file": petro_file,
            "latest_esso_file": esso_file,
            "petro_source": petro_source,
            "esso_source": esso_source,
            "petro_stats": petro_stats,
            "esso_stats": esso_stats,
            "petro_source_rows": len(petro_df),
            "petro_matched_rows": petro_stats["matched_rows"],
            "petro_unmatched_rows": petro_stats["unmatched_rows"],
            "esso_source_rows": len(esso_df),
            "esso_matched_rows": esso_stats["matched_rows"],
            "esso_unmatched_rows": esso_stats["unmatched_rows"],
            "display_rows": 0,
            "avg_all_in": 0.0,
        }
        return df, meta

    df["Province"] = df["Province"].astype(str).str.strip().str.upper()
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["Address"] = df["Address"].fillna("Address missing")

    df = df.dropna(subset=["Price"]).copy()

    df["Sales_Tax_Rate"] = df["Province"].map(PROV_TAX).fillna(0.13)
    df["All_In_Price"] = (df["Price"] * (1 + df["Sales_Tax_Rate"])).round(3)

    df["Miles_from_Current"] = haversine(
        current_lat,
        current_lon,
        df["Latitude"],
        df["Longitude"],
    ).round(1)

    use_dest_lat = current_lat if dest_lat is None else dest_lat
    use_dest_lon = current_lon if dest_lon is None else dest_lon

    df["Miles_from_Destination"] = haversine(
        use_dest_lat,
        use_dest_lon,
        df["Latitude"],
        df["Longitude"],
    ).round(1)

    df["Miles_from_Yard"] = haversine(
        DEFAULT_YARD["lat"],
        DEFAULT_YARD["lon"],
        df["Latitude"],
        df["Longitude"],
    ).round(1)

    df = df[
        df["Latitude"].notna()
        & df["Longitude"].notna()
        & (df["Miles_from_Current"] <= float(max_miles))
    ].copy()

    avg_price = float(df["All_In_Price"].mean()) if not df.empty else 0.0
    df["Savings_per_1000L"] = ((avg_price - df["All_In_Price"]) * 1000).round(0)

    df = df.sort_values(["All_In_Price", "Miles_from_Current"], ascending=[True, True]).reset_index(drop=True)

    meta = {
        "latest_petro_file": petro_file,
        "latest_esso_file": esso_file,
        "petro_source": petro_source,
        "esso_source": esso_source,
        "petro_stats": petro_stats,
        "esso_stats": esso_stats,
        "petro_source_rows": len(petro_df),
        "petro_matched_rows": petro_stats["matched_rows"],
        "petro_unmatched_rows": petro_stats["unmatched_rows"],
        "esso_source_rows": len(esso_df),
        "esso_matched_rows": esso_stats["matched_rows"],
        "esso_unmatched_rows": esso_stats["unmatched_rows"],
        "display_rows": len(df),
        "avg_all_in": round(avg_price, 3),
    }

    return df, meta