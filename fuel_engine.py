from __future__ import annotations

import csv
import io
import os
import json
import re
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

print("🔥 USING GOOGLE DRIVE VERSION OF FUEL ENGINE")

DRIVE_FOLDER_ID = "18Cqpj-pVLDk5Esx2r3Cj_IR6Bd7lubCT"

DEFAULT_YARD = {"lat": 43.69823, "lon": -79.58937}

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

# ---------------- REQUIRED FUNCTIONS (FIX) ---------------- #

def get_base_dir():
    return Path(__file__).resolve().parent


def read_driver_master():
    try:
        path = get_base_dir() / "Locations" / "driver_master.csv"
        return pd.read_csv(path)
    except:
        return None

# ---------------- GOOGLE DRIVE ---------------- #

def get_drive_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise Exception("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    creds_dict = json.loads(raw)

    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )

    return build("drive", "v3", credentials=creds)


def list_drive_files():
    service = get_drive_service()

    results = service.files().list(
        q=f"'{DRIVE_FOLDER_ID}' in parents",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc",
    ).execute()

    return results.get("files", [])


def download_drive_file(file_id):
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh

# ---------------- FILE LOADING ---------------- #

def load_latest_from_drive(prefix):
    files = list_drive_files()
    matching = [f for f in files if f["name"].startswith(prefix)]

    for file in matching:
        try:
            content = download_drive_file(file["id"])
            return content, file["name"], "google_drive"
        except:
            continue

    return None, None, None


def load_latest_local(prefix):
    base = get_base_dir() / "Prices"
    files = sorted(base.glob(f"{prefix}*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)

    for f in files:
        return f, f.name, "local"

    return None, None, None

# ---------------- HELPERS ---------------- #

def safe_read_csv(obj):
    try:
        return pd.read_csv(obj)
    except:
        if hasattr(obj, "seek"):
            obj.seek(0)
        return pd.read_csv(obj, engine="python", on_bad_lines="skip")


def clean_price(series):
    return (
        series.astype(str)
        .str.replace(r"[^0-9.]", "", regex=True)
        .replace("", np.nan)
        .astype(float)
    )


def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8
    lat1, lon1 = np.radians(lat1), np.radians(lon1)
    lat2, lon2 = np.radians(lat2), np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))

    return R * c

# ---------------- LOADERS ---------------- #

def load_petro_prices():
    obj, name, source = load_latest_from_drive("petro_prices_")

    if obj:
        df = safe_read_csv(obj)
        df["Price"] = clean_price(df["Price"])
        return df, name, source

    obj, name, source = load_latest_local("petro_prices_")

    if obj:
        df = safe_read_csv(obj)
        df["Price"] = clean_price(df["Price"])
        return df, name, source

    return pd.DataFrame(), None, None


def load_esso_prices():
    obj, name, source = load_latest_from_drive("esso_prices_")

    if obj:
        df = safe_read_csv(obj)
        df["Price"] = clean_price(df["Price"])
        return df, name, source

    obj, name, source = load_latest_local("esso_prices_")

    if obj:
        df = safe_read_csv(obj)
        df["Price"] = clean_price(df["Price"])
        return df, name, source

    return pd.DataFrame(), None, None

# ---------------- MAIN ---------------- #

def build_price_table(current_lat, current_lon, max_miles=1000):

    petro_df, petro_file, petro_source = load_petro_prices()
    esso_df, esso_file, esso_source = load_esso_prices()

    df = pd.concat([petro_df, esso_df], ignore_index=True)

    if df.empty:
        return df, {}

    df["All_In_Price"] = df["Price"] * (1 + df["Province"].map(PROV_TAX).fillna(0.13))

    df["Miles_from_Current"] = haversine(
        current_lat,
        current_lon,
        df["Latitude"],
        df["Longitude"],
    )

    df = df[df["Miles_from_Current"] <= max_miles]

    avg_price = df["All_In_Price"].mean()

    df["Savings_per_1000L"] = (avg_price - df["All_In_Price"]) * 1000

    df = df.sort_values("All_In_Price")

    meta = {
        "latest_petro_file": petro_file,
        "latest_esso_file": esso_file,
        "petro_source": petro_source,
        "esso_source": esso_source,
        "display_rows": len(df),
    }

    return df.reset_index(drop=True), meta