"""
OBYR Geotab Engine
Pulls live fleet data from MyGeotab for the OBYR highway fleet.

Credentials are read exclusively from environment variables:
  GEOTAB_USERNAME   fuel@obyrtransport.com
  GEOTAB_PASSWORD   (set in Render dashboard — never in code)
  GEOTAB_DATABASE   obyr

Fleet registry is maintained below — update FLEET_REGISTRY when new
units arrive or are retired. No other code changes needed.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Fleet registry
# Unit numbers map to Geotab device names — adjust if Geotab uses a
# different naming convention (e.g. "OBYR-017" vs "017").
# ---------------------------------------------------------------------------

FLEET_REGISTRY = {
    "017": {
        "driver":   "Jarek Krzyzanowski",
        "vehicle":  "2018 Freightliner Cascadia",
        "engine":   "Detroit DD15",
        "hp":       475,
        "l100km":   38.0,   # DD15 2018 — slightly higher than new Cascadias
        "tank_l":   1136.0,
        "status":   "active",
    },
    "019": {
        "driver":   "Paul Edmondson",
        "vehicle":  "Western Star Glider",
        "engine":   "Detroit Series 60 Pre-EGR",
        "hp":       500,
        "l100km":   40.0,   # Series 60 pre-EGR — older, heavier consumption
        "tank_l":   1136.0,
        "status":   "active",
    },
    "020": {
        "driver":   "Guillermo Mejia",
        "vehicle":  "Peterbilt 389",
        "engine":   "Cummins X15 Performance",
        "hp":       605,
        "l100km":   39.0,   # X15 Performance — high HP, slightly higher burn
        "tank_l":   1136.0,
        "status":   "active",
    },
    "024": {
        "driver":   "Darek Milewski",
        "vehicle":  "Freightliner Cascadia",
        "engine":   "Detroit DD15",
        "hp":       505,
        "l100km":   35.0,   # New Cascadia — most fuel efficient
        "tank_l":   1136.0,
        "status":   "active",
    },
    "025": {
        "driver":   "Alexey Nikolaev",
        "vehicle":  "Freightliner Cascadia",
        "engine":   "Detroit DD15",
        "hp":       505,
        "l100km":   35.0,
        "tank_l":   1136.0,
        "status":   "active",
    },
    "027": {
        "driver":   "Tomek Brucki",
        "vehicle":  "Freightliner Cascadia",
        "engine":   "Detroit DD15",
        "hp":       505,
        "l100km":   35.0,
        "tank_l":   1136.0,
        "status":   "active",
    },
    "028": {
        "driver":   "Lukasz Szczepanski",
        "vehicle":  "Freightliner Cascadia",
        "engine":   "Detroit DD15",
        "hp":       505,
        "l100km":   35.0,
        "tank_l":   1136.0,
        "status":   "active",
    },
    # Arriving May 2026 — uncomment and populate once unit numbers assigned
    # "TBD1": {
    #     "driver":   "TBD",
    #     "vehicle":  "Freightliner Cascadia",
    #     "engine":   "Detroit DD15",
    #     "hp":       505,
    #     "l100km":   35.0,
    #     "tank_l":   1136.0,
    #     "status":   "arriving",
    # },
}

# Active units only — used for Geotab API queries
ACTIVE_UNITS = [u for u, s in FLEET_REGISTRY.items() if s["status"] == "active"]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GEOTAB_SERVER   = "my.geotab.com"
GEOTAB_DATABASE = os.getenv("GEOTAB_DATABASE", "obyr")
GEOTAB_USERNAME = os.getenv("GEOTAB_USERNAME", "")
GEOTAB_PASSWORD = os.getenv("GEOTAB_PASSWORD", "")

TANK_CAPACITY_L = 1136.0   # 300 US gallons — default, overridden per truck from registry
RESERVE_L       = 150.0    # ~40 gallons — never recommend running below this

GEOTAB_API_URL  = f"https://{GEOTAB_SERVER}/apiv1"

# ---------------------------------------------------------------------------
# Authentication — session token cached per Python process lifetime
# ---------------------------------------------------------------------------

_SESSION: Dict = {}   # keys: credentials, sessionId, userName, server


def _authenticate() -> Dict:
    """
    Authenticate with MyGeotab and return the session credentials dict.
    Re-authenticates if credentials have changed or session is absent.
    """
    global _SESSION

    creds_key = f"{GEOTAB_USERNAME}:{GEOTAB_DATABASE}"
    if _SESSION.get("_creds_key") == creds_key and _SESSION.get("sessionId"):
        return _SESSION

    payload = {
        "method": "Authenticate",
        "params": {
            "userName": GEOTAB_USERNAME,
            "password": GEOTAB_PASSWORD,
            "database": GEOTAB_DATABASE,
        },
    }
    resp = requests.post(GEOTAB_API_URL, json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"Geotab auth failed: {data['error'].get('message', data['error'])}")

    result = data["result"]
    credentials = result["credentials"]
    # Geotab may redirect to a different server after auth
    server = result.get("path", GEOTAB_SERVER)
    if server and server != "ThisServer":
        credentials["_server"] = server
    else:
        credentials["_server"] = GEOTAB_SERVER

    credentials["_creds_key"] = creds_key
    _SESSION = credentials
    return _SESSION


def _call(method: str, params: dict) -> dict:
    """
    Make an authenticated MyGeotab API call.
    Automatically re-authenticates on session expiry (InvalidUserException).
    """
    global _SESSION
    creds = _authenticate()
    server = creds.get("_server", GEOTAB_SERVER)

    payload = {
        "method": method,
        "params": {
            **params,
            "credentials": {
                "userName":  creds["userName"],
                "sessionId": creds["sessionId"],
                "database":  creds["database"],
            },
        },
    }

    resp = requests.post(f"https://{server}/apiv1", json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # Session expired — clear and retry once
    if "error" in data:
        msg = str(data["error"].get("message", ""))
        if "InvalidUser" in msg or "session" in msg.lower():
            _SESSION = {}
            return _call(method, params)
        raise RuntimeError(f"Geotab API error ({method}): {msg}")

    return data.get("result", {})


# ---------------------------------------------------------------------------
# Fleet data fetchers
# ---------------------------------------------------------------------------

def get_raw_device_names() -> List[Dict]:
    """
    Debug helper — return the raw name, id, and license of every device
    in the Geotab database so we can see exactly how vehicles are named.
    """
    result = _call("Get", {"typeName": "Device", "search": {}})
    return [
        {
            "id":      v.get("id", ""),
            "name":    v.get("name", ""),
            "license": v.get("licensePlate", ""),
            "serial":  v.get("serialNumber", ""),
        }
        for v in (result or [])
    ]


def get_vehicles() -> List[Dict]:
    """
    Return list of all active vehicles in the database.
    Each dict has: id, name, licensePlate, engineType
    """
    result = _call("Get", {"typeName": "Device", "search": {}})
    vehicles = []
    for v in (result or []):
        vehicles.append({
            "id":      v.get("id", ""),
            "name":    v.get("name", ""),
            "license": v.get("licensePlate", ""),
        })
    return vehicles


def get_latest_gps(device_ids: List[str]) -> Dict[str, Dict]:
    """
    Return the most recent GPS position for each device ID.
    Returns dict keyed by device_id: {lat, lon, speed_kmh, datetime}
    """
    result = _call("Get", {
        "typeName": "DeviceStatusInfo",
        "search": {
            "deviceSearch": {"ids": device_ids},
        },
    })

    positions = {}
    for item in (result or []):
        dev_id = item.get("device", {}).get("id", "")
        lat = item.get("latitude")
        lon = item.get("longitude")
        if dev_id and lat is not None and lon is not None:
            positions[dev_id] = {
                "lat":       float(lat),
                "lon":       float(lon),
                "speed_kmh": float(item.get("speed", 0)),
                "datetime":  item.get("dateTime", ""),
            }
    return positions


def get_fuel_levels(device_ids: List[str]) -> Dict[str, float]:
    """
    Return the most recent fuel level (0.0–1.0) for each device.
    Uses StatusData with the FuelLevel diagnostic.
    Returns dict keyed by device_id: fuel_fraction
    """
    # Geotab diagnostic ID for fuel level percentage
    FUEL_LEVEL_DIAG = "DiagnosticFuelLevelId"

    result = _call("Get", {
        "typeName": "StatusData",
        "search": {
            "deviceSearch":     {"ids": device_ids},
            "diagnosticSearch": {"id": FUEL_LEVEL_DIAG},
            "fromDate": (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "toDate":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    })

    # Keep only the most recent reading per device
    latest: Dict[str, Dict] = {}
    for item in (result or []):
        dev_id  = item.get("device", {}).get("id", "")
        dt_str  = item.get("dateTime", "")
        value   = item.get("data")
        if not dev_id or value is None:
            continue
        if dev_id not in latest or dt_str > latest[dev_id]["dt"]:
            latest[dev_id] = {"dt": dt_str, "value": float(value)}

    # Geotab returns fuel level as 0–100 or 0–1 depending on firmware
    levels = {}
    for dev_id, rec in latest.items():
        v = rec["value"]
        levels[dev_id] = v / 100.0 if v > 1.0 else v

    return levels


def get_fuel_economy(device_ids: List[str], days: int = 30) -> Dict[str, float]:
    """
    Return average fuel economy in L/100km per device over the last `days` days.
    Uses FuelEconomy diagnostic. Falls back to 38.0 if unavailable.
    """
    FUEL_ECONOMY_DIAG = "DiagnosticFuelEconomyId"

    from_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_date   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        result = _call("Get", {
            "typeName": "StatusData",
            "search": {
                "deviceSearch":     {"ids": device_ids},
                "diagnosticSearch": {"id": FUEL_ECONOMY_DIAG},
                "fromDate": from_date,
                "toDate":   to_date,
            },
        })
    except Exception:
        return {dev_id: 38.0 for dev_id in device_ids}

    # Average all readings per device
    sums:   Dict[str, float] = {}
    counts: Dict[str, int]   = {}
    for item in (result or []):
        dev_id = item.get("device", {}).get("id", "")
        value  = item.get("data")
        if not dev_id or value is None:
            continue
        sums[dev_id]   = sums.get(dev_id, 0.0) + float(value)
        counts[dev_id] = counts.get(dev_id, 0) + 1

    economy = {}
    for dev_id in device_ids:
        if dev_id in sums and counts[dev_id] > 0:
            avg = sums[dev_id] / counts[dev_id]
            # Geotab may return km/L — convert to L/100km if needed
            economy[dev_id] = (100.0 / avg) if avg > 5 else avg
        else:
            economy[dev_id] = 38.0

    return economy


def get_driver_assignments(device_ids: List[str]) -> Dict[str, str]:
    """
    Return the currently assigned driver name per device.
    Returns dict keyed by device_id: driver_name string
    """
    try:
        result = _call("Get", {
            "typeName": "DeviceStatusInfo",
            "search": {"deviceSearch": {"ids": device_ids}},
        })
        assignments = {}
        for item in (result or []):
            dev_id = item.get("device", {}).get("id", "")
            driver = item.get("driver", {})
            name   = driver.get("name", "") if isinstance(driver, dict) else ""
            assignments[dev_id] = name or "Unassigned"
        return assignments
    except Exception:
        return {dev_id: "Unassigned" for dev_id in device_ids}


# ---------------------------------------------------------------------------
# Main fleet snapshot builder  — rate-limited to one Geotab call per 5 min
# ---------------------------------------------------------------------------

import time as _time
_FLEET_CACHE: dict = {"data": None, "fetched_at": 0.0}
FLEET_CACHE_TTL = 300  # seconds — 5 minutes


def get_fleet_snapshot(force: bool = False) -> pd.DataFrame:
    """
    Returns fleet data cached for up to 5 minutes.
    Geotab is only contacted when:
      - Cache is empty (first load)
      - Cache is older than 5 minutes
      - force=True (explicit admin refresh button)
    This prevents Streamlit reruns from hammering the Geotab API.
    """
    global _FLEET_CACHE
    age = _time.time() - _FLEET_CACHE["fetched_at"]
    if not force and _FLEET_CACHE["data"] is not None and age < FLEET_CACHE_TTL:
        return _FLEET_CACHE["data"]

    result = _get_fleet_snapshot_live()
    _FLEET_CACHE["data"] = result
    _FLEET_CACHE["fetched_at"] = _time.time()
    return result


def _get_fleet_snapshot_live() -> pd.DataFrame:
    """
    Return a DataFrame with one row per active truck in FLEET_REGISTRY.

    Matches Geotab devices to registry by unit number (device name contains unit).
    Falls back to registry baselines for economy if Geotab data unavailable.

    Columns:
      unit, truck_name, driver, vehicle, engine, hp,
      lat, lon, speed_kmh,
      fuel_pct, fuel_litres, tank_l,
      range_km, economy_l100km,
      needs_fuel, status
    """
    try:
        vehicles = get_vehicles()
        if not vehicles:
            return pd.DataFrame()

        device_ids = [v["id"] for v in vehicles]

        gps      = get_latest_gps(device_ids)
        levels   = get_fuel_levels(device_ids)
        economy  = get_fuel_economy(device_ids)

        # Build a lookup: unit_number -> geotab vehicle dict
        # Prefer exact name match ("017") over contains match ("OBYR-017")
        # Also skip any device whose name starts with "do not use" or "Defect"
        unit_to_vehicle = {}
        skip_prefixes = ("do not use", "defect", "old ")

        # Pass 1: exact match
        for v in vehicles:
            name = str(v.get("name", "")).strip()
            if name.lower().startswith(skip_prefixes):
                continue
            for unit in ACTIVE_UNITS:
                if name == unit and unit not in unit_to_vehicle:
                    unit_to_vehicle[unit] = v

        # Pass 2: contains match for any still unmatched
        for v in vehicles:
            name = str(v.get("name", "")).strip()
            if name.lower().startswith(skip_prefixes):
                continue
            for unit in ACTIVE_UNITS:
                if unit not in unit_to_vehicle and unit in name:
                    unit_to_vehicle[unit] = v

        rows = []
        for unit in ACTIVE_UNITS:
            spec   = FLEET_REGISTRY[unit]
            v      = unit_to_vehicle.get(unit)
            dev_id = v["id"] if v else None

            pos    = gps.get(dev_id, {})     if dev_id else {}
            level  = levels.get(dev_id)      if dev_id else None
            # Use Geotab live economy if available, else registry baseline
            econ   = economy.get(dev_id, spec["l100km"]) if dev_id else spec["l100km"]
            # Clamp to reasonable range — Geotab can return outliers
            if not (20.0 <= econ <= 60.0):
                econ = spec["l100km"]

            tank_l      = spec["tank_l"]
            fuel_litres = (level * tank_l) if level is not None else None
            usable_l    = max(0.0, fuel_litres - RESERVE_L) if fuel_litres is not None else None
            range_km    = (usable_l / econ * 100.0) if usable_l is not None and econ > 0 else None

            if range_km is None:
                status = "❓ No data"
            elif range_km < 150:
                status = "🔴 Low Fuel"
            elif range_km < 300:
                status = "⚠️ Fuel Soon"
            else:
                status = "✅ OK"

            rows.append({
                "unit":           unit,
                "truck_name":     v["name"] if v else f"Unit {unit}",
                "driver":         spec["driver"],
                "vehicle":        spec["vehicle"],
                "engine":         spec["engine"],
                "hp":             spec["hp"],
                "lat":            pos.get("lat"),
                "lon":            pos.get("lon"),
                "speed_kmh":      pos.get("speed_kmh", 0),
                "fuel_pct":       round(level * 100, 1) if level is not None else None,
                "fuel_litres":    round(fuel_litres, 0) if fuel_litres is not None else None,
                "tank_l":         tank_l,
                "range_km":       round(range_km, 0) if range_km is not None else None,
                "economy_l100km": round(econ, 1),
                "needs_fuel":     (range_km is not None and range_km < 300),
                "status":         status,
            })

        return pd.DataFrame(rows)

    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


# ---------------------------------------------------------------------------
# Fuel window calculator
# ---------------------------------------------------------------------------

def fuel_window(
    current_lat: float,
    current_lon: float,
    dest_lat: float,
    dest_lon: float,
    fuel_litres: float,
    economy_l100km: float,
    route_km: float,
) -> Dict:
    """
    Given a truck's current state and route, return the optimal fueling window:

      must_fuel_by_km   — latest point on route where truck MUST stop (reserve threshold)
      optimal_from_km   — earliest point worth stopping (don't buy fuel you could buy cheaper later)
      usable_range_km   — how far truck can go above reserve
      pct_route_covered — how much of the route the truck can cover on current fuel

    The fueling window is the corridor between optimal_from_km and must_fuel_by_km.
    Stations outside this window are filtered from recommendations.
    """
    usable_l        = max(0.0, fuel_litres - RESERVE_L)
    usable_range_km = (usable_l / economy_l100km * 100.0) if economy_l100km > 0 else 0.0

    # Must fuel before running into reserve
    must_fuel_by_km = usable_range_km

    # Optimal earliest stop — don't stop in first 20% of range
    # (prices tend to be similar close to origin; save the stop for further along)
    optimal_from_km = usable_range_km * 0.20

    pct_route = min(1.0, usable_range_km / route_km) if route_km > 0 else 1.0

    return {
        "usable_range_km":    round(usable_range_km, 0),
        "must_fuel_by_km":    round(must_fuel_by_km, 0),
        "optimal_from_km":    round(optimal_from_km, 0),
        "pct_route_covered":  round(pct_route * 100, 1),
        "will_make_it":       usable_range_km >= route_km,
    }
