"""
Parse the two UN school-count CSVs and merge into a single JSON payload
for the 3D globe dashboard.

Input:
  ../UN_school_count_data - Amount of Schools.csv        (2024 collection)
  ../2026_UN_school_count - Sheet1.csv                   (2026 collection)

Output:
  ./data.json   { meta: {...}, countries: [ {code, name, lat, lon,
                  y2024: {...}, y2026: {...}, growth: {...} } ] }
"""
from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import re
import urllib.request
from pathlib import Path
from typing import Optional


def _today_iso() -> str:
    return _dt.date.today().isoformat()

ROOT = Path(__file__).resolve().parent
SRC_2024 = ROOT.parent / "UN_school_count_data - Amount of Schools.csv"
SRC_2026 = ROOT.parent / "2026_UN_school_count - Sheet2.csv"
OUT = ROOT / "data.json"

CENTROIDS_URL = (
    "https://gist.githubusercontent.com/tadast/8827699/raw/"
    "f5cac3d42d16b78348610fc4ec301e9234f82821/countries_codes_and_coordinates.csv"
)


def load_centroids() -> dict[str, tuple[float, float, str]]:
    """Return {alpha2: (lat, lon, alpha3)}."""
    with urllib.request.urlopen(CENTROIDS_URL, timeout=30) as r:
        text = r.read().decode("utf-8")
    out: dict[str, tuple[float, float, str]] = {}
    for row in csv.DictReader(io.StringIO(text)):
        a2 = row["Alpha-2 code"].strip().strip('"').strip()
        a3 = row["Alpha-3 code"].strip().strip('"').strip()
        try:
            lat = float(row["Latitude (average)"].strip().strip('"'))
            lon = float(row["Longitude (average)"].strip().strip('"'))
        except ValueError:
            continue
        out[a2] = (lat, lon, a3)
    # A few manual overrides for small / special territories present in the data
    out.setdefault("XK", (42.6026, 20.9030, "XKX"))  # Kosovo
    return out


def to_int(value: Optional[str]) -> Optional[int]:
    """Parse a number that may have commas, spaces, or empty."""
    if value is None:
        return None
    s = str(value).strip().replace(",", "").replace(" ", "")
    if not s or s.lower() in {"nan", "n/a", "na", "-"}:
        return None
    # strip non-digits (handles e.g. "complex classification" cells slipping in)
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return int(float(m.group(0)))
    except ValueError:
        return None


def to_opt_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def parse_year(raw: Optional[str]) -> Optional[int]:
    """Extract the end year from strings like '2021', '2022-23', '2024-25'.

    Returns None for values outside a sensible range so typos in the source
    (e.g. "37712" for Israel) don't poison downstream comparisons.
    """
    if not raw:
        return None
    m2 = re.match(r"\s*(\d{4})\s*[-/]\s*(\d{2})\s*$", raw.strip())
    if m2:
        candidate = 2000 + int(m2.group(2))
        return candidate if 1950 <= candidate <= 2030 else None
    parts = re.findall(r"\b\d{4}\b", raw)
    if parts:
        candidates = [int(p) for p in parts if 1950 <= int(p) <= 2030]
        return max(candidates) if candidates else None
    m = re.search(r"\b(\d{2})[-/](\d{2})\b", raw)
    if m:
        candidate = 2000 + int(m.group(2))
        return candidate if 1950 <= candidate <= 2030 else None
    return None


def year_bounds(raw: Optional[str]) -> tuple[Optional[int], Optional[int]]:
    """Return (start_year, end_year) for a raw year string.

    Examples:
      "2024"      → (2024, 2024)
      "2022-23"   → (2022, 2023)
      "2023-24"   → (2023, 2024)
      "2017-18"   → (2017, 2018)

    Used for anomaly detection so the school year "2023-24" is correctly
    recognised as overlapping with "2023" (same academic year, different
    labelling) rather than being flagged as a year reversal.
    """
    if not raw:
        return (None, None)
    s = raw.strip()
    m = re.match(r"\s*(\d{4})\s*[-/]\s*(\d{2})\s*$", s)
    if m:
        start = int(m.group(1))
        end = 2000 + int(m.group(2))
        if 1950 <= start <= 2030 and 1950 <= end <= 2030:
            return (start, end)
    m = re.match(r"\s*(\d{4})\s*[-/]\s*(\d{4})\s*$", s)
    if m:
        start, end = int(m.group(1)), int(m.group(2))
        if 1950 <= start <= 2030 and 1950 <= end <= 2030:
            return (start, end)
    m = re.search(r"\b(\d{4})\b", s)
    if m:
        y = int(m.group(1))
        if 1950 <= y <= 2030:
            return (y, y)
    return (None, None)


def load_2024() -> list[dict]:
    rows: list[dict] = []
    with SRC_2024.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            code = (r.get("Country Code") or "").strip()
            if not code:
                continue
            rows.append(
                {
                    "code": code,
                    "name": to_opt_str(r.get("Country")),
                    "amount": to_int(r.get("Amount")),
                    "public": to_int(r.get("Public Schools")),
                    "private": to_int(r.get("Private Schools")),
                    "preschool": to_int(r.get("Pre-school")),
                    "primary": to_int(r.get("Primary")),
                    "secondary": to_int(r.get("Secondary")),
                    "source": to_opt_str(r.get("Source")),
                    "source_tag": to_opt_str(r.get("SourceTag")),
                    "quality": to_opt_str(r.get("Data Quality")),
                    "timeframe_raw": to_opt_str(r.get("Time-frame")),
                    "timeframe_year": parse_year(r.get("Time-frame")),
                }
            )
    return rows


def load_2026() -> list[dict]:
    rows: list[dict] = []
    with SRC_2026.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            code = (r.get("Country Code") or "").strip()
            if not code:
                continue
            rows.append(
                {
                    "code": code,
                    "name": to_opt_str(r.get("Country")),
                    "amount": to_int(r.get("Amount")),
                    "public": to_int(r.get("Public")),
                    "private": to_int(r.get("Private")),
                    # Sheet2 renamed this column to "Other-ownership";
                    # fall back to legacy "Othertype" for older rows.
                    "other_type": to_int(r.get("Other-ownership") or r.get("Othertype")),
                    "preschool": to_int(r.get("Pre-school")),
                    "primary": to_int(r.get("Primary")),
                    "secondary": to_int(r.get("Secondary")),
                    "other_combined": to_int(r.get("Other/combined")),
                    "source": to_opt_str(r.get("Source_Link")),
                    "source_tag": to_opt_str(r.get("SourceTag")),
                    "quality": to_opt_str(r.get("Data_Quality")),
                    "timeframe_raw": to_opt_str(r.get("Year")),
                    "timeframe_year": parse_year(r.get("Year")),
                    "notes": to_opt_str(r.get("Notes")),
                }
            )
    return rows


def main() -> None:
    centroids = load_centroids()
    y2024 = {r["code"]: r for r in load_2024()}
    y2026 = {r["code"]: r for r in load_2026()}

    all_codes = sorted(set(y2024) | set(y2026))
    countries: list[dict] = []
    missing_coords: list[str] = []
    year_anomalies: list[dict] = []

    for code in all_codes:
        c24 = y2024.get(code)
        c26 = y2026.get(code)
        name = (c26 or c24 or {}).get("name") or code

        coord = centroids.get(code)
        if coord is None:
            missing_coords.append(f"{code} / {name}")
            continue
        lat, lon, a3 = coord

        a24 = c24["amount"] if c24 else None
        a26 = c26["amount"] if c26 else None
        growth_abs = (a26 - a24) if (a24 is not None and a26 is not None) else None
        growth_pct = (growth_abs / a24 * 100.0) if (growth_abs is not None and a24) else None

        # Anomaly flag: only raise if the 2026 collection's END year is
        # *strictly earlier* than the 2024 collection's START year — i.e. the
        # two reported windows do not overlap at all. This correctly treats
        # "2023-24" (academic year, bounds 2023→2024) as overlapping with
        # "2023", so Libya / Venezuela (same academic year, different label)
        # are no longer false-flagged. Panama (2024 → 2022) is still caught
        # because the windows genuinely don't overlap.
        ty24 = c24["timeframe_year"] if c24 else None
        ty26 = c26["timeframe_year"] if c26 else None
        b24 = year_bounds(c24["timeframe_raw"]) if c24 else (None, None)
        b26 = year_bounds(c26["timeframe_raw"]) if c26 else (None, None)
        year_anomaly = False
        if b24[0] is not None and b26[1] is not None and b26[1] < b24[0]:
            year_anomaly = True
            year_anomalies.append(
                {"code": code, "name": name, "y2024_year": ty24, "y2026_year": ty26}
            )

        # Year-keyed schema: collections are keyed by the collection label
        # ("2024", "2026") but the inner `year` / `year_raw` fields carry the
        # source's *reporting* year. Forward-compat for V2 when more years
        # (e.g. "2020", "2021", ...) land as additional keys.
        obs_2024 = (
            {
                "collection": "2024",
                "amount": a24,
                "year": ty24,
                "year_raw": c24["timeframe_raw"] if c24 else None,
                "public": c24["public"] if c24 else None,
                "private": c24["private"] if c24 else None,
                "preschool": c24["preschool"] if c24 else None,
                "primary": c24["primary"] if c24 else None,
                "secondary": c24["secondary"] if c24 else None,
                "source": c24["source"] if c24 else None,
                "source_tag": c24.get("source_tag") if c24 else None,
                "quality": c24["quality"] if c24 else None,
            }
            if c24
            else None
        )
        obs_2026 = (
            {
                "collection": "2026",
                "amount": a26,
                "year": ty26,
                "year_raw": c26["timeframe_raw"] if c26 else None,
                "public": c26["public"] if c26 else None,
                "private": c26["private"] if c26 else None,
                "other_type": c26["other_type"] if c26 else None,
                "preschool": c26["preschool"] if c26 else None,
                "primary": c26["primary"] if c26 else None,
                "secondary": c26["secondary"] if c26 else None,
                "other_combined": c26["other_combined"] if c26 else None,
                "source": c26["source"] if c26 else None,
                "source_tag": c26.get("source_tag") if c26 else None,
                "quality": c26["quality"] if c26 else None,
                "notes": c26["notes"] if c26 else None,
            }
            if c26
            else None
        )
        years: dict[str, dict] = {}
        if obs_2024:
            years["2024"] = obs_2024
        if obs_2026:
            years["2026"] = obs_2026

        countries.append(
            {
                "code": code,
                "alpha3": a3,
                "name": name,
                "lat": lat,
                "lon": lon,
                "years": years,
                "reported_diff_abs": growth_abs,
                "reported_diff_pct": growth_pct,
                "year_anomaly": year_anomaly,
            }
        )

    total24 = sum(
        c["years"]["2024"]["amount"]
        for c in countries
        if "2024" in c["years"] and c["years"]["2024"]["amount"]
    )
    total26 = sum(
        c["years"]["2026"]["amount"]
        for c in countries
        if "2026" in c["years"] and c["years"]["2026"]["amount"]
    )

    def has_year(c: dict, y: str) -> bool:
        return y in c["years"] and c["years"][y]["amount"] is not None

    payload = {
        "meta": {
            "build_date": _today_iso(),
            "collections": ["2024", "2026"],
            "total_countries": len(countries),
            "countries_with_2024": sum(1 for c in countries if has_year(c, "2024")),
            "countries_with_2026": sum(1 for c in countries if has_year(c, "2026")),
            "countries_with_both": sum(
                1 for c in countries if has_year(c, "2024") and has_year(c, "2026")
            ),
            "total_schools_2024": total24,
            "total_schools_2026": total26,
            "year_anomalies": year_anomalies,
            "missing_coords": missing_coords,
            "publisher": "Sunstone Institute",
            "license": "CC BY 4.0",
        },
        "countries": countries,
    }

    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"wrote {OUT}")
    print(f"  countries:           {payload['meta']['total_countries']}")
    print(f"  both years reported: {payload['meta']['countries_with_both']}")
    print(f"  total schools 2024:  {total24:,}")
    print(f"  total schools 2026:  {total26:,}")
    if year_anomalies:
        print(f"  year anomalies:      {len(year_anomalies)}")
    if missing_coords:
        print(f"  missing coords:      {missing_coords}")


if __name__ == "__main__":
    main()
