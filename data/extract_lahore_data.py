"""
Lahore Geospatial Dataset Extractor
====================================
Extracts two datasets for ML-based commercial site selection:
  1. lahore_population.csv  — population density mapped to lat/lon
  2. lahore_commercial.csv  — commercial zones/buildings with centroids

INSTALL DEPENDENCIES (run once in your terminal / Anaconda prompt):
--------------------------------------------------------------------
pip install osmnx geopandas pandas requests h3

For conda users (recommended on Windows to avoid GDAL issues):
  conda install -c conda-forge osmnx geopandas
  pip install h3 requests
"""

import requests
import pandas as pd
import geopandas as gpd
import osmnx as ox
import h3
import json
import os

# ──────────────────────────────────────────────
#  Lahore Bounding Box  (tight city boundary)
#  South, West, North, East
# ──────────────────────────────────────────────
LAHORE_BBOX = {
    "south": 31.41,
    "west":  74.01,
    "north": 31.65,
    "east":  74.47,
}

OUTPUT_DIR = "."   # saves CSVs in the same folder as this script


# ══════════════════════════════════════════════════════════════════
#  TASK 1 — Lahore Population Data
#  Source: Kontur Population (H3 hex grid, ~400 m resolution)
#  Free, no API key required.
# ══════════════════════════════════════════════════════════════════

def download_kontur_population() -> pd.DataFrame:
    """
    Downloads Kontur Population dataset for Pakistan and clips it to
    Lahore's bounding box.  Returns a DataFrame with columns:
        Latitude, Longitude, Population
    Each row is the centroid of one H3 hex cell (~400 m across).
    """

    # Kontur publishes per-country GeoPackage files (~200 MB for Pakistan).
    # We stream only the HTTP response headers first to confirm the URL is
    # alive, then download to a temp file and read with GeoPandas.
    KONTUR_URL = (
        "https://geodata-eu-central-1-kontur-public.s3.amazonaws.com"
        "/kontur_datasets/kontur_population_PK_20231101.gpkg.gz"
    )

    local_gz   = os.path.join(OUTPUT_DIR, "_kontur_pk.gpkg.gz")
    local_gpkg = os.path.join(OUTPUT_DIR, "_kontur_pk.gpkg")

    # ── Download ──────────────────────────────────────────────────
    if not os.path.exists(local_gpkg):
        print("[Task 1] Downloading Kontur Population for Pakistan (~200 MB) …")
        print("         (This happens only once; subsequent runs reuse the cached file.)")
        with requests.get(KONTUR_URL, stream=True, timeout=300) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            with open(local_gz, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):   # 1 MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(f"\r         {pct:.1f}%  ({downloaded>>20} MB / {total>>20} MB)", end="")
        print("\n[Task 1] Download complete. Decompressing …")

        import gzip, shutil
        with gzip.open(local_gz, "rb") as f_in, open(local_gpkg, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(local_gz)

    # ── Load & clip to Lahore ─────────────────────────────────────
    # Kontur GeoPackages are stored in EPSG:3857 (Web Mercator, metres).
    # gpd.read_file(bbox=...) expects the bbox in the file's native CRS,
    # so we must convert our WGS-84 lat/lon corners to EPSG:3857 first.
    from pyproj import Transformer
    wgs84_to_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    # always_xy=True means we pass (lon, lat) → (x_m, y_m)
    x_min, y_min = wgs84_to_3857.transform(LAHORE_BBOX["west"],  LAHORE_BBOX["south"])
    x_max, y_max = wgs84_to_3857.transform(LAHORE_BBOX["east"],  LAHORE_BBOX["north"])

    print("[Task 1] Reading GeoPackage and clipping to Lahore …")
    print(f"         EPSG:3857 bbox  →  x: {x_min:.0f} – {x_max:.0f}   y: {y_min:.0f} – {y_max:.0f}")

    gdf = gpd.read_file(
        local_gpkg,
        bbox=(x_min, y_min, x_max, y_max),   # native CRS units (metres)
    )

    if gdf.empty:
        raise RuntimeError(
            "No population records found inside Lahore's bounding box even after CRS fix.\n"
            "Try opening the .gpkg in QGIS to confirm data coverage."
        )

    # Reproject to WGS-84 so centroids come out as lat/lon degrees
    gdf = gdf.to_crs(epsg=4326)

    # Compute centroids in a projected CRS (UTM 42N) for accuracy,
    # then convert the centroid points back to WGS-84.
    gdf_proj = gdf.to_crs(epsg=32642)
    gdf["centroid"] = gdf_proj.geometry.centroid.to_crs(epsg=4326)

    # Kontur column names vary slightly by release; find the population column
    pop_col = next(
        (c for c in gdf.columns if "population" in c.lower() and c != "geometry"),
        None,
    )
    if pop_col is None:
        raise RuntimeError(
            f"Could not find a population column. Available columns: {list(gdf.columns)}"
        )

    df = pd.DataFrame({
        "Latitude":   gdf["centroid"].y.round(6),
        "Longitude":  gdf["centroid"].x.round(6),
        "Population": gdf[pop_col].fillna(0).astype(int),
    })

    # Drop rows with zero population (unpopulated hexes)
    df = df[df["Population"] > 0].reset_index(drop=True)
    return df


def task1_population():
    print("\n" + "="*60)
    print("TASK 1 — Lahore Population Data")
    print("="*60)

    df = download_kontur_population()

    out_path = os.path.join(OUTPUT_DIR, "lahore_population.csv")
    df.to_csv(out_path, index=False)

    print(f"[Task 1] ✓ Saved {len(df):,} rows → {out_path}")
    print(df.head())


# ══════════════════════════════════════════════════════════════════
#  TASK 2 — Lahore Commercial Area Network
#  Source: OpenStreetMap via osmnx
#  Tags queried:
#    • landuse = commercial
#    • building = commercial
#    • shop = *   (all shop types — strong commercial signal)
# ══════════════════════════════════════════════════════════════════

def fetch_osm_commercial() -> pd.DataFrame:
    """
    Pulls commercial landuse zones from OpenStreetMap for Lahore.
    Returns a DataFrame with columns: Latitude, Longitude, Type, Name.

    Strategy: raw OverpassQL query via requests, bypassing osmnx's
    sub-query splitting entirely.  Falls back to a mirror server on
    failure, and caches the raw JSON so a re-run never re-downloads.
    """
    import json, time

    # ── Overpass servers (primary + two mirrors) ───────────────────
    SERVERS = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    ]

    CACHE = os.path.join(OUTPUT_DIR, "_overpass_commercial.json")

    S  = LAHORE_BBOX["south"]
    W  = LAHORE_BBOX["west"]
    N  = LAHORE_BBOX["north"]
    E  = LAHORE_BBOX["east"]

    # OverpassQL: fetch all nodes/ways/relations tagged landuse=commercial
    # inside the Lahore bbox.  [timeout:900] is sent TO the server so it
    # won't abort a slow query; out center gives us a representative point
    # for every way/relation without downloading full geometry.
    QUERY = f"""
[out:json][timeout:900];
(
  node["landuse"="commercial"]({S},{W},{N},{E});
  way["landuse"="commercial"]({S},{W},{N},{E});
  relation["landuse"="commercial"]({S},{W},{N},{E});
);
out center tags;
"""

    # ── Fetch (or load from cache) ─────────────────────────────────
    if os.path.exists(CACHE):
        print("[Task 2] Loading cached Overpass response …")
        with open(CACHE, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = None
        for i, server in enumerate(SERVERS):
            print(f"[Task 2] Trying server {i+1}/{len(SERVERS)}: {server}")
            try:
                resp = requests.post(
                    server,
                    data={"data": QUERY},
                    timeout=960,          # client-side timeout > server timeout
                    headers={"User-Agent": "LahoreMLProject/1.0"},
                )
                resp.raise_for_status()
                data = resp.json()
                # Cache the raw response so re-runs are instant
                with open(CACHE, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                print(f"[Task 2] Success on server {i+1}.")
                break
            except Exception as e:
                print(f"[Task 2] Server {i+1} failed: {e}")
                if i < len(SERVERS) - 1:
                    print("         Waiting 10 s before trying next mirror …")
                    time.sleep(10)

        if data is None:
            raise RuntimeError(
                "All Overpass servers failed. Try again in a few minutes."
            )

    # ── Parse elements ─────────────────────────────────────────────
    elements = data.get("elements", [])
    if not elements:
        raise RuntimeError("Overpass returned zero elements for Lahore commercial zones.")

    records = []
    for el in elements:
        etype = el.get("type")
        tags  = el.get("tags", {})

        # Nodes have lat/lon directly; ways/relations have a 'center' key
        if etype == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            center = el.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")

        if lat is None or lon is None:
            continue

        records.append({
            "Latitude":  round(lat, 6),
            "Longitude": round(lon, 6),
            "Type":      tags.get("landuse", "commercial"),
            "Name":      tags.get("name", tags.get("name:en", "")),
        })

    df = pd.DataFrame(records)

    # Final bbox clip (guard against any edge elements)
    df = df[
        (df["Latitude"]  >= LAHORE_BBOX["south"]) &
        (df["Latitude"]  <= LAHORE_BBOX["north"]) &
        (df["Longitude"] >= LAHORE_BBOX["west"])  &
        (df["Longitude"] <= LAHORE_BBOX["east"])
    ].reset_index(drop=True)

    return df
def task2_commercial():
    print("\n" + "="*60)
    print("TASK 2 — Lahore Commercial Area Network")
    print("="*60)

    df = fetch_osm_commercial()

    out_path = os.path.join(OUTPUT_DIR, "lahore_commercial.csv")
    df.to_csv(out_path, index=False)

    print(f"[Task 2] ✓ Saved {len(df):,} rows → {out_path}")
    print(df.head())


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    task1_population()
    task2_commercial()

    print("\n" + "="*60)
    print("All done!  Files saved:")
    print("  • lahore_population.csv")
    print("  • lahore_commercial.csv")
    print("="*60)
