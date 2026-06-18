"""
build_feature_matrix.py
========================
Builds a unified H3-indexed feature matrix for XGBoost-based commercial
site selection (PU Learning) in Lahore, Pakistan.

OUTPUT
------
training_data.csv  — one row per H3 hex cell (resolution 9), columns:
    h3_index | population_in_hex | avg_rent_in_hex | dist_to_nearest_road
    | commercial_density_1km | education_density_1km | parking_density_1km
    | lifestyle_density_1km | luxury_density_1km | is_hotspot

INSTALL (run once)
------------------
pip install h3 pandas numpy scipy scikit-learn
"""

import os
import math
import warnings
import numpy as np
import pandas as pd
import h3
from scipy.spatial import cKDTree

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DATA_DIR   = "."          # folder containing all 10 CSVs
OUTPUT_CSV = "training_data.csv"
H3_RES     = 9            # ~174 m edge length
RADIUS_M   = 1_000        # 1 km radius for density features

# Lahore bounding box (WGS-84)
LAHORE_BBOX = dict(south=31.41, west=74.01, north=31.65, east=74.47)

# ── CSV file paths & their lat/lon column names ──────────────────
FILES = {
    "cafes":       ("lahore_cafes_restaurants.csv",   "Latitude", "Longitude"),
    "commercial":  ("lahore_commercial.csv",           "Latitude", "Longitude"),
    "population":  ("lahore_population.csv",           "Latitude", "Longitude"),
    "education":   ("lahore_education.csv",            "Latitude", "Longitude"),
    "parking":     ("lahore_parking.csv",              "Latitude", "Longitude"),
    "roads":       ("lahore_road_nodes.csv",           "Latitude", "Longitude"),
    "luxury":      ("lahore_luxuries_amenities.csv",   "Latitude", "Longitude"),
    "lifestyle":   ("lahore_lifestyle_combined.csv",   "Latitude", "Longitude"),
    "graana":      ("lahore_graana_clean.csv",         "Latitude", "Longitude"),
    "rent":        ("lahore_rent_final.csv",           "Latitude", "Longitude"),
}

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def load_csv(key: str) -> pd.DataFrame:
    """Load a CSV, normalise lat/lon column names, drop bad rows."""
    fname, lat_col, lon_col = FILES[key]
    path = os.path.join(DATA_DIR, fname)
    if not os.path.exists(path):
        print(f"  [WARN] {fname} not found — feature will be zero/NaN.")
        return pd.DataFrame(columns=["lat", "lon"])

    df = pd.read_csv(path)

    # ── Flexible column detection ─────────────────────────────────
    # Accepts any case variant of Latitude / Longitude / Lat / Lon / Y / X
    col_map = {}
    for col in df.columns:
        cl = col.strip().lower()
        if cl in ("latitude", "lat", "y") and "lat" not in col_map:
            col_map["lat"] = col
        if cl in ("longitude", "lon", "long", "lng", "x") and "lon" not in col_map:
            col_map["lon"] = col

    if "lat" not in col_map or "lon" not in col_map:
        print(f"  [WARN] {fname} — cannot find lat/lon columns (found: {list(df.columns)}). Skipping.")
        return pd.DataFrame(columns=["lat", "lon"])

    df = df.rename(columns={col_map["lat"]: "lat", col_map["lon"]: "lon"})
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])

    # Clip to Lahore bbox
    df = df[
        (df["lat"] >= LAHORE_BBOX["south"]) & (df["lat"] <= LAHORE_BBOX["north"]) &
        (df["lon"] >= LAHORE_BBOX["west"])  & (df["lon"] <= LAHORE_BBOX["east"])
    ].reset_index(drop=True)

    print(f"  Loaded {fname:45s}  →  {len(df):,} rows")
    return df


def latlon_to_h3(lat: float, lon: float, res: int = H3_RES) -> str:
    return h3.latlng_to_cell(lat, lon, res)


def h3_center(h3_idx: str):
    """Return (lat, lon) of H3 cell center."""
    return h3.cell_to_latlng(h3_idx)   # returns (lat, lon)


def haversine_m(lat1, lon1, lat2, lon2):
    """Haversine distance in metres between two points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def build_kd_tree(df: pd.DataFrame):
    """
    Build a cKDTree from lat/lon columns.

    IMPORTANT: cKDTree works in Euclidean space so we convert degrees to
    approximate metres using a local equirectangular projection centred on
    Lahore (31.53°N).  This is accurate to <0.5% over a ~50 km city bbox.
    """
    if df.empty:
        return None, None
    LAT0 = math.radians(31.53)
    R    = 6_371_000
    x = R * np.radians(df["lon"].values) * math.cos(LAT0)
    y = R * np.radians(df["lat"].values)
    coords = np.column_stack([x, y])
    return cKDTree(coords), coords


def project_point(lat: float, lon: float):
    """Project a single (lat, lon) to the same local metre space."""
    LAT0 = math.radians(31.53)
    R    = 6_371_000
    x = R * math.radians(lon) * math.cos(LAT0)
    y = R * math.radians(lat)
    return np.array([[x, y]])


def density_in_radius(tree, center_xy, radius_m: float) -> int:
    """Count points in tree within radius_m of center_xy."""
    if tree is None:
        return 0
    return len(tree.query_ball_point(center_xy[0], radius_m))


# ─────────────────────────────────────────────
#  STEP 0 — LOAD ALL DATA
# ─────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 0 — Loading CSVs")
print("="*60)

df_cafes      = load_csv("cafes")
df_commercial = load_csv("commercial")
df_population = load_csv("population")
df_education  = load_csv("education")
df_parking    = load_csv("parking")
df_roads      = load_csv("roads")
df_luxury     = load_csv("luxury")
df_lifestyle  = load_csv("lifestyle")
df_graana     = load_csv("graana")
df_rent       = load_csv("rent")


# ─────────────────────────────────────────────
#  STEP 1 — BUILD H3 GRID
# ─────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 1 — Building H3 grid (resolution 9)")
print("="*60)

# Seed the grid from ALL data sources so every data point
# belongs to a real hex in the grid.
all_coords = pd.concat(
    [df[["lat", "lon"]] for df in [
        df_cafes, df_commercial, df_population, df_education,
        df_parking, df_roads, df_luxury, df_lifestyle, df_rent
    ] if not df.empty],
    ignore_index=True,
)

all_coords["h3_index"] = all_coords.apply(
    lambda r: latlon_to_h3(r["lat"], r["lon"]), axis=1
)
hex_set = set(all_coords["h3_index"].unique())
print(f"  Total unique H3 hexes: {len(hex_set):,}")

# Build master grid DataFrame
grid = pd.DataFrame({"h3_index": sorted(hex_set)})

# Pre-compute hex center coordinates (vectorised)
centers = [h3_center(h) for h in grid["h3_index"]]
grid["_lat"] = [c[0] for c in centers]
grid["_lon"] = [c[1] for c in centers]

print(f"  Grid built: {len(grid):,} hexes")


# ─────────────────────────────────────────────
#  STEP 2 — GROUND TRUTH (PU Learning target)
# ─────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 2 — Labelling hotspots (PU Learning)")
print("="*60)

if not df_cafes.empty:
    cafe_hexes = set(
        df_cafes.apply(lambda r: latlon_to_h3(r["lat"], r["lon"]), axis=1)
    )
else:
    cafe_hexes = set()
    print("  [WARN] No cafe data — all labels will be 0.")

grid["is_hotspot"] = grid["h3_index"].isin(cafe_hexes).astype(int)
print(f"  Positive hexes (is_hotspot=1): {grid['is_hotspot'].sum():,}")
print(f"  Unlabelled hexes (is_hotspot=0): {(grid['is_hotspot']==0).sum():,}")


# ─────────────────────────────────────────────
#  STEP 3 — FEATURE ENGINEERING
# ─────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 3 — Engineering features")
print("="*60)

# ── 3a. Population in hex ─────────────────────────────────────────
print("  [3a] population_in_hex …")
if not df_population.empty:
    # Assign each population point to a hex, sum within hex
    df_population["h3_index"] = df_population.apply(
        lambda r: latlon_to_h3(r["lat"], r["lon"]), axis=1
    )
    pop_col = next(
        (c for c in df_population.columns if "pop" in c.lower() and c not in ("lat","lon","h3_index")),
        None
    )
    if pop_col:
        pop_agg = df_population.groupby("h3_index")[pop_col].sum().reset_index()
        pop_agg.columns = ["h3_index", "population_in_hex"]
    else:
        # No numeric population column — count points as proxy
        pop_agg = df_population.groupby("h3_index").size().reset_index(name="population_in_hex")
    grid = grid.merge(pop_agg, on="h3_index", how="left")
    grid["population_in_hex"] = grid["population_in_hex"].fillna(0)
else:
    grid["population_in_hex"] = 0

# ── 3b. Average rent in hex (with nearest-neighbour imputation) ───
print("  [3b] avg_rent_in_hex (with KD-Tree imputation) …")
if not df_rent.empty:
    # Detect rent column: first numeric column that isn't lat/lon
    rent_col = next(
        (c for c in df_rent.columns
         if c not in ("lat", "lon", "h3_index")
         and pd.api.types.is_numeric_dtype(df_rent[c])),
        None
    )
    if rent_col is None:
        # Try columns with 'rent', 'price', 'value' in name
        for c in df_rent.columns:
            if any(k in c.lower() for k in ("rent", "price", "value", "cost")):
                rent_col = c
                break

    if rent_col:
        df_rent["h3_index"] = df_rent.apply(
            lambda r: latlon_to_h3(r["lat"], r["lon"]), axis=1
        )
        df_rent[rent_col] = pd.to_numeric(df_rent[rent_col], errors="coerce")
        rent_agg = (
            df_rent.dropna(subset=[rent_col])
                   .groupby("h3_index")[rent_col]
                   .mean()
                   .reset_index()
        )
        rent_agg.columns = ["h3_index", "avg_rent_in_hex"]
        grid = grid.merge(rent_agg, on="h3_index", how="left")

        # KD-Tree nearest-neighbour imputation for hexes with no rent data
        has_rent = grid["avg_rent_in_hex"].notna()
        if has_rent.sum() > 0 and (~has_rent).sum() > 0:
            print(f"       Imputing rent for {(~has_rent).sum():,} hexes via nearest neighbour …")
            src = grid[has_rent][["_lat", "_lon", "avg_rent_in_hex"]].reset_index(drop=True)
            tgt = grid[~has_rent][["_lat", "_lon"]].reset_index(drop=True)

            tree_rent, _ = build_kd_tree(src.rename(columns={"_lat":"lat","_lon":"lon"}))
            tgt_xy = np.column_stack([
                project_point(r["_lat"], r["_lon"])[0]
                for _, r in tgt.iterrows()
            ]).T

            # Query each target point for its nearest source
            LAT0 = math.radians(31.53); R = 6_371_000
            tgt_x = R * np.radians(tgt["_lon"].values) * math.cos(LAT0)
            tgt_y = R * np.radians(tgt["_lat"].values)
            tgt_coords = np.column_stack([tgt_x, tgt_y])
            _, nn_idx = tree_rent.query(tgt_coords, k=1)
            imputed_rents = src["avg_rent_in_hex"].values[nn_idx]
            grid.loc[~has_rent, "avg_rent_in_hex"] = imputed_rents
    else:
        print("       [WARN] No rent value column found — feature will be 0.")
        grid["avg_rent_in_hex"] = 0.0
else:
    grid["avg_rent_in_hex"] = 0.0

grid["avg_rent_in_hex"] = grid["avg_rent_in_hex"].fillna(0.0)

# ── 3c. Distance to nearest road ─────────────────────────────────
print("  [3c] dist_to_nearest_road …")
if not df_roads.empty:
    tree_roads, _ = build_kd_tree(df_roads)
    LAT0 = math.radians(31.53); R_earth = 6_371_000

    grid_x = R_earth * np.radians(grid["_lon"].values) * math.cos(LAT0)
    grid_y = R_earth * np.radians(grid["_lat"].values)
    grid_coords = np.column_stack([grid_x, grid_y])

    dists, _ = tree_roads.query(grid_coords, k=1)
    grid["dist_to_nearest_road"] = np.round(dists, 2)
else:
    grid["dist_to_nearest_road"] = np.nan

# ── 3d. Density features (count within 1 km radius) ──────────────
density_sources = {
    "commercial_density_1km": df_commercial,
    "education_density_1km":  df_education,
    "parking_density_1km":    df_parking,
    "lifestyle_density_1km":  df_lifestyle,
    "luxury_density_1km":     df_luxury,
}

# Pre-project all hex centres once (reused for all density queries)
LAT0 = math.radians(31.53); R_earth = 6_371_000
grid_x = R_earth * np.radians(grid["_lon"].values) * math.cos(LAT0)
grid_y = R_earth * np.radians(grid["_lat"].values)
grid_xy = np.column_stack([grid_x, grid_y])

for feat_name, df_src in density_sources.items():
    print(f"  [3d] {feat_name} …")
    if df_src.empty:
        grid[feat_name] = 0
        continue

    tree, _ = build_kd_tree(df_src)
    counts = np.array([
        len(tree.query_ball_point(pt, RADIUS_M))
        for pt in grid_xy
    ])
    grid[feat_name] = counts

print(f"\n  Feature engineering complete. Grid shape: {grid.shape}")


# ─────────────────────────────────────────────
#  STEP 4 — ASSEMBLE & SAVE
# ─────────────────────────────────────────────
print("\n" + "="*60)
print("STEP 4 — Assembling training_data.csv")
print("="*60)

FEATURE_COLS = [
    "population_in_hex",
    "avg_rent_in_hex",
    "dist_to_nearest_road",
    "commercial_density_1km",
    "education_density_1km",
    "parking_density_1km",
    "lifestyle_density_1km",
    "luxury_density_1km",
]

output = grid[["h3_index"] + FEATURE_COLS + ["is_hotspot"]].copy()

# Final type coercions
for col in FEATURE_COLS:
    output[col] = pd.to_numeric(output[col], errors="coerce").fillna(0)

output.to_csv(OUTPUT_CSV, index=False)

print(f"\n  Saved → {OUTPUT_CSV}")
print(f"  Shape : {output.shape[0]:,} rows × {output.shape[1]} columns")
print(f"  Positive labels (is_hotspot=1) : {output['is_hotspot'].sum():,}")
print(f"  Unlabelled      (is_hotspot=0) : {(output['is_hotspot']==0).sum():,}")
print(f"\n  Column summary:")
print(output[FEATURE_COLS].describe().T.to_string())
print("\n  First 5 rows:")
print(output.head().to_string(index=False))
print("\n" + "="*60)
print("Done. training_data.csv is ready for XGBoost.")
print("="*60)
