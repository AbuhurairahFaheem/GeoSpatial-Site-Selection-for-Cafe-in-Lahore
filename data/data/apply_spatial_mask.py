"""
apply_spatial_mask.py
=====================
Phase 2 of the Final Evaluation Pipeline — GIS Spatial Masking.

Takes the top-200 XGBoost-scored hexagons from predictions.csv and
vetoes any that overlap with unbuildable zones (rivers, graveyards,
military areas, etc.) by more than OVERLAP_THRESHOLD (default 10%).

Input files
-----------
  outputs/predictions.csv          — from evaluate_models.py
  data/unbuildable_zones.geojson   — you supply this (see notes below)

Output files
------------
  outputs/masked_predictions.csv   — top-200 hexes, each with:
                                       status    : 'Approved' | 'Vetoed by GIS Mask'
                                       veto_reason : name/type of the zone that caused veto
                                       overlap_pct : actual overlap percentage (0–100)
  outputs/plots/spatial_mask_map.png — static overview map

How to obtain unbuildable_zones.geojson for Lahore
----------------------------------------------------
Option A — OSM via overpassql (recommended, free):
  Run this query on https://overpass-turbo.eu/ then export as GeoJSON:

    [out:json][timeout:120];
    (
      way["natural"="water"](31.41,74.01,31.65,74.47);
      relation["natural"="water"](31.41,74.01,31.65,74.47);
      way["landuse"="cemetery"](31.41,74.01,31.65,74.47);
      way["landuse"="military"](31.41,74.01,31.65,74.47);
      way["amenity"="grave_yard"](31.41,74.01,31.65,74.47);
    );
    out body; >; out skel qt;

Option B — QGIS + OSM QuickOSM plugin:
  Query the same tags and export as GeoJSON.

Install
-------
  Windows (conda — avoids GDAL issues):
    conda install -c conda-forge geopandas
    pip install h3 pandas shapely matplotlib

  Windows (pip-only):
    pip install geopandas h3 pandas shapely matplotlib
    (If you see GDAL errors: pip install gdal and retry)

Usage
-----
  python apply_spatial_mask.py
  python apply_spatial_mask.py --top-n 300 --threshold 0.05
  python apply_spatial_mask.py --zones data/my_zones.geojson --no-plot
"""

import os
import json
import argparse
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import h3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from shapely.geometry import Polygon, mapping
from shapely.ops import unary_union

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────
PREDICTIONS_PATH  = os.path.join("outputs", "predictions.csv")
ZONES_PATH        = os.path.join("data",    "unbuildable_zones.geojson")
OUTPUT_CSV        = os.path.join("outputs", "masked_predictions.csv")
PLOT_PATH         = os.path.join("outputs", "plots", "spatial_mask_map.png")

DEFAULT_TOP_N     = 200    # how many top-ranked hexes to process
DEFAULT_THRESHOLD = 0.10   # veto if overlap fraction exceeds this (10%)

# H3 resolution used when building the feature matrix
H3_RESOLUTION = 9

# Coordinate system constants
# All H3 geometry and GeoJSON input are in WGS-84 (EPSG:4326).
# For area-based overlap we project to UTM Zone 42N (EPSG:32642)
# which covers Lahore in metres — giving accurate area calculations.
WGS84_EPSG  = 4326
METRE_EPSG  = 32642   # UTM Zone 42N — accurate for Lahore


# ─────────────────────────────────────────────────────────────────
#  ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="GIS spatial mask — veto H3 hexes overlapping unbuildable zones."
    )
    p.add_argument("--predictions", default=PREDICTIONS_PATH,
                   help="Path to predictions.csv (from evaluate_models.py)")
    p.add_argument("--zones",       default=ZONES_PATH,
                   help="Path to unbuildable_zones.geojson")
    p.add_argument("--output",      default=OUTPUT_CSV,
                   help="Output path for masked_predictions.csv")
    p.add_argument("--top-n",       type=int,   default=DEFAULT_TOP_N,
                   help="Number of top-ranked hexes to process (default: 200)")
    p.add_argument("--threshold",   type=float, default=DEFAULT_THRESHOLD,
                   help="Overlap fraction that triggers a veto (default: 0.10 = 10%%)")
    p.add_argument("--no-plot",     action="store_true",
                   help="Skip generating the map PNG")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────
#  STEP 1 — Load predictions and select top-N hexes
# ─────────────────────────────────────────────────────────────────
def load_top_hexes(predictions_path: str, top_n: int) -> pd.DataFrame:
    print(f"\n{'='*60}")
    print(f"STEP 1 — Loading predictions, selecting top {top_n} hexes")
    print(f"{'='*60}")

    if not os.path.exists(predictions_path):
        raise FileNotFoundError(
            f"predictions.csv not found at '{predictions_path}'.\n"
            "Run evaluate_models.py first."
        )

    df = pd.read_csv(predictions_path)
    print(f"  Loaded {len(df):,} total hexes from '{predictions_path}'")

    # Validate required columns
    required = ["h3_index", "xgb_hotspot_probability"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing columns in predictions.csv: {missing}\n"
            f"Found: {list(df.columns)}"
        )

    # Sort by probability descending, take top N
    df_sorted = df.sort_values("xgb_hotspot_probability", ascending=False).reset_index(drop=True)
    top = df_sorted.head(top_n).copy()
    top["original_rank"] = top.index + 1   # 1-based rank before masking

    print(f"  Selected top {len(top):,} hexes")
    print(f"  Probability range: {top['xgb_hotspot_probability'].min():.4f} – "
          f"{top['xgb_hotspot_probability'].max():.4f}")

    return top


# ─────────────────────────────────────────────────────────────────
#  STEP 2 — Convert H3 indices to Shapely polygons
# ─────────────────────────────────────────────────────────────────
def h3_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert each h3_index string into a Shapely Polygon by retrieving
    the boundary vertex coordinates from the h3-py library.

    h3.cell_to_boundary() returns a list of (lat, lon) tuples.
    Shapely expects (lon, lat) / (x, y), so we swap the order.
    """
    print(f"\n{'='*60}")
    print("STEP 2 — Converting H3 indices to hexagon polygons")
    print(f"{'='*60}")

    def h3_to_polygon(h3_idx: str) -> Polygon:
        # Returns list of (lat, lon) — swap to (lon, lat) for Shapely
        boundary = h3.cell_to_boundary(h3_idx)   # [(lat,lon), ...]
        coords   = [(lon, lat) for lat, lon in boundary]
        return Polygon(coords)

    polygons = [h3_to_polygon(idx) for idx in df["h3_index"]]

    gdf = gpd.GeoDataFrame(df.copy(), geometry=polygons, crs=f"EPSG:{WGS84_EPSG}")

    print(f"  Created {len(gdf):,} hexagon polygons in EPSG:{WGS84_EPSG}")
    print(f"  Approximate hex area: ~0.105 km² each (H3 Resolution {H3_RESOLUTION})")

    return gdf


# ─────────────────────────────────────────────────────────────────
#  STEP 3 — Load and prepare unbuildable zones
# ─────────────────────────────────────────────────────────────────
def load_unbuildable_zones(zones_path: str) -> gpd.GeoDataFrame:
    """
    Load the unbuildable zones GeoJSON.

    If the file does not exist yet, we generate a PLACEHOLDER with
    three synthetic polygons (a river strip, a graveyard, a military
    zone) so the script can be tested immediately without real data.
    The placeholder is written to disk so you can inspect/replace it.
    """
    print(f"\n{'='*60}")
    print("STEP 3 — Loading unbuildable zones")
    print(f"{'='*60}")

    if not os.path.exists(zones_path):
        print(f"  [WARN] '{zones_path}' not found.")
        print("  Generating a PLACEHOLDER GeoJSON for testing …")
        print("  Replace this file with real OSM data before final submission.")
        _generate_placeholder_zones(zones_path)

    zones = gpd.read_file(zones_path)
    print(f"  Loaded {len(zones):,} unbuildable zone polygons")

    # Ensure WGS-84
    if zones.crs is None:
        print("  [WARN] No CRS on zones file — assuming EPSG:4326")
        zones = zones.set_crs(WGS84_EPSG)
    elif zones.crs.to_epsg() != WGS84_EPSG:
        zones = zones.to_crs(WGS84_EPSG)
        print(f"  Reprojected zones to EPSG:{WGS84_EPSG}")

    # Show zone type breakdown if a 'type' or 'landuse' column exists
    type_col = next(
        (c for c in zones.columns if c.lower() in ("type", "landuse", "natural", "amenity")),
        None
    )
    if type_col:
        print(f"  Zone types ({type_col}):")
        for val, cnt in zones[type_col].value_counts().items():
            print(f"    {val}: {cnt}")

    # Dissolve all zones into a single MultiPolygon for fast intersection
    # (one spatial operation instead of N per-zone intersections)
    zones_union = unary_union(zones.geometry)
    print(f"  Dissolved into single geometry for vectorised intersection")

    return zones, zones_union


def _generate_placeholder_zones(output_path: str):
    """
    Write a minimal placeholder GeoJSON over Lahore so the script
    runs out of the box even without real data.

    Covers three synthetic 'unbuildable' areas:
      1. Ravi River corridor (thin horizontal strip)
      2. A graveyard polygon near Model Town
      3. A military zone near Walton
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    features = [
        {
            "type": "Feature",
            "properties": {"name": "Ravi River (Placeholder)", "type": "water"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [74.01,  31.63], [74.47,  31.63],
                    [74.47,  31.645],[74.01,  31.645],
                    [74.01,  31.63],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"name": "Model Town Graveyard (Placeholder)", "type": "cemetery"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [74.32, 31.47], [74.335, 31.47],
                    [74.335, 31.482],[74.32,  31.482],
                    [74.32, 31.47],
                ]]
            }
        },
        {
            "type": "Feature",
            "properties": {"name": "Walton Military Zone (Placeholder)", "type": "military"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [74.36, 31.50], [74.39, 31.50],
                    [74.39, 31.52], [74.36, 31.52],
                    [74.36, 31.50],
                ]]
            }
        },
    ]

    geojson = {"type": "FeatureCollection", "features": features}
    with open(output_path, "w") as f:
        json.dump(geojson, f, indent=2)

    print(f"  Placeholder written → {output_path}")
    print("  !! Replace with real OSM data before final submission !!")


# ─────────────────────────────────────────────────────────────────
#  STEP 4 — Spatial intersection and overlap calculation
# ─────────────────────────────────────────────────────────────────
def apply_spatial_mask(
    gdf_hexes: gpd.GeoDataFrame,
    zones_gdf: gpd.GeoDataFrame,
    zones_union,
    threshold: float,
) -> gpd.GeoDataFrame:
    """
    For each of the top-N hexagons:
      1. Check if it intersects the dissolved unbuildable zones.
      2. If yes, compute overlap area as a fraction of hex area.
      3. If overlap > threshold → veto the hex.

    Area calculations are done in EPSG:32642 (metres²) for accuracy.
    Doing this in WGS-84 degrees would give wrong results because
    one degree of longitude ≠ one degree of latitude in area terms.

    Returns the GeoDataFrame with new columns:
      overlap_pct   : 0.0–100.0 (% of hex area covered by a zone)
      status        : 'Approved' | 'Vetoed by GIS Mask'
      veto_reason   : name/type of offending zone, or '' if approved
    """
    print(f"\n{'='*60}")
    print(f"STEP 4 — Spatial intersection (threshold = {threshold:.0%})")
    print(f"{'='*60}")

    # Project to metre-based CRS for accurate area arithmetic
    gdf_m    = gdf_hexes.to_crs(METRE_EPSG)
    zones_m  = zones_gdf.to_crs(METRE_EPSG)

    # Pre-build spatial index on zones for fast per-hex candidate lookup
    zones_sindex = zones_m.sindex

    overlap_pcts  = []
    statuses      = []
    veto_reasons  = []

    n_vetoed = 0

    for idx, row in gdf_m.iterrows():
        hex_geom = row.geometry
        hex_area = hex_geom.area   # m²

        # Fast bounding-box pre-filter via spatial index
        candidate_idxs = list(zones_sindex.intersection(hex_geom.bounds))

        if not candidate_idxs:
            # No candidate zones anywhere near this hex → guaranteed approved
            overlap_pcts.append(0.0)
            statuses.append("Approved")
            veto_reasons.append("")
            continue

        # Compute actual intersection with candidate zones only
        candidates = zones_m.iloc[candidate_idxs]

        # Find which zone has the largest overlap with this hex
        best_overlap_frac = 0.0
        best_zone_name    = ""

        for _, zone_row in candidates.iterrows():
            if not hex_geom.intersects(zone_row.geometry):
                continue
            intersection = hex_geom.intersection(zone_row.geometry)
            overlap_frac = intersection.area / max(hex_area, 1.0)

            if overlap_frac > best_overlap_frac:
                best_overlap_frac = overlap_frac
                # Try to get a meaningful zone name for the veto reason
                for name_col in ("name", "type", "landuse", "natural", "amenity"):
                    if name_col in zone_row.index and zone_row[name_col]:
                        best_zone_name = str(zone_row[name_col])
                        break
                if not best_zone_name:
                    best_zone_name = "Unbuildable Zone"

        overlap_pct = best_overlap_frac * 100.0
        overlap_pcts.append(round(overlap_pct, 2))

        if best_overlap_frac > threshold:
            statuses.append("Vetoed by GIS Mask")
            veto_reasons.append(best_zone_name)
            n_vetoed += 1
        else:
            statuses.append("Approved")
            veto_reasons.append("")

    gdf_hexes = gdf_hexes.copy()
    gdf_hexes["overlap_pct"] = overlap_pcts
    gdf_hexes["status"]      = statuses
    gdf_hexes["veto_reason"] = veto_reasons

    # Zero out probability for vetoed hexes as required
    veto_mask = gdf_hexes["status"] == "Vetoed by GIS Mask"
    gdf_hexes.loc[veto_mask, "xgb_hotspot_probability"] = 0.0

    n_approved = len(gdf_hexes) - n_vetoed
    print(f"  Approved  : {n_approved:,}")
    print(f"  Vetoed    : {n_vetoed:,}  (overlap > {threshold:.0%})")

    if n_vetoed > 0:
        print("\n  Vetoed hexes:")
        vetoed = gdf_hexes[gdf_hexes["status"] == "Vetoed by GIS Mask"][
            ["h3_index", "original_rank", "overlap_pct", "veto_reason"]
        ]
        print(vetoed.to_string(index=False))

    return gdf_hexes


# ─────────────────────────────────────────────────────────────────
#  STEP 5 — Re-rank approved hexes and save
# ─────────────────────────────────────────────────────────────────
def save_results(gdf: gpd.GeoDataFrame, output_path: str) -> pd.DataFrame:
    print(f"\n{'='*60}")
    print("STEP 5 — Finalising and saving results")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    # Assign final_rank: approved hexes ranked by original probability,
    # vetoed hexes pushed to the end and not numbered
    approved = gdf[gdf["status"] == "Approved"].copy()
    approved["final_rank"] = (
        approved["xgb_hotspot_probability"]
        .rank(ascending=False, method="first")
        .astype(int)
    )

    vetoed = gdf[gdf["status"] == "Vetoed by GIS Mask"].copy()
    vetoed["final_rank"] = -1   # sentinel: not ranked

    result = pd.concat([approved, vetoed]).sort_values(
        ["status", "final_rank"]
    ).reset_index(drop=True)

    # Drop shapely geometry column — not needed in CSV
    out_df = result.drop(columns=["geometry"], errors="ignore")

    out_df.to_csv(output_path, index=False)
    print(f"  Saved → {output_path}  ({len(out_df):,} rows)")

    approved_count = (out_df["status"] == "Approved").sum()
    print(f"\n  Top 5 APPROVED recommended sites after GIS masking:")
    top5 = out_df[out_df["status"] == "Approved"].nsmallest(5, "final_rank")[
        ["h3_index", "xgb_hotspot_probability", "final_rank", "overlap_pct"]
    ]
    print(top5.to_string(index=False))

    return out_df


# ─────────────────────────────────────────────────────────────────
#  STEP 6 — Static overview map
# ─────────────────────────────────────────────────────────────────
def generate_map(
    gdf: gpd.GeoDataFrame,
    zones_gdf: gpd.GeoDataFrame,
    plot_path: str,
    threshold: float,
):
    """
    Render a static Matplotlib map showing:
      - Approved hexes (blue, opacity by probability)
      - Vetoed hexes   (red, hatched)
      - Unbuildable zones (grey fill)
    """
    print(f"\n{'='*60}")
    print("STEP 6 — Generating spatial mask overview map")
    print(f"{'='*60}")

    os.makedirs(os.path.dirname(plot_path), exist_ok=True)

    fig, ax = plt.subplots(1, 1, figsize=(11, 9), facecolor="#F0F4F8")
    ax.set_facecolor("#E8F0F8")

    # ── Unbuildable zones (grey fill) ────────────────────────────
    if not zones_gdf.empty:
        zones_gdf.plot(
            ax=ax, color="#B0B0B0", edgecolor="#777777",
            linewidth=0.8, alpha=0.55, zorder=2,
        )

    # ── Approved hexes (blue spectrum by probability) ─────────────
    approved = gdf[gdf["status"] == "Approved"]
    if not approved.empty:
        approved.plot(
            ax=ax,
            column="xgb_hotspot_probability",
            cmap="Blues",
            edgecolor="#1C3557",
            linewidth=0.5,
            alpha=0.85,
            zorder=3,
            vmin=0,
            vmax=approved["xgb_hotspot_probability"].max(),
        )

    # ── Vetoed hexes (red, hatched) ───────────────────────────────
    vetoed = gdf[gdf["status"] == "Vetoed by GIS Mask"]
    if not vetoed.empty:
        vetoed.plot(
            ax=ax, color="#D94F3D", edgecolor="#8B0000",
            linewidth=0.8, alpha=0.7, zorder=4, hatch="///",
        )

    # ── Top-10 approved: number labels ───────────────────────────
    top10_approved = approved.nsmallest(10, "final_rank") if "final_rank" in approved.columns else approved.head(10)
    for _, row in top10_approved.iterrows():
        centroid = row.geometry.centroid
        rank     = int(row.get("final_rank", "?"))
        ax.text(
            centroid.x, centroid.y, str(rank),
            ha="center", va="center",
            fontsize=7, fontweight="bold", color="white",
            zorder=5,
        )

    # ── Legend ───────────────────────────────────────────────────
    legend_handles = [
        mpatches.Patch(facecolor="#3A7FC1", edgecolor="#1C3557",
                       label="Approved hex (colour = XGB score)"),
        mpatches.Patch(facecolor="#D94F3D", edgecolor="#8B0000",
                       hatch="///", label=f"Vetoed (overlap > {threshold:.0%})"),
        mpatches.Patch(facecolor="#B0B0B0", edgecolor="#777777",
                       label="Unbuildable zone", alpha=0.6),
    ]
    ax.legend(handles=legend_handles, loc="lower left", fontsize=9.5,
              framealpha=0.92, facecolor="white", edgecolor="#CCCCCC")

    ax.set_title(
        f"GIS Spatial Mask — Top {len(gdf)} Hexes After Veto\n"
        f"Approved: {(gdf['status']=='Approved').sum()}  |  "
        f"Vetoed: {(gdf['status']=='Vetoed by GIS Mask').sum()}  |  "
        f"Overlap threshold: {threshold:.0%}",
        fontsize=12, fontweight="bold", color="#1A2A3A", pad=12,
    )
    ax.set_xlabel("Longitude", fontsize=10, color="#444444")
    ax.set_ylabel("Latitude",  fontsize=10, color="#444444")
    ax.tick_params(labelsize=8.5, colors="#555555")

    plt.tight_layout()
    fig.savefig(plot_path, dpi=150, bbox_inches="tight", facecolor="#F0F4F8")
    plt.close(fig)
    print(f"  Map saved → {plot_path}")


# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    print(f"\n{'='*60}")
    print("apply_spatial_mask.py — GIS Post-Processing")
    print(f"  Top-N    : {args.top_n}")
    print(f"  Threshold: {args.threshold:.0%}")
    print(f"  Zones    : {args.zones}")
    print(f"{'='*60}")

    # 1. Load top-N predictions
    top_hexes = load_top_hexes(args.predictions, args.top_n)

    # 2. Convert H3 indices to Shapely polygons
    gdf_hexes = h3_to_geodataframe(top_hexes)

    # 3. Load (or generate placeholder) unbuildable zones
    zones_gdf, zones_union = load_unbuildable_zones(args.zones)

    # 4. Spatial intersection + veto logic
    gdf_masked = apply_spatial_mask(
        gdf_hexes, zones_gdf, zones_union, args.threshold
    )

    # 5. Save masked_predictions.csv
    out_df = save_results(gdf_masked, args.output)

    # 6. Static map
    if not args.no_plot:
        generate_map(gdf_masked, zones_gdf, PLOT_PATH, args.threshold)

    # ── Final summary ─────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("MASKING COMPLETE")
    print(f"{'='*60}")
    n_approved = (out_df["status"] == "Approved").sum()
    n_vetoed   = (out_df["status"] == "Vetoed by GIS Mask").sum()
    print(f"  Input hexes processed : {len(out_df):,}")
    print(f"  Approved              : {n_approved:,}")
    print(f"  Vetoed by GIS mask    : {n_vetoed:,}")
    print(f"  Overlap threshold     : {args.threshold:.0%}")
    print(f"\n  Output files:")
    print(f"    {args.output}")
    if not args.no_plot:
        print(f"    {PLOT_PATH}")
    print(f"\n  Next step → python generate_final_map.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
