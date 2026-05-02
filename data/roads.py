# steps/step1c_roads.py

import osmnx as ox
import geopandas as gpd
import pandas as pd
import numpy as np
import time
from tqdm import tqdm  # <--- New Import
from utils.logging import get_logger

# -----------------------------
# LOGGER
# -----------------------------
logger = get_logger(__name__)

# -----------------------------
# CONFIG
# -----------------------------
CRS_WGS84 = "EPSG:4326"
CRS_PROJECTED = "EPSG:3857"

OUTPUT_PATH = "data/lahore_roads.gpkg"

# ✅ VERIFIED LAHORE BBOX (tight + safe)
NORTH = 31.700
SOUTH = 31.300
EAST  = 74.550
WEST  = 74.200


# -----------------------------
# OSMNX SETTINGS (IMPORTANT)
# -----------------------------
ox.settings.use_cache = True
ox.settings.cache_folder = "cache"
ox.settings.timeout = 180


# -----------------------------
# HELPERS
# -----------------------------
def normalize_highway(val):
    if isinstance(val, list):
        return val[0]
    return val


def validate_edges(df):
    if df.empty:
        raise ValueError("Road dataset is empty")

    if df["geometry"].isnull().sum() > 0:
        raise ValueError("Null geometries found")

    if df["length_m"].min() <= 0:
        raise ValueError("Invalid road lengths")


# -----------------------------
# MAIN
# -----------------------------
def run():
    logger.info("STEP 1C: Road Network Extraction (BBOX mode)")

    # -----------------------------
    # DOWNLOAD GRAPH (With Spinner)
    # -----------------------------
    try:
        # Since ox.graph_from_bbox is one big request, we use a status message
        print("🛰️  Connecting to OpenStreetMap Overpass API... (this may take 1-3 mins)")
        logger.info("Downloading road network using bbox...")

        bbox = (31.700, 31.300, 74.550, 74.200)

        G = ox.graph_from_bbox(
            bbox=bbox,  # Ensure keyword argument for OSMnx v2
            network_type="drive",
            simplify=True,
            retain_all=False
        )

        logger.info(f"Graph loaded: {len(G.nodes)} nodes, {len(G.edges)} edges")

    except Exception:
        logger.exception("Failed to download road network")
        raise

    # -----------------------------
    # CONVERT TO GDFS
    # -----------------------------
    logger.info("Converting graph to GeoDataFrames...")
    nodes, edges = ox.graph_to_gdfs(G, nodes=True, edges=True)
    edges = edges.reset_index()

    # -----------------------------
    # CLEAN DATA (With Progress Bar)
    # -----------------------------
    print("\n🧹 Cleaning and Processing Road Segments...")
    
    # We use tqdm on the apply function to see the row-by-row progress
    tqdm.pandas(desc="Normalizing Highways")
    edges["highway"] = edges["highway"].progress_apply(normalize_highway)

    # -----------------------------
    # PROJECT CRS → CORRECT LENGTH
    # -----------------------------
    print("🌍 Projecting CRS and calculating lengths...")
    edges_proj = edges.to_crs(CRS_PROJECTED)
    
    # Simple geometry operations are fast, but for huge datasets:
    edges_proj["length_m"] = edges_proj.geometry.length
    edges = edges_proj.to_crs(CRS_WGS84)

    # -----------------------------
    # ROAD CLASS FEATURE (Using progress bar for mapping)
    # -----------------------------
    ROAD_CLASS_MAP = {
        "motorway": 5, "trunk": 5, "primary": 4, 
        "secondary": 3, "tertiary": 2, "residential": 1, "service": 0.5,
    }

    print("🏷️  Mapping Road Classes...")
    # tqdm handles series mapping well
    edges["road_class"] = [ROAD_CLASS_MAP.get(h, 1) for h in tqdm(edges["highway"], desc="Classifying")]

    # -----------------------------
    # REMOVE NOISE & DEDUPLICATE
    # -----------------------------
    before = len(edges)
    edges = edges[edges["length_m"] > 5]
    
    edges_slim = edges[["highway", "road_class", "length_m", "geometry"]].copy()
    
    print("✂️  Removing duplicates and tiny segments...")
    edges_slim = edges_slim.drop_duplicates(subset=["geometry"])

    # -----------------------------
    # SAVE (With "Please Wait" notification)
    # -----------------------------
    print(f"💾 Saving {len(edges_slim)} edges to {OUTPUT_PATH}...")
    
    edges_slim.to_file(
        OUTPUT_PATH,
        driver="GPKG",
        engine="pyogrio"
    )

    print("✨ STEP 1C COMPLETE\n")
    logger.info("STEP 1C COMPLETE")

if __name__ == "__main__":
    run()