import osmnx as ox
import geopandas as gpd
import pandas as pd
import time
import os
from utils.logging import get_logger

# -----------------------------
# LOGGER & CONFIG
# -----------------------------
logger = get_logger(__name__)

BBOX = (74.200, 31.300, 74.550, 31.700) 
CRS_PROJECTED = "EPSG:3857"
CRS_WGS84 = "EPSG:4326"

# Directory management
OUTPUT_DIR = "data"
EDU_FILE = os.path.join(OUTPUT_DIR, "lahore_education.csv")
PARKING_FILE = os.path.join(OUTPUT_DIR, "lahore_parking.csv")

ox.settings.use_cache = True
ox.settings.timeout = 180

# -----------------------------
# HELPERS
# -----------------------------
def safe_centroid(gdf):
    gdf_proj = gdf.to_crs(CRS_PROJECTED)
    gdf["geometry"] = gdf_proj.centroid.to_crs(CRS_WGS84)
    return gdf

def fetch_layer(tags, name):
    logger.info(f"Fetching {name} from Overpass...")
    try:
        gdf = ox.features_from_bbox(bbox=BBOX, tags=tags)
        if gdf.empty:
            return None
        
        # Name handling
        if "name" not in gdf.columns:
            gdf["name"] = f"Unnamed {name}"
        else:
            gdf["name"] = gdf["name"].fillna(f"Unnamed {name}")
            
        return gdf[["name", "geometry"]]
    except Exception as e:
        logger.error(f"Failed to fetch {name}: {e}")
        return None

# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run():
    # 1. Ensure directory exists before doing anything
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    logger.info("STEP 1A: Starting POI Extraction for Lahore")

    layers = [
        ("university", {"amenity": "university"}),
        ("college", {"amenity": "college"}),
        ("parking", {"amenity": "parking"})
    ]

    all_data = []

    for layer_name, tags in layers:
        gdf = fetch_layer(tags, layer_name)

        if gdf is not None:
            gdf = safe_centroid(gdf)
            gdf["lat"] = gdf.geometry.y
            gdf["lon"] = gdf.geometry.x
            gdf["layer"] = layer_name

            df = pd.DataFrame(gdf.drop(columns="geometry"))
            all_data.append(df)
            logger.info(f"Collected {len(df)} items for {layer_name}")
            time.sleep(2)
        else:
            logger.warning(f"No data found for {layer_name}")

    if not all_data:
        logger.error("No data collected.")
        return

    # Combine everything into one master dataframe first
    full_df = pd.concat(all_data, ignore_index=True)

    # 2. Deduplication logic
    full_df["lat_round"] = full_df["lat"].round(4)
    full_df["lon_round"] = full_df["lon"].round(4)
    full_df = full_df.drop_duplicates(subset=["lat_round", "lon_round"])
    full_df = full_df.drop(columns=["lat_round", "lon_round"])

    # 3. SPLIT DATA
    # Group University + College together
    edu_df = full_df[full_df["layer"].isin(["university", "college"])]
    
    # Group Parking separately
    parking_df = full_df[full_df["layer"] == "parking"]

    # 4. SAVE FILES
    if not edu_df.empty:
        edu_df.to_csv(EDU_FILE, index=False)
        logger.info(f"Saved {len(edu_df)} Education POIs to {EDU_FILE}")
    
    if not parking_df.empty:
        parking_df.to_csv(PARKING_FILE, index=False)
        logger.info(f"Saved {len(parking_df)} Parking POIs to {PARKING_FILE}")

    print(f"\n✨ Split complete:")
    print(f"   - Education (Uni/College): {len(edu_df)} sites")
    print(f"   - Parking: {len(parking_df)} sites")

if __name__ == "__main__":
    run()