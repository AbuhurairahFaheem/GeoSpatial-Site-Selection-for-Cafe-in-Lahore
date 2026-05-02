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

# Coordinates for Lahore (West, South, East, North)
BBOX = (74.200, 31.300, 74.550, 31.700) 
CRS_PROJECTED = "EPSG:3857"
CRS_WGS84 = "EPSG:4326"

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "lahore_cafes_restaurants.csv")

ox.settings.use_cache = True
ox.settings.timeout = 180

# -----------------------------
# HELPERS
# -----------------------------
def safe_centroid(gdf):
    """Calculates the center point of the restaurant/cafe area."""
    gdf_proj = gdf.to_crs(CRS_PROJECTED)
    gdf["geometry"] = gdf_proj.centroid.to_crs(CRS_WGS84)
    return gdf

def fetch_layer(tags, name):
    logger.info(f"Fetching {name} from Overpass...")
    try:
        gdf = ox.features_from_bbox(bbox=BBOX, tags=tags)
        if gdf.empty:
            return None
        
        # Ensure name column exists
        if "name" not in gdf.columns:
            gdf["name"] = f"Unnamed {name}"
        else:
            gdf["name"] = gdf["name"].fillna(f"Unnamed {name}")
            
        # Return only the essential columns
        return gdf[["name", "amenity", "geometry"]]
    except Exception as e:
        logger.error(f"Failed to fetch {name}: {e}")
        return None

# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    logger.info("STEP 1B: Fetching Food & Beverage POIs")

    # Define the tags for competitors/traffic indicators
    tags = {
        "amenity": ["cafe", "restaurant", "fast_food", "food_court"]
    }

    # Fetch data
    gdf = fetch_layer(tags, "Dining_Spots")

    if gdf is None or gdf.empty:
        logger.error("No dining data found in the specified area.")
        return

    # 1. Convert areas/points to centroids
    gdf = safe_centroid(gdf)

    # 2. Extract Coordinates
    gdf["lat"] = gdf.geometry.y
    gdf["lon"] = gdf.geometry.x

    # 3. Clean Dataframe
    df = pd.DataFrame(gdf.drop(columns="geometry"))

    # 4. Spatial Deduplication
    df["lat_r"] = df["lat"].round(4)
    df["lon_r"] = df["lon"].round(4)
    
    before = len(df)
    df = df.drop_duplicates(subset=["lat_r", "lon_r"])
    df = df.drop(columns=["lat_r", "lon_r"])
    
    logger.info(f"Removed {before - len(df)} spatial duplicates.")

    # 5. Save
    df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Saved {len(df)} dining spots to {OUTPUT_FILE}")
    
    print(f"\n✅ SUCCESS: Collected {len(df)} Restaurants, Cafes, and Fast Food spots.")
    print(f"📍 Location: {OUTPUT_FILE}")

if __name__ == "__main__":
    run()