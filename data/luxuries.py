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

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "lahore_luxuries_amenities.csv")

ox.settings.use_cache = True
ox.settings.timeout = 180

# -----------------------------
# HELPERS
# -----------------------------
def safe_centroid(gdf):
    gdf_proj = gdf.to_crs(CRS_PROJECTED)
    gdf["geometry"] = gdf_proj.centroid.to_crs(CRS_WGS84)
    return gdf

def fetch_luxury_layer(layer_name, tags, weight):
    logger.info(f"Fetching {layer_name} (Weight: {weight})...")
    try:
        # Fetch features
        gdf = ox.features_from_bbox(bbox=BBOX, tags=tags)
        
        if gdf.empty:
            return None
        
        # Standardize columns
        if "name" not in gdf.columns:
            gdf["name"] = f"Unnamed {layer_name}"
        else:
            gdf["name"] = gdf["name"].fillna(f"Unnamed {layer_name}")
            
        # Process geometries to points
        gdf = safe_centroid(gdf)
        
        # Extract attributes
        gdf["lat"] = gdf.geometry.y
        gdf["lon"] = gdf.geometry.x
        gdf["layer"] = layer_name
        gdf["weight"] = weight
        
        return pd.DataFrame(gdf.drop(columns="geometry"))[["name", "layer", "lat", "lon", "weight"]]
    
    except Exception as e:
        logger.error(f"Error fetching {layer_name}: {e}")
        return None

# -----------------------------
# MAIN PIPELINE
# -----------------------------
def run():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    logger.info("STEP 1D: Fetching Luxury Layers and Amenities")

    # Your defined layers with tags and weights
    LUXURY_LAYERS = [
        ('mall',           {'shop': 'mall'},                      1.0),
        ('hotel',          {'tourism': 'hotel'},                  0.9),
        ('beauty_salon',   {'shop': 'beauty'},                    0.85),
        ('hairdresser',    {'shop': 'hairdresser'},               0.7),
        ('spa',            {'amenity': 'spa'},                    0.9),
        ('gym',            {'leisure': 'fitness_centre'},         0.8),
        ('hospital',       {'amenity': ['hospital', 'clinic']},   0.75),
        ('private_school', {'amenity': 'school'},                 0.7),
        ('bank',           {'amenity': 'bank'},                   0.6),
        ('jewellery',      {'shop': 'jewelry'},                   0.8),
        ('clothing',       {'shop': 'clothes'},                   0.65),
        ('cinema',         {'amenity': 'cinema'},                 0.85),
    ]

    all_data = []

    for layer_name, tags, weight in LUXURY_LAYERS:
        df = fetch_luxury_layer(layer_name, tags, weight)
        
        if df is not None:
            all_data.append(df)
            logger.info(f"Found {len(df)} locations for {layer_name}")
            time.sleep(1.5) # API politeness
        else:
            logger.warning(f"No data for {layer_name}")

    if not all_data:
        logger.error("No luxury data collected at all.")
        return

    # Combine all layers
    final_df = pd.concat(all_data, ignore_index=True)

    # Spatial Deduplication (Catch duplicates across different tags)
    before = len(final_df)
    final_df["lat_r"] = final_df["lat"].round(4)
    final_df["lon_r"] = final_df["lon"].round(4)
    final_df = final_df.drop_duplicates(subset=["lat_r", "lon_r", "layer"])
    final_df = final_df.drop(columns=["lat_r", "lon_r"])
    
    logger.info(f"Deduplication: Reduced from {before} to {len(final_df)} points.")

    # Save to CSV
    final_df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"Saved luxury dataset to {OUTPUT_FILE}")
    print(f"\n✅ SUCCESS: Collected {len(final_df)} Luxury/Amenity locations.")

if __name__ == "__main__":
    run()