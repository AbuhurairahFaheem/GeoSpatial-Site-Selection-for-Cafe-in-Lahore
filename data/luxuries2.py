import osmnx as ox
import pandas as pd
import time
import os
from utils.logging import get_logger

logger = get_logger(__name__)

# BBOX for Lahore (West, South, East, North)
BBOX = (74.200, 31.300, 74.550, 31.700) 
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "lahore_lifestyle_combined.csv")

ox.settings.use_cache = True

def fetch_layer(layer_name, tags, weight):
    logger.info(f"Fetching: {layer_name}...")
    try:
        gdf = ox.features_from_bbox(bbox=BBOX, tags=tags)
        if gdf.empty: return None
        
        # Project and get centroids
        gdf_proj = gdf.to_crs(epsg=3857)
        gdf["lat"] = gdf_proj.centroid.to_crs(epsg=4326).y
        gdf["lon"] = gdf_proj.centroid.to_crs(epsg=4326).x
        
        gdf["name"] = gdf.get("name", f"Unnamed {layer_name}").fillna(f"Unnamed {layer_name}")
        gdf["layer"] = layer_name
        gdf["weight"] = weight
        
        return pd.DataFrame(gdf[["name", "layer", "lat", "lon", "weight"]])
    except Exception:
        return None

def run():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # UPDATED LIST: Broadened for South Asian OSM Mapping Habits
    LAYERS = [
        # --- Sports & Padel ---
        ('sports_and_padel', {'leisure': ['sports_centre', 'pitch', 'club']}, 1.0),
        
        # --- Wellness & Fitness ---
        ('fitness_gym',    {'leisure': ['fitness_centre', 'health_club']}, 0.95),
        ('gym_alt',        {'amenity': 'gym'},                             0.9),
        
        # --- Beauty & Personal Care ---
        ('beauty_wellness',{'shop': ['beauty', 'hairdresser', 'barber']},  0.8),
        ('medical_beauty', {'amenity': 'clinic'},                          0.85),
        
        # --- Social & Indoor Fun ---
        ('recreation',     {'leisure': ['billiards', 'amusement_arcade']}, 0.8),
        ('social_hubs',    {'amenity': ['social_facility', 'community_centre']}, 0.7),
        
        # --- Shopping & Work ---
        ('coworking_office',{'office': ['coworking', 'commercial']},       1.0),
        ('high_end_shop',   {'shop': ['supermarket', 'mall', 'boutique']}, 0.8),
        ('books_and_tech',  {'shop': ['books', 'electronics']},            0.7),
    ]

    all_data = []
    for name, tags, weight in LAYERS:
        df = fetch_layer(name, tags, weight)
        if df is not None:
            all_data.append(df)
            logger.info(f"Found {len(df)} locations for {name}")
            time.sleep(1)

    if not all_data:
        logger.error("No data collected.")
        return

    final_df = pd.concat(all_data, ignore_index=True)

    # --- FIXING THE PANDAS ERROR ---
    # Create temp columns for rounding
    final_df["lat_round"] = final_df["lat"].round(4)
    final_df["lon_round"] = final_df["lon"].round(4)
    
    before = len(final_df)
    # Deduplicate using the temp column names (strings), not the series
    final_df = final_df.drop_duplicates(subset=["lat_round", "lon_round"])
    
    # Drop temp columns
    final_df = final_df.drop(columns=["lat_round", "lon_round"])
    
    logger.info(f"Deduplication: Reduced from {before} to {len(final_df)} points.")

    # Save
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ SUCCESS: {len(final_df)} Lifestyle points saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    run()