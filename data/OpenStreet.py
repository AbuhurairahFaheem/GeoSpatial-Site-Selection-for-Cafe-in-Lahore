import osmnx as ox
import pandas as pd
import time

# 1. Optimized Server Settings
ox.settings.timeout = 180  # Shorter timeout to fail fast and retry
ox.settings.use_cache = True
ox.settings.log_console = True

# 2. TIGHTER BOUNDING BOX (Focuses on high-ROI luxury zones only)
# This reduces the area size significantly to avoid server disconnection
center_lat, center_lon = 31.520, 74.350 # Centered between Gulberg and DHA
lat_offset, lon_offset = 0.08, 0.10     # Tighter window

north, south = center_lat + lat_offset, center_lat - lat_offset
east, west = center_lon + lon_offset, center_lon - lon_offset
BBOX = (north, south, east, west)

def fetch_with_retry(bbox, tags, label, retries=3):
    print(f"\n--- Starting {label} ---")
    for i in range(retries):
        try:
            pois = ox.features_from_bbox(bbox, tags=tags)
            if pois.empty: return pd.DataFrame()
            
            pois = pois.reset_index()
            # Simplified Centroid (Faster)
            pois["lat"] = pois.geometry.centroid.y
            pois["lon"] = pois.geometry.centroid.x
            
            cols = ["name", "amenity", "shop", "leisure", "building", "lat", "lon"]
            return pois[[c for c in cols if c in pois.columns]]
        except Exception as e:
            print(f"Attempt {i+1} failed: {e}. Retrying in 5s...")
            time.sleep(5)
    return pd.DataFrame()

# --- 3. DATA COLLECTION ---

# Universities
df_unis = fetch_with_retry(BBOX, {"amenity": ["university", "college"]}, "Universities")
df_unis.to_csv("universities_lahore.csv", index=False)

# Competitors
df_cafes = fetch_with_retry(BBOX, {"amenity": ["cafe", "restaurant"]}, "Competitors")
df_cafes.to_csv("cafes_lahore.csv", index=False)

# # Luxury Part 1: Hotels & Banks
# df_lux1 = fetch_with_retry(BBOX, {"amenity": ["bank"], "building": ["hotel"]}, "Luxury P1")

# # Luxury Part 2: High-end Retail & Malls
# df_lux2 = fetch_with_retry(BBOX, {"shop": ["mall", "boutique", "jewelry"]}, "Luxury P2")

# # Luxury Part 3: Lifestyle & Fitness
# df_lux3 = fetch_with_retry(BBOX, {"leisure": ["fitness_centre", "sports_club"]}, "Luxury P3")

# # Combine and Save
# df_luxury = pd.concat([df_lux1, df_lux2, df_lux3], ignore_index=True)
# df_luxury.to_csv("luxury_anchors_osm.csv", index=False)

# Parking
df_parking = fetch_with_retry(BBOX, {"amenity": ["parking"]}, "Parking")
df_parking.to_csv("parking_lahore.csv", index=False)

print("\nSUCCESS: All critical CSV files generated.")