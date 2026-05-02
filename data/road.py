import osmnx as ox
import pandas as pd
import os
from utils.logging import get_logger

logger = get_logger(__name__)

BBOX = (74.200, 31.300, 74.550, 31.700)
OUTPUT_DIR = "data"
OUTPUT_ROADS = os.path.join(OUTPUT_DIR, "lahore_weighted_roads.csv")

# Define Weights based on Commercial Value
ROAD_WEIGHTS = {
    'primary': 1.0,
    'secondary': 0.9,
    'trunk': 0.8,
    'tertiary': 0.7,
    'residential': 0.4,
    'unclassified': 0.3,
    'motorway': 0.1,  # Hard to stop on a motorway!
    'service': 0.2
}

def run():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

    logger.info("Fetching Massive-Scale Road Network...")
    
    # Fetch all drivable roads
    G = ox.graph_from_bbox(bbox=BBOX, network_type='drive', simplify=True)
    
    # Convert to GeoDataFrame to manipulate data
    nodes, edges = ox.graph_to_gdfs(G)
    
    logger.info(f"Processing {len(edges)} road segments...")

    # Assign Weights
    def assign_weight(row):
        # OSM sometimes returns a list if a road has multiple tags
        h_type = row['highway']
        if isinstance(h_type, list):
            h_type = h_type[0]
        return ROAD_WEIGHTS.get(h_type, 0.1)

    edges['commercial_weight'] = edges.apply(assign_weight, axis=1)
    
    # Calculate 'Importance Score' (Length * Weight)
    # This helps identify long, high-traffic corridors
    edges['importance_score'] = edges['length'] * edges['commercial_weight']

    # Keep only important columns for massive scale analysis
    # We strip geometry to keep the CSV lightweight
    road_data = edges[['highway', 'oneway', 'length', 'commercial_weight', 'importance_score']].copy()
    
    # Reset index to get u, v, key (OSM Node IDs)
    road_data = road_data.reset_index()

    road_data.to_csv(OUTPUT_ROADS, index=False)
    
    print(f"✅ Success: {len(road_data)} road segments weighted and saved.")
    print(f"📍 File: {OUTPUT_ROADS}")

if __name__ == "__main__":
    run()