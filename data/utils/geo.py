# utils/geo.py

import numpy as np

def haversine_vectorized(lat1, lon1, lat2, lon2):
    R = 6371  # km

    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlat = lat2 - lat1[:, None]
    dlon = lon2 - lon1[:, None]

    a = np.sin(dlat/2)**2 + np.cos(lat1[:, None]) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


def safe_centroid(gdf):
    gdf = gdf.to_crs(epsg=3857)
    gdf["geometry"] = gdf.centroid
    return gdf.to_crs(epsg=4326)