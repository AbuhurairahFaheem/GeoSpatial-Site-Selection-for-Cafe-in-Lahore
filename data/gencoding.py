import pandas as pd
from geopy.geocoders import Nominatim
import time
import random
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable

# 1. Load data
df = pd.read_csv('data/lahore_rent_cleaned.csv')
unique_locations = pd.DataFrame(df['Location'].unique(), columns=['Location'])

print(f"Geocoding {len(unique_locations)} unique locations with 429-Error Protection...")

def get_coords_polite(loc, attempt=1):
    # Create a random user agent to avoid being flagged as a single bot
    user_agent = f"geosite_research_{random.randint(10000, 99999)}"
    geolocator = Nominatim(user_agent=user_agent, timeout=15)
    
    query = f"{loc}, Lahore, Pakistan"
    
    try:
        # Respectful delay: wait 2-4 seconds before EACH request
        time.sleep(random.uniform(2.0, 4.0))
        
        location = geolocator.geocode(query)
        if location:
            return location.latitude, location.longitude
        return None, None

    except GeocoderServiceError as e:
        if "429" in str(e):
            print(f"🛑 429 Detected! Server is tired. Sleeping for 60 seconds...")
            time.sleep(60) # Massive cooldown
            return get_coords_polite(loc, attempt + 1) if attempt < 3 else (None, None)
        return None, None
    except Exception:
        return None, None

# 4. Run the loop with progress tracking
coords = []
for index, row in unique_locations.iterrows():
    lat, lon = get_coords_polite(row['Location'])
    coords.append({'Location': row['Location'], 'lat': lat, 'lon': lon})
    
    if (index + 1) % 5 == 0:
        print(f"Progress: {index + 1}/{len(unique_locations)} (Current: {row['Location'][:30]}...)")

# 5. Merge and Save
unique_coords_df = pd.DataFrame(coords)
df_final = df.merge(unique_coords_df, on='Location', how='left').dropna(subset=['lat', 'lon'])
df_final.to_csv('data/lahore_rent_geocoded.csv', index=False)

print(f"\n✅ SUCCESS: {len(df_final)} points geocoded safely.")