import requests
import pandas as pd

def download_roads_raw():
    print("Bypassing osmnx. Hitting Overpass API directly for major roads...")
    
    overpass_url = "https://overpass-api.de/api/interpreter"
    
    # Raw OverpassQL: Get major roads in Lahore bbox, output only the center coordinates
    overpass_query = """
    [out:json][timeout:900];
    way["highway"~"motorway|trunk|primary|secondary|tertiary"](31.41,74.01,31.65,74.47);
    out center;
    """
    
    try:
        # Adding a User-Agent header so the server knows we aren't a malicious bot
        headers = {
            'User-Agent': 'LahoreCafeSiteSelection/1.0 (92ammartalib786@gmail.com)'
        }
        response = requests.post(overpass_url, data={'data': overpass_query}, headers=headers)
        response.raise_for_status() # Check for HTTP errors
        
        # THIS IS THE MISSING LINE: Parse the response into JSON
        data = response.json() 
        
        records = []
        # Extract the center points calculated by the server
        for element in data.get('elements', []):
            if element['type'] == 'way' and 'center' in element:
                records.append({
                    'Latitude': element['center']['lat'],
                    'Longitude': element['center']['lon']
                })
                
        df = pd.DataFrame(records)
        
        if df.empty:
            print("[ERROR] Server returned no data. It might be rate-limiting you.")
            return

        output_file = 'lahore_road_nodes.csv'
        df.to_csv(output_file, index=False)
        print(f"\n[SUCCESS] Bypassed! Saved {len(df):,} major road points to {output_file}")
        
    except Exception as e:
        print(f"\n[ERROR] Request failed: {e}")

if __name__ == "__main__":
    download_roads_raw()