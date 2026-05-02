# # import kagglehub

# # # Download latest version
# # path = kagglehub.dataset_download("ahmadalam69/lahore-property-rents-geocoded")

# # print("Path to dataset files:", path)

# # import pandas as pd
# # import io

# # # 1. LOAD DATA
# # # (Assuming your data is in a file named zameen_geocoded_raw.csv)
# # # For now, I'm using your provided text as a string input
# # raw_data = """Type,Price,Location,Baths,Area,Beds,Lat,Lng
# # Upper Portion,PKR45 Thousand,"Wapda Town, Lahore, Punjab",3,10 Marla,2,31.4311985,74.26435819999999
# # House,PKR5.95 Lakh,"Gulberg, Lahore, Punjab",7,2 Kanal,6,31.5164883,74.3499496
# # House,PKR15 Lakh,"Gulberg, Lahore, Punjab",7,4 Kanal,6,31.5164883,74.3499496
# # Flat,PKR45 Thousand,"Bahria Town, Lahore, Punjab",1,2.4 Marla,1,31.3694884,74.1768412"""

# # df = pd.read_csv('data/lahore-property-rents-geocoded.csv') 

# # def clean_zameen_data(df):
# #     # --- Part A: Price Cleaning ---
# #     def parse_price(x):
# #         x = str(x).lower().replace('pkr', '').replace(',', '').strip()
# #         multiplier = 1
# #         if 'lakh' in x:
# #             multiplier = 100000
# #             x = x.replace('lakh', '').strip()
# #         elif 'thousand' in x:
# #             multiplier = 1000
# #             x = x.replace('thousand', '').strip()
# #         elif 'crore' in x:
# #             multiplier = 10000000
# #             x = x.replace('crore', '').strip()
# #         try:
# #             return float(x) * multiplier
# #         except:
# #             return 0

# #     # --- Part B: Area Cleaning (to Sqft) ---
# #     def parse_area(x):
# #         x = str(x).lower().strip()
# #         try:
# #             val = float(x.split()[0])
# #             if 'kanal' in x:
# #                 return val * 4500
# #             if 'marla' in x:
# #                 return val * 225
# #             return val
# #         except:
# #             return 0

# #     # Apply transformations
# #     df['price_numeric'] = df['Price'].apply(parse_price)
# #     df['area_sqft'] = df['Area'].apply(parse_area)
    
# #     # --- Part C: Calculate Feature ---
# #     # Avoid division by zero
# #     df = df[df['area_sqft'] > 0]
# #     df['rent_per_sqft'] = df['price_numeric'] / df['area_sqft']
    
# #     # Rename columns to match the Pipeline v2 requirements
# #     df = df.rename(columns={'Lat': 'lat', 'Lng': 'lon'})
    
# #     return df[['lat', 'lon', 'rent_per_sqft', 'Location']]

# # # Execute
# # cleaned_df = clean_zameen_data(df)
# # cleaned_df.to_csv('data/lahore_rent_final_clean.csv', index=False)

# # print("✅ Data Cleaned & Normalized!")
# # print(cleaned_df.head())

# import pandas as pd
# import os

# def finalize_zameen_cleaning(input_path, output_path):
#     if not os.path.exists(input_path):
#         print(f"❌ Error: {input_path} not found. Check your folder location!")
#         return

#     df = pd.read_csv(input_path)

#     # 1. Standardize Price (Handling 'Lakh' and 'Thousand')
#     def normalize_price(p):
#         p = str(p).lower().replace('pkr', '').replace(',', '').strip()
#         try:
#             if 'lakh' in p:
#                 return float(p.replace('lakh', '').strip()) * 100000
#             if 'thousand' in p:
#                 return float(p.replace('thousand', '').strip()) * 1000
#             return float(p)
#         except: return 0

#     # 2. Standardize Area (Kanal to Marla, then to Sqft)
#     def normalize_area(a):
#         a = str(a).lower().strip()
#         try:
#             val = float(a.split()[0])
#             if 'kanal' in a: return val * 4500
#             if 'marla' in a: return val * 225
#             return val
#         except: return 0

#     df['rent_total'] = df['Price'].apply(normalize_price)
#     df['total_sqft'] = df['Area'].apply(normalize_area)
    
#     # Avoid division by zero
#     df = df[df['total_sqft'] > 0]
#     df['rent_per_sqft'] = df['rent_total'] / df['total_sqft']

#     # 3. IDENTIFY PROPERTY CLASS (Crucial for 2026 Pipeline)
#     # If it's a House/Flat, it represents 'Neighborhood Wealth'
#     # If it's a Shop/Office/Commercial, it represents 'Direct Business Cost'
#     df['is_commercial'] = df['Type'].str.contains('Shop|Office|Commercial|Building', case=False)
    
#     # Logic: If it's residential, we use it as a proxy for neighborhood eliteness
#     # We create a 'wealth_score' (higher rent = wealthier residents)
#     df['wealth_index'] = df.apply(lambda x: x['rent_per_sqft'] if not x['is_commercial'] else np.nan, axis=1)

#     # 4. Clean Outliers
#     # Remove entries where rent is unrealistic for Lahore (e.g., < 5 or > 1000 per sqft)
#     df = df[(df['rent_per_sqft'] > 5) & (df['rent_per_sqft'] < 1500)]

#     df.to_csv(output_path, index=False)
#     print(f"✅ Success! {len(df)} records cleaned and classified.")
#     print(f"📍 Saved to: {output_path}")

# if __name__ == "__main__":
#     # Adjusting paths for your current terminal location (inside /data)
#     finalize_zameen_cleaning('lahore-property-rents-geocoded.csv', 'lahore_rent_final.csv')


# import kagglehub

# # Download latest version
# path = kagglehub.dataset_download("hassaanmustafavi/real-estate-dataset-pakistan")

# print("Path to dataset files:", path)
# import kagglehub

# # Download latest version
# path = kagglehub.dataset_download("huzzefakhan/pakistans-1st-online-real-estate-data-set")

# print("Path to dataset files:", path)

# import pandas as pd
# import numpy as np

# def clean_graana_pipeline_data(input_path, output_path):
#     # 1. Load Data
#     df = pd.read_csv(input_path)
    
#     # 2. Filter for Lahore only (Project Scope)
#     # The snippet you provided shows Islamabad; this ensures only Lahore data enters your H3 grid
#     df = df[df['city_name'].str.contains('Lahore', case=False, na=False)].copy()
    
#     if df.empty:
#         print("⚠️ No Lahore listings found in the Graana dataset. Please check your source file.")
#         return

#     # 3. Filter for Rental Listings only
#     df = df[df['purpose'].str.lower() == 'rent']

#     # 4. Standardize Area to Sqft (Lahore benchmarks: 1 Marla = 225 sqft)
#     def normalize_area(row):
#         size = float(row['size'])
#         unit = str(row['size_unit']).lower()
#         if 'marla' in unit:
#             return size * 225
#         if 'kanal' in unit:
#             return size * 4500
#         return size # Defaults to sqft if already standardized

#     df['total_sqft'] = df.apply(normalize_area, axis=1)
    
#     # 5. Calculate Rent Per Sqft
#     # Graana price is usually numeric, avoiding the 'Lakh/Crore' parsing issues of Zameen
#     df['price_per_sqft'] = df['price'] / df['total_sqft']

#     # 6. Signal Classification (Commercial vs Residential)
#     # Commercial = Direct Cost Proxy | Residential = Neighborhood Wealth Proxy
#     commercial_types = ['shop', 'office', 'commercial', 'plaza', 'building']
#     df['is_commercial'] = df['type'].str.lower().isin(commercial_types)
    
#     # Wealth Signal for Residential (House/Flat/Portion)
#     df['wealth_index'] = np.where(df['is_commercial'] == False, df['price_per_sqft'], np.nan)

#     # 7. Final Formatting & Coordinate Check
#     # Dropping rows without valid coordinates as they cannot be mapped to H3 cells
#     df = df.dropna(subset=['lat', 'lon'])
    
#     # Selecting columns mapped to the 22 features required for XGBoost
#     final_cols = [
#         'type', 'area_name', 'lat', 'lon', 
#         'price_per_sqft', 'is_commercial', 'wealth_index'
#     ]
    
#     df_final = df[final_cols]
#     df_final.to_csv(output_path, index=False)
    
#     print(f"✅ Cleaned {len(df_final)} Lahore listings from Graana data.")
#     print(f"📍 Saved output to: {output_path}")

# if __name__ == "__main__":
#     # Ensure this matches your file location
#     clean_graana_pipeline_data('data/graana.csv', 'data/lahore_graana_clean.csv')


import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

def scrape_graana_robust(total_pages=5): # Start with 5 pages to test
    all_listings = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for page in range(1, total_pages + 1):
        url = f"https://www.graana.com/rent/commercial-properties-rent-lahore-{page}/"
        if page == 1:
            url = "https://www.graana.com/rent/commercial-properties-rent-lahore/"
            
        print(f"📡 Requesting: {url}")
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ Failed to fetch page {page}. Status: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for all text blocks that look like property info
        # We search for divs that contain the 'PKR' string
        price_divs = soup.find_all('div', string=re.compile(r'PKR', re.IGNORECASE))
        
        for p_div in price_divs:
            try:
                # The parent container usually holds the rest of the info
                container = p_div.find_parent('div') 
                
                # Extracting sibling information based on common Graana patterns
                text_content = container.get_text(separator="|").split("|")
                
                # Logic: Find the price, area, and location from the text list
                price = p_div.get_text().strip()
                area = "Unknown"
                location = "Lahore"
                
                for item in text_content:
                    if 'marla' in item.lower() or 'sqft' in item.lower():
                        area = item.strip()
                    if 'lahore' in item.lower() and len(item) > 10:
                        location = item.strip()

                all_listings.append({
                    'price': price,
                    'area': area,
                    'location': location
                })
            except:
                continue
                
        print(f"✅ Found {len(price_divs)} potential items on page {page}")
        time.sleep(2)

    return pd.DataFrame(all_listings)

# Run and check
df_graana = scrape_graana_robust(total_pages=3)
if not df_graana.empty:
    df_graana.to_csv('data/graana_test.csv', index=False)
    print(df_graana.head())
else:
    print("🚨 Still empty. Graana might be blocking basic requests.")