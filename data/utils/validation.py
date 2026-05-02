# utils/validation.py

def validate_lat_lon(df):
    assert df['lat'].between(-90, 90).all()
    assert df['lon'].between(-180, 180).all()

def no_nulls(df):
    if df.isnull().sum().sum() > 0:
        raise ValueError("NULL values detected in final dataset")