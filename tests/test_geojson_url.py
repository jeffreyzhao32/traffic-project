import requests
import json
from urllib.parse import quote

# 1. We update the 'where' clause. 
# ArcGIS SQL syntax for dates usually looks like: occ_date >= DATE '2026-01-01'
WHERE_CLAUSE = "occ_date >= DATE '2024-01-01'"

# 2. Re-construct the URL with the new filter
# Note: we use quote() to ensure the spaces and quotes in the SQL are URL-safe
GEOJSON_URL = (
    "https://services8.arcgis.com/lYI034SQcOoxRCR7/arcgis/rest/services/"
    "Road_Safety/FeatureServer/0/query"
    f"?outFields=*&where={quote(WHERE_CLAUSE)}&f=geojson"
)

def fetch_geojson(url: str):
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        return None

def main():
    print(f"Fetching data with filter: {WHERE_CLAUSE}...")
    data = fetch_geojson(GEOJSON_URL)
    
    if not data:
        return

    # Basic validation
    print("GeoJSON type:", data.get("type"))
    features = data.get("features", [])
    print("Number of features found:", len(features))

    # Print a sample feature
    if features:
        sample = features[0]
        print("\nSample feature properties:")
        print(json.dumps(sample["properties"], indent=2))
    else:
        print("\nNo features matched the criteria.")

if __name__ == "__main__":
    main()