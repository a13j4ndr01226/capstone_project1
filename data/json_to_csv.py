"""
json_to_csv.py

This script loads a flat JSON file containing Spotify artist metadata 
and converts it into a clean CSV format.

Input:
- spotify_rising_artists.json

Output:
- spotify_rising_artists_batchdate.csv
"""

import json
import pandas as pd
from datetime import datetime
# import os


batch_date = datetime.now().strftime('%Y_%m_%d')
# batch_date = '2025_06_08'

# File paths
input_path = rf"C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/spotify_rising_artists_{batch_date}.json"
output_path = input_path.replace(".json", ".csv")

# Load JSON file
with open(input_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Convert to DataFrame
df = pd.DataFrame(data)

# Convert genre list to semicolon-separated string
df['genres'] = df['genres'].apply(lambda g: "; ".join(g) if isinstance(g, list) else "")

# Save as CSV
df.to_csv(output_path, index=False)

print(f"Saved {len(df)} artist records to {output_path}")
