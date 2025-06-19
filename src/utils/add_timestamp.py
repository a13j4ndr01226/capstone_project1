"""
add_timestamp.py

For any previously scraped artists in 'spotify_rising_artists.json', a manual timestamp get added.
This helps with tracking batches and previous sessions.

"""


import json
from datetime import datetime

timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
file_date = datetime.now().strftime('%m_%d_%Y')

# Path to your existing JSON file
file_path = f"C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/spotify_rising_artists_{file_date}.json"

# Load the file
with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Update each entry
for entry in data:
    if 'scrape_date' not in entry:
        entry['scrape_date'] = timestamp

# Overwrite the original file
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2)

print(f"Timestamp added to {len(data)} records and saved to {file_path}")