import json

file_path = r"C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/spotify_rising_artists.json"

with open(file_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total number of artist entries: {len(data)}")
