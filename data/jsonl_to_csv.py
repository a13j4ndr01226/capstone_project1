"""
jsonl_to_csv.py

This script loads a JSONL file containing artist trend data, reshapes it into a tabular format,
fills missing or zero scores for each artist-location pair across a 1-year period, and saves the result as a CSV.

Features:
- Handles nested genre lists
- Fills full date range (365 days)
- Smoothly imputes missing weeks using random weekly averages

Input:
- spotify_rising_with_trends.jsonl

Output:
- artist_trend_scores_1year.csv
"""

import pandas as pd
import numpy as np
import json
import re

# Load JSONL file
input_path = "C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/spotify_rising_with_trends.jsonl"
data = []
with open(input_path, 'r') as f:
    for line in f:
        data.append(json.loads(line))

# Normalize data and melt trend columns
df = pd.json_normalize(data)
id_vars = ['artist', 'id', 'genres']
trend_cols = [col for col in df.columns if col.startswith("daily_trends_")]
melted = df.melt(id_vars=id_vars, value_vars=trend_cols, var_name="location_date", value_name="trend_score")
melted = melted.dropna(subset=["trend_score"])

# Extract location and date from column names
def extract_location_date(value):
    match = re.match(r'daily_trends_([a-z]+)\.(\d{4}-\d{2}-\d{2})', value)
    return (match.group(1).upper(), match.group(2)) if match else (None, None)

melted[['location', 'date']] = melted['location_date'].apply(lambda x: pd.Series(extract_location_date(x)))
melted['date'] = pd.to_datetime(melted['date'])
melted = melted[['artist', 'id', 'genres', 'location', 'date', 'trend_score']]

# Filter to the last 365 days
latest_date = melted['date'].max()
start_date = latest_date - pd.Timedelta(days=364)
full_dates = pd.date_range(start=start_date, end=latest_date, freq='D')
filtered = melted[(melted['date'] >= start_date) & (melted['date'] <= latest_date)]

# Helper functions
def generate_weekly_scores(avg_score):
    scores = np.random.rand(7)
    scores = scores / scores.sum() * avg_score * 7
    return np.round(scores)

def fill_week(group):
    group = group.copy()
    mask = group['trend_score'].isna() | (group['trend_score'] == 0)
    if mask.sum() == 7:
        avg = np.random.randint(20, 81)
        group.loc[mask, 'trend_score'] = generate_weekly_scores(avg)
    return group

# Reindex each artist-location pair to fill missing dates
filled_rows = []
for (artist, location), group in filtered.groupby(['artist', 'location']):
    genre = group['genres'].iloc[0]
    genre_str = '; '.join(genre) if isinstance(genre, list) else str(genre)
    artist_id = group['id'].iloc[0]

    temp = group.set_index('date').reindex(full_dates).reset_index().rename(columns={'index': 'date'})
    temp['artist'] = artist
    temp['location'] = location
    temp['id'] = artist_id
    temp['genres'] = genre_str
    temp['trend_score'] = temp['trend_score'].astype(float)

    temp['week'] = temp['date'].dt.to_period('W').apply(lambda r: r.start_time)
    temp = temp.groupby('week').apply(fill_week).reset_index(drop=True)
    temp = temp.drop(columns='week')

    filled_rows.append(temp)

# Final DataFrame
final_df = pd.concat(filled_rows)
final_df = final_df.sort_values(by=['artist', 'location', 'date'])

# Save to CSV
output_path = "C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/artist_trend_scores_1year.csv"
final_df.to_csv(output_path, index=False)

print(f"Saved to {output_path} with shape: {final_df.shape}")
