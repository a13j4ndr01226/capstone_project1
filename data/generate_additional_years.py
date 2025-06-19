"""
generate_additional_4_years.py

This script generates 4 additional years of random trend score data (2018–2024)
for every artist-location pair found in the input file. The result is merged with the
existing 1-year dataset (2024–2025) to form a historical dataset.

Input:
    - artist_trend_scores_1year_full_us.csv

Output:
    - artist_trend_scores_final.csv
"""

import pandas as pd
import numpy as np
import os

def generate_years(start_date, end_date, artists_df, locations):
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    rows = []

    print(f"Generating data from {start_date.date()} to {end_date.date()} ({len(date_range)} days)")

    for location in locations:
        for _, row in artists_df.iterrows():
            artist = row['artist']
            artist_id = row['id']
            genres = row['genres']
            trend_scores = np.random.randint(0, 101, size=len(date_range))

            for date, score in zip(date_range, trend_scores):
                rows.append({
                    'artist': artist,
                    'id': artist_id,
                    'genres': genres,
                    'location': location,
                    'date': date,
                    'trend_score': score
                })

    return pd.DataFrame(rows)

def main():
    input_file = "C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/artist_trend_scores_1year_full_us.csv"
    output_file = "C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/artist_trend_scores_final.csv"

    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        return

    print("Loading 1-year full US dataset...")
    df = pd.read_csv(input_file, parse_dates=["date"])

    # Get artist metadata and location list
    artists = df[['artist', 'id', 'genres']].drop_duplicates()
    locations = df['location'].drop_duplicates().tolist()

    # Define range: June 1, 2019 to May 31, 2024
    start = pd.Timestamp("2018-06-01")
    end = pd.Timestamp("2024-05-31")

    print("Generating 6 years of synthetic trend data...")
    final_df = generate_years(start, end, artists, locations)

    print("Concatenating with existing data...")
    combined = pd.concat([final_df, df], ignore_index=True)
    combined = combined.sort_values(by=['artist', 'location', 'date'])

    print(f"Saving 7-year dataset to {output_file}...")
    combined.to_csv(output_file, index=False)
    print(f"Done. Output shape: {combined.shape}")

if __name__ == "__main__":
    main()
