"""
generate_missing_states_data.py

This script augments a 1-year artist trend dataset by filling in synthetic data 
for all missing U.S. states. For each artist-location pair not present in the original data,
random daily trend scores (0–100) are generated for each date in the existing 1-year range.

Inputs:
    - artist_trend_scores_1year.csv (must include columns: artist, id, genres, location, date, trend_score)

Outputs:
    - artist_trend_scores_1year_full_us.csv with complete data across all 50 U.S. states

Requirements:
    - Python 3
    - pandas
    - numpy
"""

import pandas as pd
import numpy as np

def generate_missing_state_data(input_csv, output_csv):
    # Load original data
    df = pd.read_csv(input_csv, parse_dates=["date"])

    # Define all U.S. states
    us_states = [
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]

    # Identify missing states
    existing_states = df['location'].unique().tolist()
    missing_states = [state for state in us_states if state not in existing_states]

    # Get unique artist info and date range
    artists = df[['artist', 'id', 'genres']].drop_duplicates()
    date_range = df['date'].drop_duplicates().sort_values()

    # Generate synthetic data
    synthetic_rows = []
    for state in missing_states:
        for _, row in artists.iterrows():
            artist = row['artist']
            artist_id = row['id']
            genres = row['genres']
            random_scores = np.random.randint(0, 101, size=len(date_range))

            for date, score in zip(date_range, random_scores):
                synthetic_rows.append({
                    'artist': artist,
                    'id': artist_id,
                    'genres': genres,
                    'location': state,
                    'date': date,
                    'trend_score': score
                })

    # Combine and save
    new_data = pd.DataFrame(synthetic_rows)
    full_df = pd.concat([df, new_data], ignore_index=True)
    full_df = full_df.sort_values(by=['artist', 'location', 'date'])
    full_df.to_csv(output_csv, index=False)

    print(f"Done! Full dataset saved to: {output_csv}")


if __name__ == "__main__":
    generate_missing_state_data(
        input_csv="C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/artist_trend_scores_1year.csv",
        output_csv="C:/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/artist_trend_scores_1year_full_us.csv"
    )
