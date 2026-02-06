import pandas as pd
import json
import time
import csv
from pathlib import Path
from datetime import date
from pathlib import Path
from typing import Dict, List, Any

input_path = Path(r"C:\Users\Aleja\Documents\Data_Engineering\springboard\capstone_project1\data\raw\tracks\2026_02_03\genre=house\market=US\tracks.json") 

df = pd.read_json(input_path)

print("Loaded shape:", df.shape)
print(df.head())
print("----------Data Types--------------")
print(df.dtypes)
""" 
----------Data Types--------------
album             object
artists           object
disc_number        int64
duration_ms        int64
explicit            bool
external_ids      object
external_urls     object
href              object
id                object
is_local            bool
is_playable         bool
name              object
popularity         int64
preview_url      float64
track_number       int64
type              object
uri               object
dtype: object
"""