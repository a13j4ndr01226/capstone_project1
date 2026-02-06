import pandas as pd
import json
import time
import csv
from pathlib import Path
from datetime import date
from pathlib import Path
from typing import Dict, List, Any

input_path = "/mnt/c/Users/Aleja/Documents/Data_Engineering/springboard/capstone_project1/data/raw/2025_08_31/spotify_rising_with_trends_2025_08_31.csv"

df = pd.read_csv(input_path)

print("Loaded shape:", df.shape)
print(df.head(5))
print("----------Data Types--------------")
print(df.dtypes)
""" 
----------Data Types--------------
external_urls    object
followers        object
genres           object
href             object
id               object
images           object
name             object
popularity        int64
type             object
uri              object

"""