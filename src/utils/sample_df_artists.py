import pandas as pd
import json
import time
import csv
from pathlib import Path
from datetime import date
from pathlib import Path
from typing import Dict, List, Any

input_path = Path(r"C:\Users\Aleja\Documents\Data_Engineering\springboard\capstone_project1\data\raw\artists\2026_02_03\genre=house\market=US\artists.json") 

df = pd.read_json(input_path)

print("Loaded shape:", df.shape)
print(df.head())
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