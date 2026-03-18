import pandas as pd
from src.io.read_sources import find_input_files, read_csv_files
from src.config import REQUIRED_INPUT_COLUMNS

def test_input_schema():
    files = find_input_files("data/raw")
    df = read_csv_files(files)
    for c in REQUIRED_INPUT_COLUMNS:
        assert c in df.columns
