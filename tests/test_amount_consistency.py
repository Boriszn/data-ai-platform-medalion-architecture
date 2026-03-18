from src.io.read_sources import find_input_files, read_csv_files
from src.bronze.build_bronze import build_bronze
from src.silver.build_silver import build_silver

def test_amount_consistency_passes():
    files = find_input_files("data/raw")
    df_raw = read_csv_files(files)
    df_bronze = build_bronze(df_raw)
    df_silver = build_silver(df_bronze)
    assert len(df_silver) == len(df_bronze)
