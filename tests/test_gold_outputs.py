from src.io.read_sources import find_input_files, read_csv_files
from src.bronze.build_bronze import build_bronze
from src.silver.build_silver import build_silver
from src.gold.controller_aggregates import build_controller_monthly_aggregates
from src.gold.auditor_totals import build_auditor_cross_subsidiary_totals

def test_gold_outputs_shapes():
    files = find_input_files("data/raw")
    df_raw = read_csv_files(files)
    df_bronze = build_bronze(df_raw)
    df_silver = build_silver(df_bronze)

    controller = build_controller_monthly_aggregates(df_silver)
    auditor = build_auditor_cross_subsidiary_totals(df_silver)

    assert set(controller.columns) == {"subsidiary_id","pay_period","employee_count","gross_total","taxes_total","net_total"}
    assert set(auditor.columns) == {"pay_period","subsidiary_count","employee_count","gross_total","taxes_total","net_total"}
