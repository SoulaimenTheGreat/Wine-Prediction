from src.models.name_normalizer import WineFuzzyMatch
from dagster import pipeline
from preprocess_data import *
# fuzzy_match = WineFuzzyMatch()
# wine_quotations_df = wine_quotation()
# wine_estimator_df = wine_estimator_perfect_state()
# lots_df = lots_perfect_state()
# new_df = fuzzy_match.checker(df, "wineName")
# new_df = fuzzy_match.checker(df1, "title")


@pipeline
def processing_pipeline():
    read_csv_from_raw()
    process_wine_quotation()
