from src.models.name_normalizer import WineFuzzyMatch
from dagster import pipeline, ModeDefinition
from preprocess_data import *

dev_mode = ModeDefinition("dev")
staging_mode = ModeDefinition("staging")
prod_mode = ModeDefinition("prod")


@pipeline(mode_defs=[dev_mode, staging_mode, prod_mode])
def processing_pipeline():
    lots, weighted_wine_estimations, wine_quotes, wine_estimations = get_file_names_raw()
    lots_df = read_json_from_raw(lots)
    weighted_wine_estimations_df = read_csv_from_raw(weighted_wine_estimations)
    wine_quotes_df = read_csv_from_raw(wine_quotes)
    wine_estimations_df = read_csv_from_raw(wine_estimations)
    lots_df_processed = process_lots_perfect_state(lots_df, weighted_wine_estimations_df)
    wine_estimations_df_processed = process_estimator_perfect_state(wine_estimations_df)
    wine_quotes_df_processed = process_wine_quotation(wine_quotes_df)
    final_df = concat_dataframes([lots_df_processed, wine_estimations_df_processed, wine_quotes_df_processed])
    normalized_df = name_normalizer(final_df)
    save_to_file(normalized_df)
