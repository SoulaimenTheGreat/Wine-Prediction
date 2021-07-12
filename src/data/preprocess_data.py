import pandas as pd
from pandas import json_normalize
import json
import os
import sys


def read_csv_from_raw(filename):
    """
    Reads CSV file from raw data by providing filename
    :return: DataFrame contains the data
    """
    data_df = pd.read_csv("../../data/raw/" + filename + ".csv")
    return data_df


def read_json_from_raw(filename):
    """
    Read JSON file from raw data by providing filename then convert it to DataFrame
    :return: DataFrame
    """
    with open("../../data/raw/" + filename + ".json", "r") as read_data:
        data_json = json.load(read_data)

    data_df = json_normalize(data_json)
    return data_df


def lots_perfect_state():
    """
    Function that returns the lots in perfect state after fetching the weighted estimation in the perfect state and then
    filtered with the whole lot after matching with lot ids
    :return: DataFrame of filtered lot with perfect state after matching
    """
    # Read the whole lot data
    lots_df = read_json_from_raw("lots")

    # Read the weighted estimation in perfect state data
    weighted_estimation_df = read_csv_from_raw("weighted_wine_estimations")

    # Get specific columns for lot dataframe
    lots_df = lots_df.loc[:, lots_df.columns.intersection(['_id', 'title', 'lowValuation.amount', 'highValuation.amount'
                                                              , 'minimumPrice.amount', 'createdAt.$date'])]

    # Create new column contains price which is the average of low and high valuation
    lots_df['price'] = lots_df.loc[:, ('lowValuation.amount', 'highValuation.amount')].mean(axis=1)
    lots_df['price'].fillna(lots_df['minimumPrice.amount'], inplace=True)

    lots_id_filter = weighted_estimation_df['lotId']
    filtered_lots_df = lots_df.query('_id in @lots_id_filter')
    return filtered_lots_df.info()

