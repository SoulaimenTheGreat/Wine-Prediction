import pandas as pd
from pandas import json_normalize
import json
from dagster import solid


@solid
def read_csv_from_raw(filename: str):
    """
    Reads CSV file from raw data by providing filename
    :param: filename is the name of the file to import
    :return: DataFrame contains the data
    """
    data_df = pd.read_csv("../../data/raw/" + filename + ".csv")
    return data_df


@solid
def read_json_from_raw(filename: str):
    """
    Read JSON file from raw data by providing filename then convert it to DataFrame
    :param: filename is the name of the file to import
    :return: DataFrame
    """
    with open("../../data/raw/" + filename + ".json", "r") as read_data:
        data_json = json.load(read_data)

    data_df = json_normalize(data_json)
    return data_df


@solid
def process_lots_perfect_state(lots_df, weighted_estimation_df):
    """
    Function that returns the lots in perfect state after fetching the weighted estimation in the perfect state and then
    filtered with the whole lot after matching with lot ids
    Create new column "price" which will contain the mean of low and high valuation
    :return: DataFrame of filtered lot with perfect state after matching
    """
    # Get specific columns for lot dataframe
    lots_df = lots_df.loc[:, lots_df.columns.intersection(['_id', 'title', 'lowValuation.amount', 'highValuation.amount'
                                                              , 'minimumPrice.amount', 'createdAt.$date'])]

    # Create new column contains price which is the average of low and high valuation
    lots_df['price'] = lots_df.loc[:, ('lowValuation.amount', 'highValuation.amount')].mean(axis=1)
    lots_df['price'].fillna(lots_df['minimumPrice.amount'], inplace=True)

    lots_id_filter = weighted_estimation_df['lotId']
    filtered_lots_df = lots_df.query('_id in @lots_id_filter')
    filtered_lots_df.reset_index(drop=True, inplace=True)
    return filtered_lots_df.loc[:, filtered_lots_df.columns.intersection(['title','price', 'createdAt.$date'])]


@solid
def process_estimator_perfect_state(wine_estimator_df):
    """
    Load data from raw folder of wine estimations
    Create new column "price" which will contain the mean of min and max price of corrected or wine searcher estimation
    :return: DataFrame of filtered filtered estimations with perfect state
    """
    wine_estimator_df = wine_estimator_df.loc[:, wine_estimator_df.columns.intersection(['wineName', 'correctedMin',
                                                                                         'correctedMax',
                                                                                         'wineSearcherMin',
                                                                                         'wineSearcherMax',
                                                                                         'idealWinePrice', 'date'])]

    # Convert corrected min and max to hundreds to be standardized with the other prices
    wine_estimator_df['correctedMin'], wine_estimator_df['correctedMax'] = wine_estimator_df['correctedMin'] * 100,\
                                                                           wine_estimator_df['correctedMax'] * 100

    # Create new column contains price which is the average of min and max corrected price
    # If there is no corrected price fill it with mean of wine searcher min and max idealWinePrice
    # else fill it with idealwine price
    wine_estimator_df['price'] = wine_estimator_df.loc[:, ('correctedMin', 'correctedMax')].mean(axis=1)
    wine_estimator_df['price'].fillna(wine_estimator_df.loc[:, ('wineSearcherMin', 'wineSearcherMax')].mean(axis=1),
                                      inplace=True)
    wine_estimator_df['price'].fillna(wine_estimator_df['idealWinePrice'], inplace=True)

    return wine_estimator_df.loc[:, wine_estimator_df.columns.intersection(['wineName','price', 'date'])]


@solid
def process_wine_quotation(wine_quotation_df):
    """
    Load data from raw folder of wine quotaions table
    :return: DataFrame that contains filtered columns
    """
    # Read data from raw folder and pick specific columns
    wine_quotation_df = wine_quotation_df.loc[:, wine_quotation_df.columns.intersection(
        ['name', 'field_vintage_value', 'field_price_amount', 'field_date_value'])]
    return wine_quotation_df
