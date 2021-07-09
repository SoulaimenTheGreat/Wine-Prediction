# import motor.motor_asyncio
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd


def load_env_variables():
    """
    Function to load environment variables from .env file
    :return: database password and database name
    """
    load_dotenv()
    database_password = os.environ.get('PASSWORD')
    database_name = os.environ.get('DATABASE')
    return database_password, database_name


def configure_database_collection(collection_name):
    """
    Configure the database connection, database and collection
    :return: the collection
    """
    database_password, database_name = load_env_variables()
    MONGO_DETAILS = "mongodb+srv://admin:" + database_password + "@wineestimations.ycvrd.mongodb.net/" + database_name + \
                    "?retryWrites=true "
    client = MongoClient(MONGO_DETAILS)
    database = client[database_name]
    collection = database.get_collection(collection_name)
    return collection


# def estimation_helper(estimation) -> dict:
#     return {
#         "id": str(estimation["_id"]),
#         "wineName": estimation["wineName"],
#         "designation": estimation["designation"],
#         "vineyard": estimation["vineyard"],
#         "cuvee": estimation["cuvee"],
#         "bottleType": estimation["bottleType"],
#         "color": estimation["color"],
#         "vintage": estimation["vintage"],
#         "wineSearcherMin": estimation["wineSearcherMin"],
#         "wineSearcherMax": estimation["wineSearcherMax"],
#         "idealWinePrice": estimation["idealWinePrice"],
#         "correctedMin": estimation["correctedMin"],
#         "correctedMax": estimation["correctedMax"],
#         "weightedMin": estimation["weightedMin"],
#         "weightedMax": estimation["weightedMax"],
#         "wineLevel": estimation["wineLevel"],
#         "label": estimation["label"],
#         "cap": estimation["cap"],
#         "limpidity": estimation["limpidity"],
#         "date": estimation["date"],
#     }


def retrieve_filtered_estimations(collection_name: str, condition: dict):
    collection = configure_database_collection(collection_name)
    filtered_estimations = []
    for estimation in collection.find(condition):
        filtered_estimations.append(estimation)
    return filtered_estimations


def convert_to_csv(collection_name: str, condition: dict, filename: str):
    records = retrieve_filtered_estimations(collection_name, condition)
    records_df = pd.DataFrame.from_records(records)
    records_df.to_csv(path_or_buf="/home/soulaimen/PycharmProjects/wineestimation/data/raw/" + filename + ".csv",
                      index=False)


# weighted_estimations = retrieve_filtered_estimations("add_weight_collection", {"updatedWeight": True, "caps_score": 1,
#                                                                                "label_score": 1, "limpidity_score": 1,
#                                                                                "wineLevel_score": 1})
# print(estimations)
# convert_to_csv("estimations_collection", {"wineLevel": 1, "label": 1, "cap": 1, "limpidity": 1}, "wine_estimations")
convert_to_csv("add_weight_collection", {"updatedWeight": True, "caps_score": 1, "label_score": 1, "limpidity_score": 1,
                                         "wineLevel_score": 1}, "weighted_wine_estimations")
