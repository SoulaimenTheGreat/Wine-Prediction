import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from src.data.toolbox import get_year, strip_accents
from src.data.preprocess_data import wine_estimator_perfect_state, lots_perfect_state
import numpy as np


class WineFuzzyMatch:
    """
    class to normalize wine names
    using fuzzy match with lists of already existing wine names
    """

    def __init__(self):
        """
        Initialize constructor containing available wine names
        """
        self.correct_options = self.get_wine_name('bordeaux') + self.get_wine_name('burgundy') + self.get_wine_name(
            'rhone') + self.get_wine_name('champagne') + self.get_wine_name('loire') + self.get_wine_name('jura') + \
                               self.get_wine_name('languedoc_roussillon') + self.get_wine_name(
            'armagnac') + self.get_wine_name('normandy') \
                               + self.get_wine_name('cognac') + self.get_wine_name(
            'south_west_france') + self.get_wine_name('beaujolais') \
                               + self.get_wine_name('provence') + self.get_wine_name('alsace') + self.get_wine_name(
            'savoie') + \
                               self.get_wine_name('corsica') + self.get_wine_name('portugal') + self.get_wine_name(
            'spain') + \
                               self.get_wine_name('italy') + self.get_wine_name('usa') + self.get_wine_name('germany') + \
                               self.get_wine_name('igp') + self.get_wine_name('uk') + self.get_wine_name(
            'vin_de_france_vin_de_table') + \
                               self.get_wine_name('china') + self.get_wine_name('hungary') + self.get_wine_name(
            'rest_world')

    @staticmethod
    def get_wine_name(filename):
        """
        return list containing the wine names based on the filename which represents the region
        :param filename: the name of the file that contains the correct names
        :return: list of wine names
        """
        df = pd.read_excel('../../data/external/wine-names/' + filename + '.xlsx')
        wine_names = df['wine_name'].tolist()
        return wine_names

    def checker(self, data, column: str):
        """
        Normalize wine names based on normalized wine name using the fuzzy algorithm
        :param data: DataFrame contains the wine names that should be normalized
        :param column: The column name that we want to normalize
        :return: excel file that contains the old name, normalized name and ratio
        """
        # Initialize variables
        correct_options = self.correct_options
        wrong_options = data[column]
        names_array = []
        vintage_array = []

        for wrong_option in wrong_options:
            if wrong_option in correct_options:
                names_array.append(wrong_option)
            else:
                x = process.extractOne(strip_accents(wrong_option), correct_options, scorer=fuzz.token_set_ratio)
                year = get_year(wrong_option)
                names_array.append(x[0])
                vintage_array.append(year[0])

        # Add normalized_name and vintage columns to the passed DataFrame
        data['normalized_name'] = np.asarray(names_array)
        data['vintage'] = np.asarray(vintage_array)
        return data


fuzzy_match = WineFuzzyMatch()
# df = wine_estimator_perfect_state()
df1 = lots_perfect_state()
# new_df = fuzzy_match.checker(df, "wineName")
new_df = fuzzy_match.checker(df1, "title")

print(new_df[['normalized_name','vintage','price']])
