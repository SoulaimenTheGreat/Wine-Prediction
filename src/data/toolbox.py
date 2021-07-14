import re
import unicodedata


def get_year(name: str):
    """
    function to extract year from input name
    :param name: text to extract from
    :return: year if the regex pattern exists else return void
    """
    year = re.findall(r"\d{4}", name)
    return year or " "


def strip_accents(text):
    """
    Strip accents from input String.

    :param text: The input string.
    :type text: String.

    :returns: The processed String.
    :rtype: String.
    """
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text).lower()
