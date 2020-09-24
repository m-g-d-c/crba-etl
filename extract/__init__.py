from abc import ABC, abstractmethod, abstractproperty

import pandas as pd
import requests


class Extractor(ABC):
    """Abstract base class for extractor classes
    """

    type = None

    @classmethod
    def content_type(cls):
        return cls.type

    @classmethod
    @abstractmethod
    def data(cls, url):
        """Performs the actual extraction of the data from the provided URL 

        Args:
            url (string): URL to fetch the data from

        Returns:
            DataFrame: DataFrame contianing the data
        """
        raise NotImplementedError

    @classmethod
    def extract(cls, url):

        raw_data = cls.data(url)

        # Log functionality for user
        print(
            "The following columns are present in the datasets, and this is the number of unique values they have. "
        )
        for col in raw_data:
            print(
                "The column {} has {} unique values.".format(
                    col, raw_data[col].astype(str).nunique()
                )
            )

        # Return pandas dataframe
        return raw_data


class CSVExtractor(Extractor):

    type = "csv"

    @classmethod
    def data(cls, url):

        raw_data = pd.read_csv(url, sep=",",)

        return raw_data


class JSONExtractor(Extractor):

    type = "json"

    @classmethod
    def data(cls, url):
        # Extract data and convert to pandas dataframe
        raw_data = pd.json_normalize(requests.get(url).json()["data"])

        return raw_data

