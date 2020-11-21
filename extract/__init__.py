from io import StringIO
from abc import ABC, abstractmethod, abstractproperty

import pandas as pd
import requests
import bs4 as bs


class Extractor(ABC):
    """Abstract base class for extractor classes"""

    type = None
    headers = {}

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
    def api_request(cls, address, params=None, headers=None):
        try:
            response = requests.get(address, params=params, headers=headers)
            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as error:
            print(f"Other error occurred: {error}")
        # return response object
        return response

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

        csv_data = cls.api_request(url, headers=cls.headers).text

        raw_data = pd.read_csv(StringIO(csv_data), sep=",")

        return raw_data


class JSONExtractor(Extractor):

    type = "json"

    @classmethod
    def data(cls, url):
        # Extract data and convert to pandas dataframe
        try:
            # Most json data is from SDG; which deturn json with key "data" having the data as value
            raw_data = pd.json_normalize(requests.get(url).json()["data"])
        except:
            # However, some of the data is also from World Bank where the command returns list, which must be subset with list index
            raw_data = pd.json_normalize(
                requests.get(url).json()[1]
            )  # 0 is metadata, 1 contains actual data)

        return raw_data


class SDMXExtractor(CSVExtractor):

    type = "sdmx-csv"

    headers = {
        "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
        "Accept-Encoding": "gzip",
    }


class HTMLExtractor(Extractor):

    type = "html"

    @classmethod
    def data(cls, url):
        # Get http request
        response = cls.api_request(url.strip())

        # Soupify the actual html content
        soup = bs.BeautifulSoup(response.text, features="lxml")

        # Extract the target table as attribute
        target_table = str(
            soup.find_all(
                "table",
                {
                    "class": "table table-striped table-bordered table-hover table-condensed"
                },
            )
        )

        # Create dataframe with the data
        raw_data = pd.read_html(io=target_table, header=0)[
            0
        ]  # return is a list of DFs, specify [0] to get actual DF

        # Return result
        return raw_data


class SeleniumExtractor(Extractor):

    type = "html-selenium"

    @classmethod
    def data(cls, url):
        # Specify location to chrome driver
        # TO DO

        # Add option to make it headless (so that it doesn't open an actual chrome window)
        options = Options()
        options.headless = True
        driver = webdriver.Chrome(driver_location, chrome_options=options)

        # Get HTTP response
        response = driver.get(urlstrip())

        # Retrieve the actual html
        html = driver.page_source

        # Soupify
        soup = bs.BeautifulSoup(html)

        # Extract the target table as attribute
        target_table = str(
            soup.find_all("table", {"cellspacing": "0", "class": "horizontalLine"})
        )

        # Create dataframe with the data
        raw_data = pd.read_html(io=target_table, header=0)[
            0
        ]  # return is a list of DFs, specify [0] to get actual DF

        # Return result
        return raw_data
