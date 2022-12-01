from abc import ABC
import requests
import pandas as pd
import logging

log = logging .getLogger(__name__)


class Extractor(ABC):

    @classmethod
    def api_request(cls, address, params=None, headers=None):
        """
        Dont catch exceptions. When erros occured the extraction should faile
        """
        response = requests.get(address, params=params, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        # return response object
        return response

    def data(self):
        return pd.DataFrame()
    def extract(self):
        try:
            df = self._extract()
            return df
        except Exception as ex:
            raise ExtractionError(f"Source {self.source_id} failed to extract")

class ExtractionError(Exception):
    pass


class EmptyExtractor(Extractor):
    
    def __init__(self,SOURCE_ID,**kwarg):
        self.source_id = SOURCE_ID

    def _extract(self):
        return None