from abc import ABC, abstractmethod
import logging

import requests
import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult

from crba_project.conf import Config

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)

class ExtractionError(Exception):
    def __init__(self, message, SOURCE_ID):
        super().__init__(message)
        self.source_id = SOURCE_ID
    pass

class Extractor(ABC):
    """
    Maybe subcalss from Pandas Dataframe?!?!?
    """

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
    
    @abstractmethod
    def __init__(
        self,
        config:Config,
        SOURCE_ID,
        SOURCE_TYPE,
        ENDPOINT_URL,
        SOURCE_TITLE,
        VALUE_LABELS,
        INDICATOR_NAME_y,
        INDEX,
        ISSUE,
        CATEGORY,
        INDICATOR_CODE,
        ADDRESS,
        SOURCE_BODY,
        INDICATOR_DESCRIPTION,
        INDICATOR_EXPLANATION,
        EXTRACTION_METHODOLOGY,
        UNIT_MEASURE,
        VALUE_ENCODING,
        DIMENSION_VALUES_NORMALIZATION,
        INVERT_NORMALIZATION,
        INDICATOR_ID, **kwargs
    ):

        self.config = config
        self.source_id = SOURCE_ID
        self.source_type = SOURCE_TYPE
        self.source_titel = SOURCE_TITLE
        self.endpoint = ENDPOINT_URL
        self.value_labels = VALUE_LABELS
        self.indicator_name_y = INDICATOR_NAME_y
        self.index = INDEX
        self.issue = ISSUE
        self.category = CATEGORY
        self.indicator_code = INDICATOR_CODE
        self.address = ADDRESS
        self.source_body = SOURCE_BODY
        self.indicator_description = INDICATOR_DESCRIPTION
        self.indicator_explanation = INDICATOR_EXPLANATION
        self.extraction_methodology = EXTRACTION_METHODOLOGY
        self.unit_measure = UNIT_MEASURE
        self.value_encoding = VALUE_ENCODING
        self.dimension_values_normalization = DIMENSION_VALUES_NORMALIZATION
        self.invert_normalization = INVERT_NORMALIZATION
        self.indicator_id = INDICATOR_ID

                    

    def download(self):
        self.dataframe = self._download()
        
        #TODO establish Great Expectation to validate sources  
        assert len(self.dataframe) > 0, "The source has not provided any data  "
        

        return self

    def transform(self):
        ##TODO do not reassign Dataframe but instead edit in place
        self.dataframe = self._transform()
        self.dataframe["SOURCE_ID"] = self.source_id
        return self


    # def run_greate_expectation_checkpoint(self):
    #     batch_request = RuntimeBatchRequest(
    #         datasource_name="default_datasource",
    #         data_connector_name="default_runtime_data_connector",
    #         data_asset_name=self.source_id,  # This can be anything that identifies this data_asset for you
    #         runtime_parameters={"batch_data": self.dataframe},  # df is your dataframe
    #         batch_identifiers={"default_identifier_name": "default_identifier"},
    #     )

    #     result: CheckpointResult = self.config.ge_context.run_checkpoint(
    #         checkpoint_name="indicator_sdmx_checkpoint",
    #         validations=batch_request,
    #         run_name=f"{self.source_id}-{self.config.run_id}",
    #     )
    #     if not result["success"]:
    #         log.warn("Validation failed!")
            


    def get(self):
        """
        Good pattern is to only create new columns but not delete old ones. 
        While the data is small enough this pattern helps debugging
        """
        try:
            self.download() \
                .transform()

            #self.run_greate_expectation_checkpoint()
            return self.dataframe
        except Exception as ex:
            #Store all Data processed until now to help debugging
            if hasattr(self,'dataframe'):
                self.dataframe.to_csv(
                    self.config.error_folder / str(self.source_id + ".csv"),
                    sep = ";")
            raise ExtractionError(
                f"Source {self.source_id} failed to extract cause of: {str(ex)}",self.source_id
            ) from ex
            



class EmptyExtractor(Extractor):
    def __init__(self, SOURCE_ID, **kwarg):
       self.source_id = SOURCE_ID

    def _download(self):
        raise NotImplementedError(f"For {self.source_id} no download method defined")

    def _transform(self):
        raise NotImplementedError(f"For {self.source_id} no transformations are defined")
