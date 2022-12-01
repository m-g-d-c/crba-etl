from crba_project import cleanse
from crba_project.extractor import Extractor
import pandas as pd
import requests
from normalize import scaler ##TODO Understand

class DefaultJsonExtractor(Extractor):

    def __init__(
        self,
        config,
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
        INDICATOR_ID,
        NA_ENCODING,
        
        **kwarg
    ):
        """
        Maybe use:
        for key in kwargs:
            setattr(self, key, kwargs[key])
        less readable but shorter code
        """
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
        self.na_encoding = NA_ENCODING


    def data(self):
        # Extract data and convert to pandas dataframe
        try:
            # Most json data is from SDG; which deturn json with key "data" having the data as value
            raw_data = pd.json_normalize(requests.get(self.endpoint).json()["data"])
        except:
            # However, some of the data is also from World Bank where the command returns list, which must be subset with list index
            raw_data = pd.json_normalize(
                requests.get(self.endpoint).json()[1]
            )  # 0 is metadata, 1 contains actual data)

        return raw_data

    def _extract(self):
        dataframe = self.data()
        # Save dataframe
        dataframe.to_csv(
            self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
            sep = ";")
        
        # Log that we are entering cleasning
        # print("\n - - - - - \n Cleansing source {} \n".format(self.source_id))
        
        # Cleansing in 
        dataframe = cleanse.Cleanser().extract_who_raw_data(
            raw_data=dataframe,
            variable_type = self.value_labels,
            display_value_col="Display Value"
        )
        
        dataframe = cleanse.Cleanser().rename_and_discard_columns(
            raw_data=dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all
        )

        dataframe = cleanse.Cleanser().convert_nan_strings_into_nan(
            dataframe = dataframe
        )

        dataframe = cleanse.Cleanser().extract_year_from_timeperiod(
            dataframe=dataframe,
            year_col="TIME_PERIOD",
            time_cov_col="COVERAGE_TIME"
        )

        dataframe = cleanse.Cleanser().retrieve_latest_observation(
            renamed_data=dataframe,
            dim_cols = self.config.sdmx_df_columns_dims,
            country_cols = self.config.sdmx_df_columns_country,
            time_cols = self.config.sdmx_df_columns_time,
            attr_cols=self.config.sdmx_df_columns_attr,
        )

        dataframe = cleanse.Cleanser().add_and_discard_countries(
            grouped_data=dataframe,
            crba_country_list=self.config.country_crba_list,
            country_list_full = self.config.country_full_list
        )

        dataframe = cleanse.Cleanser().add_cols_fill_cells(
            grouped_data_iso_filt=dataframe,
            dim_cols=self.config.sdmx_df_columns_dims,
            time_cols=self.config.sdmx_df_columns_time,
            indicator_name_string=self.indicator_name_y,
            index_name_string=self.index,
            issue_name_string=self.issue,
            category_name_string=self.category,
            indicator_code_string=self.indicator_code,
            indicator_source_string=self.address,
            indicator_source_body_string=self.source_body,
            indicator_description_string=self.indicator_description,
            source_title_string=self.source_titel,
            indicator_explanation_string=self.indicator_explanation,
            indicator_data_extraction_methodology_string=self.extraction_methodology,
            source_api_link_string=self.endpoint,
            attribute_unit_string=self.unit_measure
        )

        dataframe = cleanse.Cleanser().map_values(
            cleansed_data = dataframe,
            value_mapping_dict = self.config.value_mapper
        )
        
        dataframe_cleansed = cleanse.Cleanser().encode_categorical_variables(
            dataframe = dataframe,
            encoding_string = self.value_encoding,
            encoding_labels = self.value_labels,
            na_encodings = self.na_encoding
        )

        dataframe_cleansed = cleanse.Cleanser().create_log_report_delete_duplicates(
            cleansed_data=dataframe_cleansed
        )

        # Append dataframe to combined dataframe
        #combined_cleansed_csv = combined_cleansed_csv.append(
        #    other = dataframe_cleansed
        #)

        # Save cleansed data
        dataframe_cleansed.to_csv(
            self.config.data_sources_cleansed / str(self.source_id + "_cleansed.csv"),
            sep = ";")
        
        # Normalizing section
        dataframe_normalized = scaler.normalizer(
            cleansed_data = dataframe_cleansed,
            sql_subset_query_string=self.dimension_values_normalization,
            # dim_cols=sdmx_df_columns_dims,
            variable_type =self.value_labels,
            is_inverted = self.invert_normalization,
            whisker_factor=1.5,
            raw_data_col="RAW_OBS_VALUE",
            scaled_data_col_name="SCALED_OBS_VALUE",
            maximum_score=10,
            )
        
        dataframe_normalized.to_csv(
            self.config.data_sources_normalized / str(self.indicator_id + '_' + self.source_id + '_' + self.indicator_code + "_normalized.csv"),
            sep = ";")
            
        return dataframe_normalized
