import pandas as pd
import requests

from crba_project.cleanse import Cleanser
from crba_project.extractor import Extractor
from crba_project.normalize import scaler


class DefaultJsonExtractor(Extractor):

    def __init__(self,config, NA_ENCODING,**kwarg):
        super().__init__(config,**kwarg)

        self.na_encoding = NA_ENCODING

    
    def _download(self):
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

    def _transform(self):

        # Save dataframe
        #self.dataframe.to_csv(
        #    self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
        #    sep = ";")
        
        # Log that we are entering cleasning
        # print("\n - - - - - \n Cleansing source {} \n".format(self.source_id))
        
        # Cleansing in 
        self.dataframe = Cleanser().extract_who_raw_data(
            raw_data=self.dataframe,
            variable_type = self.value_labels,
            display_value_col="Display Value"
        )
        
        self.dataframe = Cleanser().rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all
        )

        self.dataframe = Cleanser().convert_nan_strings_into_nan(
            dataframe = self.dataframe
        )

        self.dataframe = Cleanser().extract_year_from_timeperiod(
            dataframe=self.dataframe,
            year_col="TIME_PERIOD",
            time_cov_col="COVERAGE_TIME"
        )

        self.dataframe = Cleanser().retrieve_latest_observation(
            renamed_data=self.dataframe,
            dim_cols = self.config.sdmx_df_columns_dims,
            country_cols = self.config.sdmx_df_columns_country,
            time_cols = self.config.sdmx_df_columns_time,
            attr_cols=self.config.sdmx_df_columns_attr,
        )

        self.dataframe = Cleanser().add_and_discard_countries(
            grouped_data=self.dataframe,
            crba_country_list=self.config.country_crba_list,
            country_list_full = self.config.country_full_list
        )

        self.dataframe = Cleanser().add_cols_fill_cells(
            grouped_data_iso_filt=self.dataframe,
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

        self.dataframe = Cleanser().map_values(
            cleansed_data = self.dataframe,
            value_mapping_dict = self.config.value_mapper
        )
        
        self.dataframe = Cleanser().encode_categorical_variables(
            dataframe = self.dataframe,
            encoding_string = self.value_encoding,
            encoding_labels = self.value_labels,
            na_encodings = self.na_encoding
        )

        self.dataframe = Cleanser().create_log_report_delete_duplicates(
            cleansed_data=self.dataframe
        )

        # Append dataframe to combined dataframe
        #combined_cleansed_csv = combined_cleansed_csv.append(
        #    other = dataframe_cleansed
        #)

        # Save cleansed data
        #self.dataframe.to_csv(
        #    self.config.data_sources_cleansed / str(self.source_id + "_cleansed.csv"),
        #    sep = ";")
        
        # Normalizing section
        self.dataframe = scaler.normalizer(
            cleansed_data = self.dataframe,
            sql_subset_query_string=self.dimension_values_normalization,
            # dim_cols=sdmx_df_columns_dims,
            variable_type =self.value_labels,
            is_inverted = self.invert_normalization,
            whisker_factor=1.5,
            raw_data_col="RAW_OBS_VALUE",
            scaled_data_col_name="SCALED_OBS_VALUE",
            maximum_score=10,
            )
        
        #self.dataframe.to_csv(
        #    self.config.data_sources_normalized / str(self.indicator_id + '_' + self.source_id + '_' + self.indicator_code + "_normalized.csv"),
        #    sep = ";")
            
        return self.dataframe
