from io import StringIO

import pandas as pd
import numpy as np
from crba_project.cleanse import Cleanser

from crba_project.extractor import Extractor
from crba_project.normalize import scaler ##TODO Understand


class DefaultCSVExtractor(Extractor):
    """ """

    def __init__( self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        csv_data = Extractor.api_request(self.endpoint).text
        raw_data = pd.read_csv(StringIO(csv_data), sep=",")
        #TODO establish Great Expectation to check sources  
        return raw_data

    #TODO Transform this method. From if statements to subclasses
    def _transform(self):

        # Save raw data
        #self.dataframe.to_csv(
        #    self.config.data_sources_raw / str(self.source_id + "_raw.csv"), sep=";"
        #)

        # Log that we are entering cleasning
        #print("\n - - - - - \n Cleansing source {} \n".format(self.source_id))

        # Cleansing
        self.dataframe = Cleanser().extract_who_raw_data(
            raw_data=self.dataframe,
            variable_type=self.value_labels,
            display_value_col="Display Value",
        )

        # print(dataframe)
        # Exception: S-126 is a UNICEF API source, but has a different structure (repetitive columns) --> rename them so they are being included in the rename_and_discard_columns function
        if self.source_id == "S-126":
            self.dataframe = self.dataframe.rename(
                columns={
                    "Geographic area": "Geographic area_unused",
                    "Sex": "Sex_unused",
                    "AGE": "AGE_unused",
                }
            )
        else:
            pass

        self.dataframe = Cleanser().rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all,
        )

        # Specific to data from API (NRGI) --> Only two sources
        if self.source_type == "API (NRGI)":
            self.dataframe["RAW_OBS_VALUE"] = self.dataframe["RAW_OBS_VALUE"].apply(
                lambda x: np.nan if x == "." else x
            )

        self.dataframe = Cleanser().extract_year_from_timeperiod(
            dataframe=self.dataframe, year_col="TIME_PERIOD", time_cov_col="COVERAGE_TIME"
        )

        self.dataframe = Cleanser().retrieve_latest_observation(
            renamed_data=self.dataframe,
            dim_cols=self.config.sdmx_df_columns_dims,
            country_cols=self.config.sdmx_df_columns_country,
            time_cols=self.config.sdmx_df_columns_time,
            attr_cols=self.config.sdmx_df_columns_attr,
        )

        self.dataframe = Cleanser().add_and_discard_countries(
            grouped_data=self.dataframe,
            crba_country_list=self.config.country_crba_list,
            country_list_full=self.config.country_full_list,
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
            indicator_explanation_string=self.indicator_explanation,
            indicator_data_extraction_methodology_string=self.extraction_methodology,
            source_title_string=self.source_titel,
            source_api_link_string=self.endpoint,
            attribute_unit_string=self.unit_measure,
        )

        self.dataframe = Cleanser().map_values(
            cleansed_data=self.dataframe, value_mapping_dict=self.config.value_mapper
        )

        self.dataframe = Cleanser().encode_categorical_variables(
            dataframe=self.dataframe,
            encoding_string=self.value_encoding,
            encoding_labels=self.value_labels,
        )

        self.dataframe= Cleanser().create_log_report_delete_duplicates(
            cleansed_data=self.dataframe
        )

        # Append dataframe to combined dataframe
        # Dataframes get combined on the outter loop
        # combined_cleansed_csv = combined_cleansed_csv.append(other=dataframe_cleansed)

        # Save cleansed data
        #self.dataframe.to_csv(
        #    self.config.data_sources_cleansed / str(self.source_id+ "_cleansed.csv"), sep=";"
        #)

        # Normalizing
        self.dataframe = scaler.normalizer(
            cleansed_data=self.dataframe,
            sql_subset_query_string=self.dimension_values_normalization,
            # dim_cols=sdmx_df_columns_dims,
            variable_type=self.value_labels,
            is_inverted=self.invert_normalization,
            whisker_factor=1.5,
            raw_data_col="RAW_OBS_VALUE",
            scaled_data_col_name="SCALED_OBS_VALUE",
            maximum_score=10,
        )

        #self.dataframe.to_csv(
        #    self.config.data_sources_normalized
        #    / str(
        #        self.indicator_id
        #        + "_"
        #        + self.source_id
        #        + "_"
        #        + self.indicator_code
        #        + "_normalized.csv"
        #    ),
        #    sep=";",
        #)

        return self.dataframe