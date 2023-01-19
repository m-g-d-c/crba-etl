import re

import bs4 as bs
import pandas as pd

from crba_project.cleanse import Cleanser
from crba_project.extractor import Extractor
from crba_project.normalize import scaler

class UnTreaties(Extractor):
    
    def __init__(self, config,**kwarg):
        super().__init__(config,**kwarg)
    
    def _download(self):
        # Get http request
        response = Extractor.api_request(self.address.strip())

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

    def _transform(self):
        
        # Save dataframe
        #self.dataframe.to_csv(
        #    self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
        #    sep = ";")

        # Cleansing
        # Log that we are entering cleasning
        # print("\n - - - - - \n Cleansing source {} \n".format(self.source_id))

        # Cleansing
        self.dataframe = Cleanser().rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all
        )

        # UN Treaty data specific: Sometimes, countries have footnotes (numbers). These must be purged for the rest of the code to work properly
        self.dataframe['COUNTRY_NAME'] = self.dataframe['COUNTRY_NAME'].apply(lambda x: re.sub('\s\d+.*', '', x)) # delete everything after number (and the leading whitespace)

        self.dataframe = Cleanser().add_and_discard_countries(
            grouped_data=self.dataframe,
            crba_country_list= self.config.country_crba_list,
            country_list_full = self.config.country_full_list
        )

        self.dataframe = Cleanser().add_cols_fill_cells(
            grouped_data_iso_filt=self.dataframe,
            dim_cols=self.config.sdmx_df_columns_dims,
            time_cols= self.config.sdmx_df_columns_time,
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

        self.dataframe = Cleanser().encode_ilo_un_treaty_data(
            dataframe = self.dataframe,
            treaty_source_body = self.source_body
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
            variable_type = self.value_labels,
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
