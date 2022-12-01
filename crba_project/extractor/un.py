from crba_project.extractor import Extractor
import bs4 as bs
import pandas as pd
import re
from crba_project import cleanse
from normalize import scaler 

class UnTreaties(Extractor):
    
    
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

    def _extract(self):
        # Exraction section
        dataframe = self.data()
        
        # Save dataframe
        dataframe.to_csv(
            self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
            sep = ";")

        # Cleansing
        # Log that we are entering cleasning
        # print("\n - - - - - \n Cleansing source {} \n".format(self.source_id))

        # Cleansing
        dataframe = cleanse.Cleanser().rename_and_discard_columns(
            raw_data=dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all
        )

        # UN Treaty data specific: Sometimes, countries have footnotes (numbers). These must be purged for the rest of the code to work properly
        dataframe['COUNTRY_NAME'] = dataframe['COUNTRY_NAME'].apply(lambda x: re.sub('\s\d+.*', '', x)) # delete everything after number (and the leading whitespace)

        dataframe = cleanse.Cleanser().add_and_discard_countries(
            grouped_data=dataframe,
            crba_country_list= self.config.country_crba_list,
            country_list_full = self.config.country_full_list
        )

        dataframe = cleanse.Cleanser().add_cols_fill_cells(
            grouped_data_iso_filt=dataframe,
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

        dataframe_cleansed = cleanse.Cleanser().encode_ilo_un_treaty_data(
            dataframe = dataframe,
            treaty_source_body = self.source_body
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
            variable_type = self.value_labels,
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
