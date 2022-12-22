import bs4 as bs
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from crba_project.cleanse import Cleanser
from crba_project.extractor import Extractor
from crba_project.normalize import scaler



class ILO_Extractor(Extractor):
    
    def __init__( self,config,**kwarg):
        super().__init__(config,**kwarg)
        
        options = Options()
        options.headless = True
        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), chrome_options=options)

    def _download(self):
        response = self.driver.get(self.address)
        soup = bs.BeautifulSoup(self.driver.page_source,features="lxml")
        target_table = str(
        soup.find_all("table", {"cellspacing": "0", "class": "horizontalLine"})
        )

        # Create dataframe with the data
        self.dataframe  = pd.read_html(io=target_table, header=0)[
            0
        ]
        return self.dataframe 

    def _transform(self):

# return is a list of DFs, specify [0] to get actual DF

        # Save raw data (as actual raw, rather than staged raw data)
        #self.dataframe.to_csv(
        #    self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
        #    sep = ";"
        #    )

        # Log that we are entering cleasning
        #print("\n - - - - - \n Cleansing source {} \n".format(self.source_id))

        # Cleansing
        self.dataframe = Cleanser().rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all
        )

        self.dataframe = Cleanser().decompose_country_footnote_ilo_normlex(
            dataframe = self.dataframe,
            country_name_list = self.config.country_full_list["COUNTRY_NAME"]
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

        self.dataframe = Cleanser().encode_ilo_un_treaty_data(
            dataframe = self.dataframe,
            treaty_source_body='ILO NORMLEX'
        )

        # Save cleansed data
        #self.dataframe.to_csv(
        #    self.config.data_sources_cleansed / str(self.source_id + "_cleansed.csv"),
        #    sep = ";")

        # Append dataframe to combined dataframe
        #combined_cleansed_csv = combined_cleansed_csv.append(
        #    other = dataframe_cleansed
        #)

        # Create log info
        self.dataframe = Cleanser().create_log_report_delete_duplicates(
            cleansed_data=self.dataframe
        )

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
            log_info=True
            )

        #self.dataframe.to_csv(
        #    self.config.data_sources_normalized / str(self.indicator_id + '_' + self.source_id + '_' + self.indicator_code + "_normalized.csv"),
        #    sep = ";")

        return self.dataframe