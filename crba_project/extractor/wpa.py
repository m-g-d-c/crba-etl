import pandas as pd

from crba_project.cleanse import Cleanser
from crba_project.extractor import Extractor
from crba_project.normalize import scaler

class WPA_Extractor(Extractor):
     
    def __init__(self,config, WPA_YEAR_COL,WPA_OBS_RAW_COL,**kwarg):
        super().__init__(config,**kwarg)

        self.wpa_year_col = WPA_YEAR_COL
        self.wpa_obs_raw_col = WPA_OBS_RAW_COL
    
    def _download(self):
        if not hasattr(WPA_Extractor, 'wpa_combined') or WPA_Extractor.wpa_combined is None:
            # 1. Create a flat file of all WPA sources
            # Read and join all world policy analysis centre data
            wpa_child_labor = pd.read_excel(
                io = self.config.data_sources_raw_manual_machine / 'S_8, S_9' / 'WORLD_child_labor.xls'
            )

            wpa_childhood = pd.read_excel(
                io = self.config.data_sources_raw_manual_machine / 'S_10, S_13, S_36, S_45, S_49' / 'WORLD_Dataset_Childhood_4.16.15.xls'
            )

            wpa_adult_labor = pd.read_excel(
                io = self.config.data_sources_raw_manual_machine / 'S_40, S_41, S_63, S_64, S_65, S_66, S_67, S_68' / 'WORLD_Dataset_Adult_Labor_9.17.2018.xls'
            )

            wpa_discrimination = pd.read_excel(
                io = self.config.data_sources_raw_manual_machine / 'S_42, S_43, S_44' / 'WORLD_discrimination_at_work.xls'
            )
            
            # Create list to write a loop
            wpa_combined_list=[
                #wpa_child_labor,
                wpa_childhood,
                wpa_adult_labor,
                wpa_discrimination
            ]

            # Loop to join all dataframes
            wpa_combined = wpa_child_labor

            for df in wpa_combined_list:
                wpa_combined = wpa_combined.merge(
                    right=df,
                    on=['iso2', 'iso3']
                )
            WPA_Extractor.wpa_combined=wpa_combined
            # Hope this do the same ....
            #WPA_Extractor.wpa_combined_list = pd.concat(wpa_combined_list,
            #axis=1, # Concat columns not rows aka merge 
            #)
        return WPA_Extractor.wpa_combined

    def _transform(self):

        self.dataframe['TIME_PERIOD'] = self.wpa_year_col 
        self.dataframe = self.dataframe[['iso3', self.wpa_obs_raw_col]] 
        #print(dataframe.head(30))

        # Save dataframe
        #self.dataframe.to_csv(
        #    self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
        #    sep = ";")
        #except:
        #print("There was an issue with source {}".format(self.source_id ))
        
        # Log that we are entering cleasning
        #print("\n - - - - - \n Cleansing source {} \n".format(self.source_id ))
        
        # Cleansing 
        #print("\n - - - - - \n Cleansing source {} \n".format(self.source_id ))

        self.dataframe = Cleanser().rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all
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
            indicator_explanation_string=self.indicator_explanation,
            indicator_data_extraction_methodology_string=self.extraction_methodology,
            source_title_string=self.source_titel,
            source_api_link_string=self.endpoint,
            attribute_unit_string=self.unit_measure
        )

        self.dataframe_cleansed = Cleanser().encode_categorical_variables(
            dataframe = self.dataframe,
            encoding_string = self.value_encoding,
            encoding_labels = self.value_labels
        )

        self.dataframe_cleansed = Cleanser().create_log_report_delete_duplicates(
            cleansed_data=self.dataframe_cleansed
        )

        
        # Append dataframe to combined dataframe
        #combined_cleansed_csv = combined_cleansed_csv.append(
        #    other = dataframe_cleansed
        #)
        

        # Save cleansed data
        #self.dataframe_cleansed.to_csv(
        #    self.config.data_sources_cleansed / str(self.source_id + "_cleansed.csv"),
        #    sep = ";")

        # Normalizing
        self.dataframe_normalized = scaler.normalizer(
            cleansed_data = self.dataframe_cleansed,
            sql_subset_query_string=self.dimension_values_normalization,
            # dim_cols=sdmx_df_columns_dims,
            variable_type = self.value_labels,
            is_inverted = self.invert_normalization,
            whisker_factor=1.5,
            raw_data_col="RAW_OBS_VALUE",
            scaled_data_col_name="SCALED_OBS_VALUE",
            maximum_score=10,
            )

        #self.dataframe_normalized.to_csv(
        #    self.config.data_sources_normalized / str(self.indicator_id + '_' + self.source_id + '_' +self.indicator_code + "_normalized.csv"),
        #    sep = ";")

        # Append dataframe to combined dataframe
        #combined_normalized_csv = combined_normalized_csv.append(
        #    other = dataframe_normalized
        #)
        return self.dataframe_normalized