import datetime
from io import StringIO

import requests
import pandas as pd
import numpy as np
import re

from crba_project.cleanse import Cleanser
from crba_project.extractor import Extractor
from crba_project.normalize import scaler

class ManuelExtractor(Extractor):
    """
    Manually extracted data (both human and machine-generated data) + data which required pre-processing
    Normal ETL-pipeline
    """

    def _transform(self):
        
        # Cleansing
        self.dataframe = Cleanser().extract_who_raw_data(
            raw_data=self.dataframe,
            variable_type = self.value_labels,
            display_value_col="Display Value"
        )
        
        #print(dataframe)
        # Exception: S-126 is a UNICEF API source, but has a different structure (repetitive columns) --> rename them so they are being included in the rename_and_discard_columns function
        if self.source_id == 'S-126':
            self.dataframe = self.dataframe.rename(
                columns = {
                    'Geographic area' : 'Geographic area_unused',
                    'Sex' : 'Sex_unused',
                    'AGE' : 'AGE_unused'
                }
            )
        else:
            pass

        self.dataframe = Cleanser().rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=self.config.mapping_dict,
            final_sdmx_col_list=self.config.sdmx_df_columns_all
        )

        # Specific to data from API (NRGI) --> Only two sources
        if self.source_type == "API (NRGI)":
            self.dataframe['RAW_OBS_VALUE'] = self.dataframe['RAW_OBS_VALUE'].apply(
                lambda x: np.nan if x == '.' else x
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
            attr_cols= self.config.sdmx_df_columns_attr,
        )
        
        self.dataframe = Cleanser().add_and_discard_countries(
            grouped_data=self.dataframe,
            crba_country_list= self.config.country_crba_list,
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

        self.dataframe = Cleanser().map_values(
            cleansed_data = self.dataframe,
            value_mapping_dict = self.config.value_mapper
        )
        
        self.dataframe = Cleanser().encode_categorical_variables(
            dataframe = self.dataframe,
            encoding_string = self.value_encoding,
            encoding_labels = self.value_labels
        )

        self.dataframe = Cleanser().create_log_report_delete_duplicates(
            cleansed_data=self.dataframe
        )

        # Append dataframe to combined dataframe
        #self.dataframe = combined_cleansed_csv.append(
        #    other = self.dataframe
        #)

        # Save cleansed data
        #self.dataframe.to_csv(
        #    self.data_sources_cleansed / str(self.source_id + "_cleansed.csv"),
        #    sep = ";")
        
        # Normalizing
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
        return self.dataframe
        

class HumanEnteredExtractor(ManuelExtractor):

    
    def __init__(self,config,RAW_OBS_VALUE_TYPE, **kwarg):
        super().__init__(config,**kwarg)

        self.raw_obs_value_type = RAW_OBS_VALUE_TYPE


    def _download(self):
        self.dataframe = pd.read_excel( self.endpoint, sheet_name="Blueprint")
        raw_obs_value_col = "RAW_OBS_VALUE"
        if self.raw_obs_value_type == "categorical":
        # Delete trailing whitespace and numbers of parentheses in raw_OBS_VALUE
            self.dataframe[raw_obs_value_col] = (
                self.dataframe[raw_obs_value_col]
                .apply(lambda x: re.sub(" \(\d+\)", "", x) if type(x) == str else x)
                .apply(lambda x: x.strip() if type(x) == str else x)
            )

            # Encode missing data as "No data"
            self.dataframe.loc[
                self.dataframe[raw_obs_value_col].isnull(), "RAW_OBS_VALUE"
            ] = "No data"

        # Section dealing with numeric variables
        elif self.raw_obs_value_type == "continuous":
            self.dataframe.loc[
                (self.dataframe[raw_obs_value_col] == "No data")
                | (self.dataframe[raw_obs_value_col] == "Insufficient data")
                | (self.dataframe[raw_obs_value_col] == "No legal measures ")
                | (self.dataframe[raw_obs_value_col] == "x"),
                raw_obs_value_col,
            ] = np.nan

        else:
            print("Must specify what type of variable it is")
        # Rename COUNTRY_NAME column, to avoid trouble of ETL pipeline down the line
        self.dataframe = self.dataframe.rename(columns={"COUNTRY_NAME": "country_col_not_used"})

        # print(dataframe.RAW_OBS_VALUE.unique())

        self.dataframe = self.dataframe.dropna(axis="columns", how="all")
        return self.dataframe



class IDMC_Extractor(ManuelExtractor):
    """
    S-180, S-181, S-189, S-230
    """

    IDMC_Extractor_Source = dict()

    def __init__(self,config, ATTR_UNIT_MEASURE,**kwarg):
        super().__init__(config,**kwarg)
        self.attr_unit_measure = ATTR_UNIT_MEASURE


    def _download(self):
        if self.source_id not in IDMC_Extractor.IDMC_Extractor_Source.keys():
            #TODO change loop into indicator Excel...
            S_180_S_181_S189_S_230 = pd.read_excel(
                self.config.data_sources_raw_manual_machine
                / "S-180, S-181, S-189 S-230 idmc_displacement_all_dataset.xlsx"
            ).drop(
                0
            )  # delete first row containing strings

            # Cast year as string, required for merge command later
            S_180_S_181_S189_S_230["Year"] = S_180_S_181_S189_S_230["Year"].astype(str)

            # Join raw data and population data together
            S_180_S_181_S189_S_230_raw = self.config.un_pop_tot.merge(
                right=S_180_S_181_S189_S_230,
                how="right",
                # on="ISO3_YEAR"
                left_on=["COUNTRY_ISO_3", "year"],
                right_on=["ISO3", "Year"],
            )

            # Create list to loop through
            idmc_list = [
                ["S-180", "Conflict Stock Displacement"],
                ["S-181", "Conflict New Displacements"],
                ["S-189", "Disaster New Displacements"],
                ["S-230", "Disaster Stock Displacement"],
            ]

            # Loop through list
            for element in idmc_list:
                # Extract right columns
                dataframe = S_180_S_181_S189_S_230_raw.loc[:,["ISO3", "Year", "population", element[1]]]

                # Calculate target kpi --> Normalize to per 100.000 persons
                dataframe["RAW_OBS_VALUE"] = (
                    dataframe[element[1]] / (dataframe["population"]) * 100
                )  # Pop given inthousands, we want number per 100.000 pop

                # Add unit measure
                dataframe["ATTR_UNIT_MEASURE"] = self.attr_unit_measure
                IDMC_Extractor.IDMC_Extractor_Source[element[0]] = dataframe

        return IDMC_Extractor.IDMC_Extractor_Source[self.source_id]
            
class UN_SDG_UN_POP(ManuelExtractor):
    """""
    S-185, S-186, S-187, S-188
    """""

    def __init__(self,config, ATTR_UNIT_MEASURE,**kwarg):
        super().__init__(config,**kwarg)
        self.attr_unit_measure = ATTR_UNIT_MEASURE


    def _download(self):
        try:
            # Most json data is from SDG; which deturn json with key "data" having the data as value
            raw_data = pd.json_normalize(requests.get(self.endpoint).json()["data"])
        except:
            # However, some of the data is also from World Bank where the command returns list, which must be subset with list index
            raw_data = pd.json_normalize(
                requests.get(self.endpoint).json()[1]
            )  # 0 is metadata, 1 contains actual data)

        self.dataframe = raw_data

        # Obtain the ISO2 and ISO3 codes
        self.dataframe  = self.dataframe.merge(
            right=self.config.country_full_list,
            how="left",
            left_on="geoAreaName",
            right_on="COUNTRY_NAME",
        )

        # Cast year column as string for join
        self.dataframe.timePeriodStart = self.dataframe.timePeriodStart.astype(int)

        # Join UN Population data to to obtain population size
        self.dataframe  = self.config.un_pop_tot.merge(
            right=self.dataframe ,
            how="right",
            # on="ISO3_YEAR"
            left_on=["COUNTRY_ISO_3", "year"],
            right_on=["COUNTRY_ISO_3", "timePeriodStart"],
        )
       
        # Calculate target KPI (number of Internally displaced people per 100.000 people)
        self.dataframe ["RAW_OBS_VALUE"] = (
            self.dataframe["value"].astype(float) / (self.dataframe ["population"]) * 100
        )  # Pop given inthousands, we want number per 100.000 pop

        # Add unit measure
        self.dataframe ["ATTR_UNIT_MEASURE"] = self.attr_unit_measure

        # Rename columns to avoid double_naming of column, which produces error down the ETL line
        self.dataframe  = self.dataframe .rename(
            columns={
                "COUNTRY": "COUNTRY_ISO_3",
                "geoAreaName": "country_col_not_used",
                "year": "year_not_used",
                "COUNTRY_NAME": "country_col_2_not_used",
                "COUNTRY_ISO_2": "country_col_3_not_used",
                "value": "raw_value_before_normalisation",
                "attributes.Units": "unit_measure_not_used",
            }
        )
        return self.dataframe 

class S_157(ManuelExtractor):
    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)
    
    def _download(self):
        csv_data = Extractor.api_request(self.endpoint).text
        dataframe = pd.read_csv(StringIO(csv_data), sep=",")

        # We only have the population data for both sexes, so discrd other dimensionsubgroups
        dataframe = dataframe.loc[dataframe.SEX == "BTSX"]

        # Obtain population data
        csv_data = Extractor.api_request("https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/UNICEF,DM,1.0/.DM_POP_U5...?format=sdmx-csv&startPeriod=2015&endPeriod=2020").text
        population_data = pd.read_csv(StringIO(csv_data), sep=",")

        # Extract ISO3 code from population data
        population_data["COUNTRY_ISO_3"] = population_data["REF_AREA:Geographic area"].apply(
            lambda x: re.findall("^\w+", x)[0]
        )

        # Discard unnecessary columns
        population_data = population_data[
            ["TIME_PERIOD:Time period", "OBS_VALUE:Observation Value", "COUNTRY_ISO_3"]
        ]

        # Join population data to raw data
        s_157 = population_data.merge(
            right=dataframe, how="right", left_on="COUNTRY_ISO_3", right_on="COUNTRY"
        )

        # Compute target raw observation value
        s_157["RAW_OBS_VALUE"] = s_157["Numeric"] / (s_157["OBS_VALUE:Observation Value"]) * 100

        # Add attribute
        s_157[
            "ATTR_UNIT_MEASURE"
        ] = "The burden of disease attributable to ambient air pollution expressed as Number of deaths of children under 5 years per 100.000 children under 5 years. Note: Data about deaths drawm from WHO (https://apps.who.int/gho/data/node.imr.AIR_4?lang=en), refering to year 2016. Data about population under 5 years drawn from UNICEF (https://data.unicef.org/resources/data_explorer/unicef_f/?ag=UNICEF&df=GLOBAL_DATAFLOW&ver=1.0&dq=.DM_POP_U5..&startPeriod=2008&endPeriod=2018), referring to year 2018. Due to a lack of matching data, the WHO data from 2016 had to be normalized with population data from 2018."

        # Rename clumns
        s_157 = s_157.rename(
            columns={
                "TIME_PERIOD:Time period": "time_period_not_used",
                "OBS_VALUE:Observation Value": "obs_value_not_used",
                "COUNTRY": "country_col_not_used",
                "Display Value": "obs_value_no_used_2",
            }
        )

        return s_157

class Economist_Intelligence_Unit(ManuelExtractor):
    """
    S-11, S-120, S-124,  S-134 , S-229,
    """
    def __init__(self,config,RAW_OBS_VALUE_COLUMN_NAME,COUNTRY_NAME_COLUMN_NAME,**kwarg):
        self.raw_obs_value_column_name = RAW_OBS_VALUE_COLUMN_NAME
        self.country_name_column_name = COUNTRY_NAME_COLUMN_NAME
        super().__init__(config,**kwarg)

    def _download(self):
        self.dataframe = pd.read_excel(
            self.config.data_sources_raw_manual_machine
            / "S-11, S-120, S-124, S-134 OOSI_Out_of_the_shadows_index_60-countries_May2019.xlsm",
            sheet_name="Ranking",
            header=17,
            usecols=[
                "Unnamed: 11",
                "Score",
                "Unnamed: 20",
                "Score.1",
                "Unnamed: 29",
                "Score.2",
                "Unnamed: 38",
                "Score.3",
                "Unnamed: 47",
                "Score.4",
                ],
        )

        self.dataframe = self.dataframe.loc[:,[self.raw_obs_value_column_name,self.country_name_column_name]]

        # Rename clumns
        self.dataframe = self.dataframe.rename(
            columns={self.raw_obs_value_column_name: "RAW_OBS_VALUE", self.country_name_column_name : "COUNTRY_NAME"}
        )

        # Add year column 
        # TODO Make dynmic?!?!
        self.dataframe["TIME_PERIOD"] = 2022

        return self.dataframe

class ICRC_Treaties(ManuelExtractor):
    """
    S-168, S-169, S170
    TODOWhats the Observation Value?
    """
    def __init__(self,config,ATTR_RATIFICATION_DATE_COLUMN_NAME,**kwarg):
        self.attr_ratification_date_column_name = ATTR_RATIFICATION_DATE_COLUMN_NAME
        super().__init__(config,**kwarg)

    def _download(self):
        try:
        # Try loading data from endpoint (preferred)
            self.dataframe = pd.read_excel(
                self.endpoint,
                sheet_name="IHL and other related Treaties",
                header=1,
            )
        except:
            # TODO Log warning
            # Load from local file if endpoint is donw
            self.dataframe = pd.read_excel(
                self.config.data_sources_raw_manual_machine
                / "S-168, S-169, S-170-IHL_and_other_related_Treaties.xls",
                sheet_name="IHL and other related Treaties",
                header=1,
                )
        self.dataframe = self.dataframe[["Country", self.attr_ratification_date_column_name]]

        # Convert datetime format
        self.dataframe[self.attr_ratification_date_column_name] = self.dataframe[self.attr_ratification_date_column_name].apply(
            lambda x: f"{x.year}-{x.month}-{x.day}" if isinstance(x, datetime.date) else x
        )

        # Rename clumns
        self.dataframe = self.dataframe.rename(columns={self.attr_ratification_date_column_name: "ATTR_RATIFICATION_DATE"})

        # Add year column
        self.dataframe["TIME_PERIOD"] = 2020

        return self.dataframe

class CRIN_Treaties(ManuelExtractor):
    """
    S-131, S-193
    """
    def __init__(self,config,RAW_OBS_VALUE_COLUMN_NAME,COUNTRY_NAME_COLUMN_NAME,**kwarg):
        self.raw_obs_value_column_name = RAW_OBS_VALUE_COLUMN_NAME
        self.country_name_column_name = COUNTRY_NAME_COLUMN_NAME
        super().__init__(config,**kwarg)


    def _download(self):
        try:
            # Try loading data from endpoint (preferred)
            self.dataframe = pd.read_excel(
                self.endpoint,
                sheet_name="All countries",
                header=1,
            ).drop(
                [0, 1]
            )  # drop rows that don't contain data
        except:
            # Load from local file if endpoint is donw
            self.dataframe = pd.read_excel(
                self.config.data_sources_raw_manual_machine / "S-131, S-193-access_to_justice_data.xls",
                sheet_name="All countries",
                header=1,
            ).drop(
                [0, 1]
            )  # drop rows that don't contain data

        self.dataframe = self.dataframe[[self.country_name_column_name, self.raw_obs_value_column_name]]

        # Add year column
        self.dataframe["TIME_PERIOD"] = 2016

        # Rename columns
        self.dataframe = self.dataframe.rename(
            columns={
                self.country_name_column_name : "COUNTRY_NAME",
                self.raw_obs_value_column_name : "RAW_OBS_VALUE",
            }
        )

        return self.dataframe

class FCTC_Data(ManuelExtractor):
    """
    S-89
    """
    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        self.dataframe = pd.read_excel(
            self.config.data_sources_raw_manual_machine / "S-89 Answers_v2.xlsx",
        )

        self.dataframe = self.dataframe.melt(id_vars="Party", var_name="TIME_PERIOD", value_name="RAW_OBS_VALUE")

        return self.dataframe


class Landmark_Data(ManuelExtractor):
    """
    S-167 
    """

    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        # Read data
        S_167 = pd.read_excel(
            self.config.data_sources_raw_manual_machine
            / "S_167_Pct_IP_CommunityLands"
            / "Pct_IP_CommunityLands_20170623.xls",
            sheet_name="Pct_IP_CommunityLands",
        )

        # The UK is split up here in its terriroties, must aggregate the data to UK-national level again

        # Pandas interprets the percentage column sometimes as string, sometimes as number  --> Convert all to numbers (in %, i.e. between 0 - 100)
        S_167["IC_F"] = S_167["IC_F"].apply(
            lambda x: x * 100
            if type(x) != str
            else ("No data" if x == "No data" else float(re.sub("%", "", x)))
        )

        # Sub selection of all GBR terriroties
        country_land_gbr = S_167.loc[236:239, "Ctry_Land"]

        # Calculate total area size of Great Britain
        total_land_gbr = sum(country_land_gbr)

        # Obtain percentage of indigenous land of all UK areas
        land_percentage = S_167.loc[
            236:239, "IC_F"
        ]  # .apply(lambda x: float(re.sub('%', '', x)) if type(x)==str else x * 100)

        # Compute total sqm rather than % of total land of indigenous land
        indi_land_tot = []
        for i in range(len(land_percentage)):
            indi_land_tot += [land_percentage.iloc[i] * country_land_gbr.iloc[i] / 100]

        # Total sum of all indiegnous land of all UK territories
        total_indi_land = sum(indi_land_tot)

        # Percentage of indigenous land in ALL UK
        total_indi_land_percent = total_indi_land / total_land_gbr * 100

        # Store all of this data in a dataframe to append it to the existing dataframe
        uk_df = pd.DataFrame(
            [
                [
                    "GBR",
                    np.nan,
                    np.nan,
                    "United Kingdom",
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    total_indi_land_percent,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    "According to the Eurostats study on common lands in Europe of 2010, common lands in the UK is always permanent grassland in the form of rough grazing. Much of these lands are found in the remote upland areas, and in many instance they have at least one special designation preventing their agricultural improvement. The area of these common lands across the UK are :591 901 ha in Scotland, 427 889 ha in England, 180 305 ha in Wales, 36 438 ha in Northern Ireland. Source : Eurostat, 2010. Farm structure survey - common land. Available at : http://ec.europa.eu/eurostat/statistics-explained/index.php?title=Common_land_statistics_-_background&oldid=262743<br>Jones, Gwyn. 2015. Common Grazing in Scotland - importance, governance, issues. Presentation at the EFNCP, available at : http://www.efncp.org/download/common-ground2015/Commons10Jones.pdf. There are also some lands held in in community ownership in Scotland. The latest figure of their total area is 0.19 Mha (470 094 acres). Based on a land mass of Scotland of 7.79 Mha (19.25 M acres), this would represent 2.44% of Scotland’s land mass. This figure has been calculated using the definition of Community Ownership that was agreed by the Scottish Government Short Life Working Group on community land ownership (I million acre target group) in September 2015. <br>Source: Peter Peacock, Community Land Scotland, personal communication. 2015/09/21. The component countries of  the United Kingdom have been treated separately on LandMark.",
                ]
            ],
            columns=S_167.columns,
        )

        # Append dataframe
        S_167 = S_167.append(other=uk_df)

        # Add time period
        S_167["TIME_PERIOD"] = 2017

        # Rename country column (to have it dsiregardedby the ETL pipeline) --> ONly rely on ISO3 column
        S_167 = S_167.rename(columns={"Country": "country_name_not_used"})

        # COnvert "No Data" strings into np.nan
        S_167.loc[S_167["IC_F"] == "No data", "IC_F"] = np.nan

        return S_167


class UCW_Data(ManuelExtractor):
    """
    S-21
    """

    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        self.dataframe = pd.read_excel(
            self.config.data_sources_raw_manual_machine / "S-21-total-HIZkLiYK.xlsx", header=1
        )

        # Unpivot
        self.dataframe = self.dataframe.melt(
            id_vars="Unnamed: 0", var_name="TIME_PERIOD", value_name="RAW_OBS_VALUE"
        )

        # Rename column
        self.dataframe = self.dataframe.rename(columns={"Unnamed: 0": "COUNTRY_NAME"})

        # Change value '..' to NaN
        self.dataframe.loc[self.dataframe.RAW_OBS_VALUE == "..", "RAW_OBS_VALUE"] = np.nan

        return self.dataframe

class Global_Slavery_Index(ManuelExtractor):
    """
    S-60
    """
    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)


    def _download(self):

        self.dataframe = pd.read_excel(
            self.config.data_sources_raw_manual_machine
            / "S-60_FINAL-GSI-2018-DATA-G20-AND-FISHING-1597151668.xlsx",
            header=2,
            sheet_name="Global prev, vuln, govt table",
        )

        # THhs daat represents the 2018 global slavery index data. Add time period
        self.dataframe["TIME_PERIOD"] = 2018

        # Add unit measure attribute
        self.dataframe[
            "ATTR_UNIT_MEASURE"
        ] = "Est. prevalence of population in modern slavery (victims per 1,000 population)"

        # rename columns
        self.dataframe = self.dataframe.rename(
            columns={
                "Country ": "COUNTRY_NAME",
                "Est. prevalence of population in modern slavery (victims per 1,000 population)": "RAW_OBS_VALUE",
            }
        )

        return self.dataframe 

class Inform_Risk_Index_Data(ManuelExtractor):
    """
    S-190
    """
    
    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        try:
            # Try and pull data from endpoint if possible
            self.dataframe = pd.read_excel(
                self.endpoint,
                header=1,
                sheet_name="INFORM Risk 2021 (a-z)",
            ).drop(0)
        except:
            # Load from local file if endpoint is donw
            self.dataframe = pd.read_excel(
                self.config.data_sources_raw_manual_machine / "S-190_INFORM_Risk_2021_v050.xlsx",
                header=1,
                sheet_name="INFORM Risk 2021 (a-z)",
            ).drop(0)

            # Log
            print(
                "Data for source S-190 could not be extracted URL endpoint. Loaded data from local repository."
            )

        # Rename column to avoid taking both country and iso3 column. Only take iso3 ()
        self.dataframe = self.dataframe.rename(
            columns={
                "COUNTRY": "country_col_not_used",  # could be anything not indcluded in column_mapping.py
                "INFORM RISK": "RAW_OBS_VALUE",
            }
        )

        # THhs daat represents data from 2021
        self.dataframe["TIME_PERIOD"] = 2021

        # Save data
        return self.dataframe


class Climate_Watch_Data_S_153(ManuelExtractor):
    """
    S-153
    """
    
    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        self.dataframe = pd.read_csv(self.config.data_sources_raw_manual_machine / "S-153_ndc_content.csv")

        # Cleanse target value variable (some encoding issues)
        self.dataframe.Value = self.dataframe.Value.apply(lambda x: re.sub(";<br>.+", "", x))

        # Cleanse target value variable (some encoding issues)
        self.dataframe["TIME_PERIOD"] = 2020

        # Rename column so that it doesn't cause error downstream
        self.dataframe = self.dataframe.rename(
            columns={
                "Sector": "sector_not_used",
                "Subsector": "sub_sector_not_used",
            }
        )
        
        return self.dataframe

class Climate_Watch_Data_S_159(ManuelExtractor):
    """
    S-159
    """
    
    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        self.dataframe = pd.read_csv(
            self.config.data_sources_raw_manual_machine / "S-159_ghg-emissions.csv",
        )

        # Unpivot the data
        self.dataframe = self.dataframe.melt(
            id_vars=["Country/Region", "unit"],
            var_name="TIME_PERIOD",
            value_name="RAW_OBS_VALUE",
        )

        return self.dataframe

class EITI(ManuelExtractor):

    """
    S-154
    """

    def __init__(self,config,**kwarg):
        super().__init__(config,**kwarg)

    def _download(self):
        try:
            # Most json data is from SDG; which deturn json with key "data" having the data as value
            self.dataframe = pd.json_normalize(requests.get(self.endpoint).json()["data"])
        except:
            # However, some of the data is also from World Bank where the command returns list, which must be subset with list index
            self.dataframe = pd.json_normalize(
                requests.get(self.endpoint).json()[1]
            )  # 0 is metadata, 1 contains actual data)

        # Add time period
        self.dataframe["TIME_PERIOD"] = 2020

        # Rename OBS-VALUE column
        self.dataframe = self.dataframe.rename(
            columns={
                "status": "RAW_OBS_VALUE",
            }
        )
        return self.dataframe

    
