import pandas as pd
import datetime
import numpy as np
from statistics import median


class Cleanser:
    @classmethod
    def cleanse(
        cls,
        raw_data,
        mapping_dictionary,
        final_sdmx_col_list,
        dim_cols,
        country_cols,
        time_cols,
        country_list_full,
        crba_country_list,
    ):
        """Cleanse raw data

        This function cleanses raw data of any source. Specifically, it does the following things:

        1. Rename columns: It uses the mapping dictionary passed as an argument to rename columns to adhere to the final SDMX structure and column names
        2. Discard columns: It discards superfluous columns, that aren't relevant for the final CRBA dataframe
        3. Retrieve latest datapoint: It retrieves the latest observation value for each country.
        4. Discard/ add rows: It discards countries, that aren't part of the final CRBA list and adds those countries which aren't in the raw data.
        5. Fill NA: It fills the dimensions columns of countries who don't have an observation value with "_T" and insert the current year for the variable "TIME_PERIOD" for those countries

        Parameters:
        raw_data (obj): Return of function Class "Extract". Should be a pandas dataframe.
        mapping_dictionary (dict): Dictionary containing the names of columns in the raw dataframe as key and the name of respective target column in the final SDMX dataframe as value
        final_sdmx_col_list (list): List of all columns in the final SDMX CRBA dataframe.
        dim_cols (list): List of all dimension columns in the final SDMX CRBA dataframe.
        country_cols (list): List of all dimension columns in the final SDMX CRBA dataframe.

        Returns:
        obj: Returns pandas dataframe, which only contains the countries meant to be in CRBA and the latest observed value for the indiator.

        """
        # 1. Rename columns
        raw_data = raw_data.rename(columns=mapping_dictionary)
        # Problem: in some raw data a the column "REF_AREA" contains the ISO 2 code, in others it contains ISO 3 code
        # See code below flagged with "DOUBLE_REF_AREA_SOL" to see where it is solved

        # Define all columns which are in dataset AND also in final dataframe
        available_cols_list = [
            col for col in raw_data.columns if col in final_sdmx_col_list
        ]

        # Define available dimensions in the dataset
        available_dims_list = [col for col in raw_data.columns if col in dim_cols]

        # Define available time columns in dataset
        available_time_list = [col for col in raw_data.columns if col in time_cols]

        # Define available country columns in dataset
        available_country_list = [
            col for col in raw_data.columns if col in country_cols
        ]

        # 2. Discard superfluous columns, which aren't gonna be part of the final SDMX-structure DF
        raw_data = raw_data[available_cols_list]

        # Prep for 3: Cast dim cols as string for groupby
        raw_data[available_dims_list] = raw_data[available_dims_list].astype(str)

        # 3. Retrieve the latest available data for each group, where group is 'col_list_gb'
        grouped_data = raw_data[
            raw_data[available_time_list[0]]
            == raw_data.groupby(by=available_dims_list + available_country_list)[
                available_time_list[0]
            ].transform("max")
        ]

        # DOUBLE_REF_AREA_SOL: make sure that the column "REF_AREA" in the raw data is mapped to the right ISO code
        try:
            if median(grouped_data["COUNTRY_ISO_3"].apply(lambda x: len(x))) < 2.5:
                print(
                    "The column REF_AREA has been renamed into COUNTRY_ISO_3, but should be COUNTRY_ISO_2. Now renaming it into COUNTRY_ISO_2"
                )
                grouped_data = grouped_data.rename(
                    columns={"COUNTRY_ISO_3": "COUNTRY_ISO_2"}
                )
        except:
            pass

        # Determine country key column for right join to CRBA list
        country_col_right_join = [
            col for col in crba_country_list.columns if col in grouped_data.columns
        ]

        # Cast as single string rather than list in case there is only only column
        if len(country_col_right_join) == 1:
            country_col_right_join = country_col_right_join[0]

        # print(median(grouped_data["COUNTRY_ISO_3"].apply(lambda x: len(x))))

        # prepare 4: Determine if the country col in the raw dataframe is ISO2, ISO3 or the actual country name
        # TO DO What if country_col_right_join is a list of > 1 element?
        med_country_col_len = median(
            crba_country_list[country_col_right_join].apply(lambda x: len(x))
        )

        # 4. Discard countries that aren't part of the final CRBA master list
        # Determine what kind of country column is in the raw data and do the right join accordingly
        if med_country_col_len < 1.5:
            raise Exception(
                "Country column in the dataframe seems to be wrong. Check column and adjust code if necessary"
            )
        elif (med_country_col_len > 1.5) & (med_country_col_len < 3.5):
            # Column in raw dataframe is ISO2 or ISO3, so can just join directly
            grouped_data_iso_filt = grouped_data.merge(
                right=crba_country_list[country_col_right_join],
                how="right",
                on=country_col_right_join,
                indicator=True,
                validate="many_to_one",
            )
        elif med_country_col_len > 3.5:
            # Column in raw dataframe is country name, so first assign ISO_2 and 3 codes
            # The raw data only contains country names. Assign ISO codes to these country names. Use country full list to make sure each country name variation is captured
            grouped_data_iso = grouped_data.merge(
                right=country_list_full,
                how="left",
                on="COUNTRY_NAME",
                validate="many_to_one",
            )

            # Discard countries that aren't part of the final CRBA master list
            grouped_data_iso_filt = grouped_data_iso.merge(
                right=crba_country_list["COUNTRY_ISO_3"],
                how="right",
                on="COUNTRY_ISO_3",
                indicator=True,
                validate="many_to_one",
            )

        # 5a Fill in _T For each dimension, where it is NaN
        grouped_data_iso_filt[available_dims_list] = grouped_data_iso_filt[
            available_dims_list
        ].fillna(value="_T")

        # 5b Fill in current year for time variable
        grouped_data_iso_filt[available_time_list[0]] = grouped_data_iso_filt[
            available_time_list[0]
        ].fillna(value=datetime.datetime.now().year)

        # Analyse the number of NA values and print it as log info for user
        percentage_na_values = grouped_data_iso_filt[
            "RAW_OBS_VALUE"
        ].isna().sum() / len(grouped_data_iso_filt["RAW_OBS_VALUE"])

        print(
            "Cleansing done. There are {} rows in the dataframe and {}% have a NA-value in the column 'OBS_RAW_VALUE".format(
                len(grouped_data_iso_filt["RAW_OBS_VALUE"]),
                round(percentage_na_values * 100, 2),
            )
        )

        # Return cleansed dataframe
        return grouped_data_iso_filt


"""Dev area: 

What should happen here in the cleasning is: 

* Rename columns (column mapping)
* Classification into 
    * DIM
    * YEAR
    * Country
    * ATT
    * Throw away things --> This happens somewhere else
* Right join to country list to 
* Create _T for all dimensions and leave an NaN for the OBS_Value --> Migrate some of 
the things of normalizer here

"""


"""
    def cleanse(
        cls,
        raw_data,
        raw_data_iso_2_col,
        country_df,
        country_df_iso2_col,
        non_dim_cols,
        time_dim="TIME_PERIOD",
    ):

        # Group by to obtain the latest available value per group, where group is 'col_list_gb'
        # Create list of all column to group by
        col_list = raw_data.columns.to_list()  # list of all columns in the dataframe
        non_dim_cols_tuple = tuple(
            non_dim_cols
        )  # parameters must be passed as list, but the following command requires a tuple
        col_list_gb = [
            e for e in col_list if e not in non_dim_cols_tuple
        ]  # exclude timePeriodStart and value, because these one'saren't used for the groupby statement

        # Some of the columns contain values which Python interprets as lists (e.g. [8.1]). These make the groupby statement malfunction. Convert them to string
        raw_data[col_list_gb] = raw_data[col_list_gb].astype(str)

        # Retreive the latest available data for each group, where group is 'col_list_gb'
        grouped_data = raw_data[
            raw_data[time_dim]
            == raw_data.groupby(col_list_gb)[time_dim].transform("max")
        ]

        # Discard countries that aren't part of the final CRBA master list
        grouped_data_iso_filt = grouped_data.merge(
            right=country_df,
            how="right",
            left_on=raw_data_iso_2_col,
            right_on=country_df_iso2_col,
            indicator=True,
            validate="many_to_one",
        )

        # return result
        return grouped_data_iso_filt.sort_values(by=country_df_iso2_col, axis=0)

"""