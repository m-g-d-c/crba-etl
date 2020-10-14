import pandas as pd


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
        crba_country_list,
    ):
        """Cleanse raw data

        Retrieve only the latest observations for each country-dimension group.
        NB Dimensions make are required to uniquely identify a row of the dataset and therefore their values define groups, e.g. male vs femeale in a country.
        Discard countries that aren't in the CRBA final list of countries

        Parameters:
        raw_data (obj): Return of function 'extract_sdg_api_data'. Should be a pandas dataframe.
        raw_data_iso_2_col (str): Name of the column in 'raw data' frame that contains the ISO2 country codes.
        country_df (obj): Dataframe containing the final CRBA list of countries
        country_df_iso2_col (str): Column in 'country_df', which contains the country iso2 codes.
        non_dim_cols (list): List of columns, which should be excluded from the group-by-statement (that is, if a column is not indicated here, then the maximum value for each of its values will be calculated)

        Returns:
        obj: Returns pandas dataframe, which only contains the countries meant to be in CRBA and the latest observed value for the indiator.

        """
        # Rename columns
        raw_data = raw_data.rename(columns=mapping_dictionary)

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

        # Discard superfluous columns, which aren't gonna be part of the final SDMX-structure DF
        raw_data = raw_data[available_cols_list]

        # Cast dim cols as string for groupby
        raw_data[available_dims_list] = raw_data[available_dims_list].astype(str)

        # Retrieve the latest available data for each group, where group is 'col_list_gb'
        grouped_data = raw_data[
            raw_data[available_time_list[0]]
            == raw_data.groupby(by=available_dims_list + available_country_list)[
                available_time_list[0]
            ].transform("max")
        ]

        # Determine country key column for right join to CRBA list
        country_col_right_join = [
            col for col in crba_country_list.columns if col in grouped_data.columns
        ]

        # Discard countries that aren't part of the final CRBA master list
        grouped_data_iso_filt = grouped_data.merge(
            right=crba_country_list[country_col_right_join],
            how="right",
            on=country_col_right_join,
            indicator=True,
            validate="many_to_one",
        )

        print(available_dims_list)
        # Fill in _T For each dimension, where it is NaN
        grouped_data_iso_filt[available_dims_list] = grouped_data_iso_filt[
            available_dims_list
        ].fillna(value="_T")

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