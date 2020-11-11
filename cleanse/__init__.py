import pandas as pd
import datetime
import numpy as np
import re
from statistics import median


class Cleanser:
    @classmethod
    def extract_who_raw_data(
        cls, raw_data, variable_type, display_value_col="Display Value"
    ):
        """Extract actual numeric data with regex from WHO data

        Numeric data from the WHO often comes in the form of <value [min-max]>, e.g.
        '13.57 [10.33 - 15.5]'
        This function extract the actual numeric value and converts the column type to numeric

        Parameters:
        raw_data (obj): Raw data, should be return from Extractor class
        variable_type(str): String which indicates what type of variable it is. Function will only be applied
            if the variable type is "Continuous variable".

        Return:
        pd.DataFrame in which values of display_value_col are of numeric type and contain only number
        """
        print("\n Calling function 'extract_who_raw_data'...")
        # Check if we are dealing with a WHO source, which habe the column "Display Value"
        if "Display Value" in raw_data.columns:
            if variable_type == "Continuous variable":
                raw_data[display_value_col] = raw_data["Display Value"].astype(str)
                raw_data[display_value_col] = pd.to_numeric(
                    raw_data[display_value_col].apply(
                        lambda x: re.sub("No data", "", re.sub(" \[.*\]", "", x))
                    ),
                    errors="coerce",
                )
            elif variable_type != "Continuous variable":
                pass

        return raw_data

    @classmethod
    def rename_and_discard_columns(
        cls, raw_data, mapping_dictionary, final_sdmx_col_list
    ):
        """Rename columns in raw dataframe and then discard columns which aren't part of final SDMX dataframe

        This function renames the columns of the raw dataframe it is passed to based on the
        mappings specified in mapping_dictionary.

        It renames all columns which have a mapping in the mapping_dictionary and then discards
        all columns which havent been renamed. Therefore, if a column of a raw data frame does not
        have a corresponding mapping column name in the maping_dictionary, it will be discarded
        in this cleansing step.

        To keep a column, you must include it in the mapping_dictionary.

        Parameters:
        raw_data (obs): Raw data, should be return from Extractor class and potentially extract_who_raw_data
        mapping_dictionary(dict): Mapping dictionary containing the raw_data-column names as keys and the
            the column names into which they should be mapped as values.
        final_sdmx_col_list(list): List of all columns names that are in the final SDMX dataframe

        Return:
        pd.DataFrame containing only (renamed) columns as contained in the final SDMX dataframe
        """
        print("\n Calling function 'rename_and_discard_columns'...")

        # 1. Rename columns
        raw_data = raw_data.rename(columns=mapping_dictionary)
        # Problem: in some raw data a the column "REF_AREA" contains the ISO 2 code, in others it contains ISO 3 code
        # See code below flagged with "DOUBLE_REF_AREA_SOL" to see where it is solved

        # Define all columns which are in dataset AND also in final dataframe
        available_cols_list = [
            col for col in raw_data.columns if col in final_sdmx_col_list
        ]

        # 2. Discard superfluous columns, which aren't gonna be part of the final SDMX-structure DF
        raw_data = raw_data[available_cols_list]

        # DOUBLE_REF_AREA_SOL make sure that the column "REF_AREA" in the raw data is mapped to the right ISO code
        try:
            if (
                raw_data["COUNTRY_ISO_3"].apply(lambda x: len(str(x))).quantile(q=0.25)
                < 2.5
            ):
                print(
                    "The column REF_AREA has been renamed into COUNTRY_ISO_3, but should be COUNTRY_ISO_2. Now renaming it into COUNTRY_ISO_2"
                )
                raw_data = raw_data.rename(columns={"COUNTRY_ISO_3": "COUNTRY_ISO_2"})
        except:
            pass

        return raw_data

    @classmethod
    def retrieve_latest_observation(
        cls,
        renamed_data,
        dim_cols,
        country_cols,
        time_cols,
        attr_cols,
    ):
        """Retrieve latest observation for each country in a raw dataset

        Most raw datasets contain several observations per country. For CRBA we are
        only interested in the latest available observation per country.

        This function extracts the latest available observation for each country.

        Parameters:
        renamed_data (obj): Return of rename_and_discard_columns method
        dim_cols (list): List of all dimension columns as name in the final SDMX dataframe
        country_cols (list): List of all country columns as name in the final SDMX dataframe (iso2, iso3, name)
        time_cols (list): List of all time columns as name in the final SDMX dataframe
        attr_cols (list): List of all attribute columns as name in the final SDMX dataframe

        Return:
        pd.DataFrame with only one observation per country (and dimension subgroup, if a raw dataframe contains dimensions as part of a primary composite key)
        """
        print("\n Calling function 'retrieve_latest_observation'...")

        # Define available dimensions in the dataset
        available_dims_list = [col for col in renamed_data.columns if col in dim_cols]

        # Define available time columns in dataset
        available_time_list = [col for col in renamed_data.columns if col in time_cols]

        # Define available country columns in dataset
        available_country_list = [
            col for col in renamed_data.columns if col in country_cols
        ]

        # Define available attribute columns in dataset
        available_attr_list = [col for col in renamed_data.columns if col in attr_cols]

        # Prep for 3: Cast dim and attr cols as string for groupby
        # (sometimes values are [...] which Python interprets as list which can't be joined because hashable)
        renamed_data[available_dims_list] = renamed_data[available_dims_list].astype(
            str
        )
        renamed_data[available_attr_list] = renamed_data[available_attr_list].astype(
            str
        )

        # 3. Retrieve the latest available data for each group, where group is 'col_list_gb'
        grouped_data = renamed_data[
            renamed_data[available_time_list[0]]
            == renamed_data.groupby(by=available_dims_list + available_country_list)[
                available_time_list[0]
            ].transform("max")
        ]

        return grouped_data

    @classmethod
    def add_and_discard_countries(
        cls, grouped_data, crba_country_list, country_list_full
    ):
        """Add and discard countries (i.e. rows) in raw data set

        The crba_country_list is the list of countries for which a CRBA score is calculated.
        Raw data never perfectly has data for those countries (and only those).

        Specifically, what can occur is that

        * Countries (or regions) are in the raw data, but not in crba_country_list
        * Countries are NOT in the raw data, but in crba_country_list
        * Countries are in both the raw data and in the crba_country_list

        To show that ddata for a given country is missing, this function

        * adds counries to a raw dataframe if they aren't present in there already (to later show NA values)
        * discards countries/ regions of the raw dataframe is they aren't in the crba_country_list

        The functioncan deal with data frames irrespetive of
        what the column name for the country column is in the raw data or what type of country
        data it contains (iso2, iso3, or simply country name).

        Note: The columns names in both crba_country_list and country_list_full must be:

        * COUNTRY_ISO_2, COUNTRY_ISO_3 and COUNTRY_NAME

        Parameters:
        grouped_data (obj): Return of retrieve_latest_observation method
        crba_country_list (obj): DataFrame containing the list of those countries included in the final CRBA SDMX df
        country_list_full (obj): DataFrame containing all possible country name variations

        return:
        DataFrame which contains at least one row for each country (or more if there dimensions
        in the dataset), and only those countries which are supposed to be in the final CRBA SDMX dataframe.
        """
        print("\n Calling function 'add_and_discard_countries'...")

        # Determine intersection of country key col in CRBA country list and raw data
        country_col_right_join = [
            col for col in crba_country_list.columns if col in grouped_data.columns
        ]

        # Cast as single string rather than list in case there is only only column
        if len(country_col_right_join) == 1:
            country_col_right_join = country_col_right_join[0]

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
            # The raw data only contains country names. Assign ISO codes to these country names.
            # Use country full list to make sure each country name variation is captured
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

        return grouped_data_iso_filt

    @classmethod
    def add_cols_fill_cells(
        cls,
        grouped_data_iso_filt,
        dim_cols,
        time_cols,
        indicator_name_string,
        index_name_string,
        issue_name_string,
        category_name_string,
        indicator_code_string,
        indicator_source_string,
        indicator_source_body_string,
        indicator_description_string,
        indicator_explanation_string,
        indicator_data_extraction_methodology_string,
        source_title_string,
        source_api_link_string,
        indicator_name_col="INDICATOR_NAME",
        index_name_col="INDICATOR_INDEX",
        issue_name_col="INDICATOR_ISSUE",
        category_name_col="INDICATOR_CATEGORY",
        indicator_code_col="INDICATOR_CODE",
        indicator_source_col="ATTR_SOURCE",
        indicator_source_body_col="ATTR_SOURCE_BODY",
        indicator_description_col="ATTR_INDICATOR_DESCRIPTION",
        indicator_explanation_col="ATTR_INDICATOR_EXPLANATION",
        indicator_data_extraction_methodology_col="ATTR_DATA_EXTRACTION_METHDOLOGY",
        crba_release_year_col="CRBA_RELEASE_YEAR",
        source_title_col="ATTR_SOURCE_TITLE",
        source_api_link_col="ATTR_API_ENDPOINT_URL",
        time_period_col="TIME_PERIOD",
    ):
        """Add several columns and fill cell values of dimensions and year column

        The final SDMX dataframe structure contains several columns, which are not present
        in ra data, e.g. the indicator code and name. This function adds those columns.

        It also fill cell values, if they are NaN, namely:

        * _T for dimension columns with NaN as cell value
        * Current year for TIME_PERIOD

        Parameters:
        grouped_data_iso_filt (obj): Return of add_and_discard_countries
        dim_cols (list): List of all dimension columns as name in the final SDMX dataframe
        time_cols (list): List of all time columns as name in the final SDMX dataframe
        indicator_name_string (str): Indicator name of indicator that is calculated from the raw data
        index_name_string (str): Index name of indicator
        issue_name_string (str): Issue name of indicator
        category_name_string (str): Category name of indicator
        indicator_code_string (str): Indicator code of indicator
        indicator_source_string (str): Source (ideally a URL) of the indicator
        indicator_source_body_string (str): Source body, i.e. the insitution of the source (e.g. SDG Database, ILO, ...)
        indicator_description_string (str): Indicator description of indicator
        source_title_string (str): Title of the source of the indicator
        indicator_explanation_string (str): Indicator explanation (why was it included in the CRBA?)
        indicator_data_extraction_methodology_string (str): Notes/comments about how data was extracted
        source_api_link_string (str): If data was drawn from an API, provide the URL here.
        indicator_name_col (str): Your desired name for the indicator name column
        index_name_col (str): Your desired name for the index name column
        issue_name_col (str): Your desired name for the issue name column
        category_name_col (str): Your desired name for the category name column
        indicator_code_col (str): Your desired name for the indicator code column
        indicator_source_col (str): Your desired name for the attribute_source column
        indicator_source_body_col (str): Your desired name for the attribute_source_body column
        crba_release_year_col (str): Your desired name for the CRBA releaseyea column
        indicator_description_col (str): Your desired name for the indicator description column
        source_title_col (str): Your desired name for the source title column
        indicator_explanation_col (str): Your desired name for the indicator explanation (justification) column
        indicator_data_extraction_methodology_col (str): Your desired name for extraction methdology column
        source_api_link_col (str): Your desired name for API link URL (only for API drawn data sources)

        Return:
        Dataframe with added columns and filled in cell values

        """
        print("\n Calling function 'add_cols_fill_cells'...")

        # # # Fill in _T and year values
        # Define available dimensions in the dataset
        available_dims_list = [
            col for col in grouped_data_iso_filt.columns if col in dim_cols
        ]

        # Define available time columns in dataset
        available_time_list = [
            col for col in grouped_data_iso_filt.columns if col in time_cols
        ]

        # UN Treaty and ILO NORMLEX data does not have a column for TIME_PERIOD, add here
        if len(available_time_list) == 0:
            grouped_data_iso_filt[time_period_col] = datetime.datetime.now().year
            available_time_list += [time_period_col]

        # 5a Fill in _T For each dimension, where it is NaN
        grouped_data_iso_filt[available_dims_list] = grouped_data_iso_filt[
            available_dims_list
        ].fillna(value="_T")

        # 5b Fill in current year for time variable
        grouped_data_iso_filt[available_time_list[0]] = grouped_data_iso_filt[
            available_time_list[0]
        ].fillna(value=datetime.datetime.now().year)

        # # # Add additional columns
        # Indicator name
        grouped_data_iso_filt[indicator_name_col] = indicator_name_string

        # Index name
        grouped_data_iso_filt[index_name_col] = index_name_string

        # Issue name
        grouped_data_iso_filt[issue_name_col] = issue_name_string

        # Category name
        grouped_data_iso_filt[category_name_col] = category_name_string

        # Create column indicator code
        grouped_data_iso_filt[indicator_code_col] = indicator_code_string

        # Create column to indicate source
        grouped_data_iso_filt[indicator_source_col] = indicator_source_string

        # Create column to indicate source body
        grouped_data_iso_filt[indicator_source_body_col] = indicator_source_body_string

        # Create column to indicate indicator description
        grouped_data_iso_filt[indicator_description_col] = indicator_description_string

        # Create column to indicate indicator explanation
        grouped_data_iso_filt[indicator_explanation_col] = indicator_explanation_string

        # Create column to indicate dataa extraction methdology
        grouped_data_iso_filt[
            indicator_data_extraction_methodology_col
        ] = indicator_data_extraction_methodology_string

        # Create column to indicate source title
        grouped_data_iso_filt[source_title_col] = source_title_string

        # Create column to indicate api/ endpoint link
        grouped_data_iso_filt[source_api_link_col] = source_api_link_string

        # YEAR_CRBA_RELEASE with current year
        grouped_data_iso_filt[crba_release_year_col] = datetime.datetime.now().year

        return grouped_data_iso_filt

    @classmethod
    def map_values(cls, cleansed_data, value_mapping_dict):
        """Map column values (assign consistent values)

        DIfferent sources may have different values for a certain variable, but may actally mean the
        same thing,. For example, in one data source the value "male" in "Gender" migh be "m", in
        others it might be "mle".

        This function map those values into one consistent system, as defined in the value_mapping_dict.
        The value_mapping_dict should fhave the following structure:

        value_mapper = {
            <column name in final sdmx df> : {
                <target value> : <list of values to be converted into the target value>
            }
        }

        Paramteres:
        cleansed_data (obj): Cleansed raw data frame (after columns have been renamed!!!)
        value_mapping_dict (dict): Dictionary containing the mappings from raw data values --> desired value

        Return:
        Dataframe with converted cell values as stipulated in value_mapping_dict

        """
        print("\n Calling function 'map_values'...")

        # Loop through all possible columns as defined for the final SDMX structure
        for key in value_mapping_dict:
            try:
                # Define emtpy lists to be mapped to each other
                original_values = []
                mapped_values = []

                # Loop obtain all possible original/ mapped value variations mappings
                for sub_key in value_mapping_dict[key]:
                    # Obtain boolean arrays for each possible original value
                    for list_element in range(len(value_mapping_dict[key][sub_key])):
                        original_values += [
                            cleansed_data[key]
                            == value_mapping_dict[key][sub_key][list_element]
                        ]

                    # Define the target value if original_values evaluates to true
                    mapped_values += len(value_mapping_dict[key][sub_key]) * [sub_key]

                # Convert (map) the values
                cleansed_data[key] = np.select(original_values, mapped_values)

                # log info for user
                print("\n Successfully mapped values of column: {}".format(key))

            # If column is not presnt (or if there are other issues)
            except:
                print(
                    "Values of column: {} couldn't be mapped. If column {} is present, there is an error with the code. ".format(
                        key, key
                    )
                )
        return cleansed_data

    @classmethod
    def encode_categorical_variables(
        cls,
        dataframe,
        encoding_string,
        encoding_labels=False,
        na_encodings="0",
        obs_raw_value_source="RAW_OBS_VALUE",
        obs_raw_value_target="RAW_OBS_VALUE",
        sep_character=";",
        assign_character="=",
    ):
        """Encode variables

        This function encodes variables, i.e. you can pass it a string containing a certain
        encoding (e.g. 'Yes=1; No=2'), and it will encode the variables accordingly.

        By default, the values are overwritten (i.e. obs_raw_value_source and obs_raw_value_target)
        are the same columns. However, encoded values can also be stored somewhere else.

        The function is used for two cases:

        * Encode data to make it fit for normalizer function: The normalizer function requires numeric
        data. So strings must be encoded as numbers.
        * Pre-encode raw data: Some raw data is already encoded in numbers (which represent strings).
        In that case they undergo a pre-encoding have the raw data contain the actual strings (i.e.
        "no minimum age", rather than "2" so as to make it more self-explantory and user friendly.)

        The encoding must be passed as string to the encoding_string parameter. The encoded value and original
        value must be connected with the assign character. Encoded-value/original-value pairs must be separater
        by sep_character.

        By default, empty values (NaNs) and empty strings are encoded into '0's.

        Values which appear in obs_raw_value_source but do not have a mapping value are
        encoded as "VALUE WITHOUT MAPPING - PLEASE MAP".


        Parameters:
        dataframe (obj): Raw dataframe containing categorical raw data
        encoding_string (str): String containing the Encoded-value/original-value pairs
        obs_raw_value_source (str): Column name of column containing raw data
        obs_raw_value_target (str): Column name of column where encoded data is written to. By default identical to obs_raw_value_source
        sep_character (str): Character by which Encoded-value/original-value pairs are separated
        assign_character (str): Character assigning Encoded-value and original-values
        create_attr_enc (bool): Should a column be created that contains the encoding string?

        Return:
        Dataframe with encoded categorical, raw data and an added column ATTR_ENCODING_LABELS containing encoding labels
        """

        print("\n Calling function 'encode_categorical_variables'...")

        if encoding_string != "Continuous variable":
            # Split string into mapping pairs
            mapping_pairs = re.split(sep_character, encoding_string)

            # Create nested list with encoded value and original value packed in a sublist
            mapping_pairs_listed = []
            for pair in mapping_pairs:
                mapping_pairs_listed += [re.split(assign_character, pair)]
                # print(mapping_pairs_listed)

            # Define conditions
            raw_values = []
            encodings = []

            # Extract raw values and encodings from mapping pairs listed
            for mapping_sublist in range(len(mapping_pairs_listed)):
                raw_values += [
                    dataframe[obs_raw_value_source].astype(
                        str
                    )  # convert to string in case raw value is numeric
                    == mapping_pairs_listed[mapping_sublist][1].rstrip().lstrip()
                ]  # delete str and after [1] aprentheses again
                encodings += [
                    mapping_pairs_listed[mapping_sublist][0].rstrip().lstrip()
                ]

            # Encode NaN values
            raw_values += [dataframe[obs_raw_value_source].isnull()]
            raw_values += [dataframe[obs_raw_value_source] == ""]

            # Target value of NaN values
            encodings += [na_encodings, na_encodings]

            dataframe[obs_raw_value_target] = np.select(
                condlist=raw_values,
                choicelist=encodings,
                default="VALUE WITHOUT MAPPING - PLEASE MAP",
            )

            if encoding_labels != False:
                # create attr_encoding_raw_values
                dataframe["ATTR_ENCODING_LABELS"] = encoding_labels

        else:
            pass

        return dataframe

    @classmethod
    def create_log_report(
        cls, cleansed_data, raw_obs_col="RAW_OBS_VALUE", year_col="TIME_PERIOD"
    ):
        """
        Calulcate how man NA values there are in a cleansed dataframe inthe column "RAW_OBS_VALUE
        """
        # Analyse the number of NA values and print it as log info for user
        percentage_na_values = cleansed_data[raw_obs_col].isna().sum() / len(
            cleansed_data[raw_obs_col]
        )

        print(
            "Cleansing done. This is some basic information about the data: \n \n There are {} rows in the dataframe and {}% have a NA-value in the column 'OBS_RAW_VALUE".format(
                len(cleansed_data[raw_obs_col]),
                round(percentage_na_values * 100, 2),
            )
        )

        try:
            cleansed_data[year_col] = pd.to_numeric(cleansed_data[year_col])
        except:
            pass

        summary_year = cleansed_data[year_col].describe()

        print(
            "\n \n This is the summary of the column 'TIME_PERIOD': {}".format(
                summary_year
            )
        )

    @classmethod
    def decompose_country_footnote_ilo_normlex(
        cls,
        dataframe,
        country_name_list,
        footnote_col="ATTR_FOOTNOTE_OF_SOURCE",
        country_col="COUNTRY_NAME",
    ):
        """Extract country and store additional info in footnotes from ILO NORMLEX

        Some ILO NORMLEX data frames contain both country and a footnote in the country column.
        This functions decomposes this information in two separate columns.
        The column country_col will then contain the country only (which
        is required for join operations and later function calls) and the
        footnote_col will contain footnotes (if present).

        Paramters:
        dataframe (obj): Dataframe, should be return of rename_and_discard_columns method in Cleanser class
        country_name_list (list-like): List containing all possible country name variations
        footnote_col (str): Name of column in final SDMX structure containing source footnotes
        country_col (str): Name of column containing country name

        Return:
        DataFrame, with country column content decomposed.
        """
        # Define function to extract country name to be applied with .apply
        def extract_country_name(cell, country_name_list_temp=country_name_list):
            # Determine which country in the full country list is contained in string
            subset_list = [x in cell for x in country_name_list_temp]

            # Sometimes several country names match, but we need exactly one
            if sum(subset_list) == 0:
                print("No country name match")
            elif sum(subset_list) == 1:
                # Retrieve the actual country name
                country_name = country_name_list[subset_list].item()
            else:
                # Retrieve the actual country name
                country_name = country_name_list[subset_list].iloc[0]

            return country_name

        # Create footnote column that store footnotes
        dataframe[footnote_col] = dataframe[country_col]

        #  extract country name
        dataframe[country_col] = dataframe[country_col].apply(extract_country_name)

        # Purge footnotes from the country
        dataframe[footnote_col] = dataframe.apply(
            lambda x: re.sub(x[country_col], "", x[footnote_col]), 1
        )

        return dataframe

    @classmethod
    def encode_ilo_un_treaty_data(  # TO DO RENAME AND CHANGE IN THE LOOP
        cls,
        dataframe,
        treaty_source_body,
        attr_rat_date_value="ATTR_RATIFICATION_DATE",
        attr_treaty_status="ATTR_TREATY_STATUS",
        obs_raw_value="RAW_OBS_VALUE",
    ):
        """Encode un treaty data TO DO: CHANGE DOCUMENTATION

        This function is very similar (almost a child class) of the encode_categorical_variables method.
        It is meant to be applied to both UN Treaty and ILO NORMLEX data, which are indicated with "UN Treaties"  and "ILO NORMLEX" in the
        indicator dictionary respectively.

        Like the encode_categorical_variables method, it creates RAW_OBS_VALUE as a numeric variable,
        so that it can be passed to the scaler function. However, this function does this
        by looking at the column "ATTR_RATIFICATION_DATE" and "ATTR_TREATY_STATUS respectively", to determine whether a treaty or convention
        was ratified (un treaties) or is currently in force (ILO NORMLEX traties).

        Parameters:
        dataframe (obj): Pandas dataframe, should be the return of the method 'add_and_discard_countries'
        treaty_source_body(str): Indicate whether its a UN or ILO NORMLEX data with these strings: "UN Treaties" or "ILO NORMLEX"
        attr_rat_date_value (str): Name of column containig the information when a treaty was ratified (for UN Treaties, it also contains a code indicating status-details of the contract)
        attr_treaty_status (str): Name of column containig information if a conventions is currently in force
        obs_raw_value (str): Name of the cilumn that is supposed to contain the raw observation value. This will contain the encoded numbers and is created by the function.

        Return:
        Pandas DataFrame with RAW_OBS_VALUE added, containing encoded values and a column added containig decoding instructions.
        """
        print("\n Calling function 'add_cols_fill_cells'...")

        # ILO NORMLEX and UN Treaties raw data is different, so require different code blocks
        if treaty_source_body == "ILO NORMLEX":
            # Define condition: Only countries which have the treaty in force are considered as a "2"
            conditions = [
                dataframe[attr_treaty_status] == "In Force",
                dataframe[attr_treaty_status] != "In Force",
            ]

            # Create the encoding_lavel_string to be inserted in col ATTR_ENCODING_LABELS
            encoding_label_string = "2=Yes, 1=No; as answer to the following question: Is the convention/ treaty in force as of today?"
        elif treaty_source_body == "UN Treaties":
            # Define condition: Only countries which have signed are listed. The ones not listed will have a NaN value (as a result of the previous join)
            conditions = [
                dataframe[attr_rat_date_value].isnull() == False,
                dataframe[attr_rat_date_value].isnull() == True,
            ]

            # Create the encoding_lavel_string to be inserted in col ATTR_ENCODING_LABELS
            encoding_label_string = "2=Yes, 1=No; as answer to the following question: Has the country done one of the following things with the treaty: Ratification, Acceptance(A), Approval(AA), Accession(a), Succession(d), Formal confirmation(c), Definitive signature(s)? Unless the date indicated is not followed by an encoding, it shows when the treaty was ratified. If it is followed by an encoding sign (e.g. 'a'), the encoding sign indicates its status."
        else:
            raise Exception(
                "Please specify the treaty_ource_body. This is required for this function to work. Consult documentation for further information."
            )

        # Define encodings
        encodings = [2, 1]

        # Do the encoding
        dataframe[obs_raw_value] = np.select(
            condlist=conditions,
            choicelist=encodings,
            default="VALUE WITHOUT MAPPING - PLEASE MAP",
        )

        # create attr_encoding_raw_values
        dataframe["ATTR_ENCODING_LABELS"] = encoding_label_string

        return dataframe
