import pandas as pd
import numpy as np
import datetime


def normalizer(
    cleansed_data,
    sql_subset_query_string,
    # dim_cols,
    variable_type="Continuous variable",
    is_inverted="not inverted",
    whisker_factor=1.5,
    raw_data_col="RAW_OBS_VALUE",
    scaled_data_col_name="SCALED_OBS_VALUE",
    maximum_score=10,
    log_info=False,
):
    """Normalize the RAW_OBS_VALUES into indicator scores

    Take the raw observation values and convert them into the actual, normalized indicator scores.
    The score will always be between 0 and 10 (but may also be "No Data")

    There are two distinct cases:

    * Numeric (continuous) variable: Scores are calculate with a standard, statistical normalization.
    The normalization depends on the min and max values of the sample. In most of the data, a raw
    is uniquely identifid not only by the country and year, but also by the values of sveral dimensions
    (e.g. gender). In that case, normalization is only being done for a specific dimension-subgroup, which
    is taken as subsample for normalization.

    * Categorical: Scores are calculated based of encoded raw data, i.e. raw data with values
    such as "yes" or "Ratified", must previously have been encoded into discrete, numeric values.
    In a given dataset, the function will identify the number of categories automatically and
    assign the corresponding scores. The value "0" (both as string or number) will always be
    encoded into "No Data".

    Parameters:
    cleansed_data (obj): Cleansed dataFrame, should be the return of Cleanser class and methods.
    sql_subset_query_string (str): String specifying for which dimension-subgroup the normalization should
    be done. Note: Only applies to numeric, continuous raw data. The string should follow this
    structure: "<column_name_1> == <"value"> & <column_name_2> == <"value"> ...
    variable_type (str): String specifying variable type, should be "Continuous variable" or any other string if categorica
    is_inverted (str): String secifying if a continuous variabl should have inverted scores
    whisker_factor (num): Value to take as whisker factor to define outliers in a distribution
    raw_data_col (str): column name containing raw data
    scaled_data_col_name (str): Desired column name for normalized (scaled) data
    maximum_score (num): Maximum score of the normalized data, typically 10
    log_info (bool): Indicate whether you want to print logging information

    Return:
    DataFrame with a new column called scaled_data_col_name containing the normalized (scaled) data
    """
    if variable_type != "Continuous variable":
        # This is unusual, but some categorical variables also have dimensions
        if sql_subset_query_string:
            cleansed_data_subset = cleansed_data.query(sql_subset_query_string)
        else:
            cleansed_data_subset = cleansed_data

        # Build conditions array
        unique_values = cleansed_data_subset[raw_data_col].unique()

        conditions = []
        for value in sorted(unique_values):
            conditions += [cleansed_data_subset[raw_data_col] == value]

        # Assign variable for readability purpose
        length_unique_values = len(cleansed_data_subset[raw_data_col].unique())

        # Build norm values array, where 0 =< norm_values => 10
        norm_values = []
        divisor = 1

        # In the encoding, the value 0 means "No Data/ no reponse/ not answered/ ..."
        if (0 in unique_values) | ("0" in unique_values):
            norm_values += [
                np.nan
            ]  # This is into what "0" is encoded into OBS_SCALED_VALUE
            length_unique_values -= 1

        # Define the normalization type, e.g. 0-5-10, or 0 - 3.3 - 6.6 - 10
        distance = maximum_score / (length_unique_values - divisor)

        for value in range(length_unique_values):
            norm_values += [round(distance * float(value), 2)]

        # store normalized scores in SCALED_OBS_VALUE
        cleansed_data_subset[scaled_data_col_name] = np.select(conditions, norm_values)

        # join normalized data to original dataframe
        cleansed_data = cleansed_data.merge(right=cleansed_data_subset, how="outer")

    elif variable_type == "Continuous variable":
        """
        Normalization requires min and max values within a certain group
        --> Out of subgroups that are defined by the dimension values
        one dimension subgroup must be selected
        """
        # Define the dimension subgroup for which normalization is done:
        if sql_subset_query_string:
            cleansed_data_subset = cleansed_data.query(sql_subset_query_string)
        else:
            cleansed_data_subset = cleansed_data

        # Determine basic descriptive statistics of the distribution that are required for the normalization
        min_val = np.nanmin(cleansed_data_subset[raw_data_col].astype("float"))
        max_val = np.nanmax(cleansed_data_subset[raw_data_col].astype("float"))
        q1 = cleansed_data_subset[raw_data_col].astype("float").quantile(q=0.25)
        q3 = cleansed_data_subset[raw_data_col].astype("float").quantile(q=0.75)
        iqr = q3 - q1

        # Define what max value to use for the normalization
        if max_val > q3 + whisker_factor * iqr:
            max_to_use = q3 + whisker_factor * iqr
            if log_info == True:
                print(
                    "The distribution of the raw data values this subgroup contains outliers or is too skewed on the upper end. The maximum value to be used for the normalisation is: 3rd quartile or distribution + {} * IQR. It is: {} \n See histogram printed below for info. \n".format(
                        whisker_factor, max_to_use
                    )
                )
        else:
            max_to_use = max_val
            if log_info == True:
                print(
                    "The distribution of the raw data for this subgroup does not contain outliers on the upper end. It is also not too skewed on the upper end. The maximum value used for the normalisation is the maximum value in the dataset, which is {}. This value corresponds to country: {} \n".format(
                        max_to_use,
                        cleansed_data_subset[
                            cleansed_data_subset[raw_data_col].astype("float")
                            == max_val
                        ],
                    )
                )

        # Define what min value to use for the normalization
        if min_val < q1 - whisker_factor * iqr:
            min_to_use = q1 - whisker_factor * iqr
            if log_info == True:
                print(
                    "The distribution of the raw data values for this subgroup contains outliers or is too skewed on the lower end. The minimum value to be used for the normalisation is 1st quartile or distribution - {} * IQR. It is: {} \n See histogram printed below for info. \n".format(
                        whisker_factor, min_to_use
                    )
                )
        else:
            min_to_use = min_val
            if log_info == True:
                print(
                    "The distribution of the raw data for this subgroup does not contain outliers or is too skewed on the lower end. The minimum value used for the normalisation is the minimum value in the dataset, which is {}. This value corresponds to country: {} \n".format(
                        min_to_use,
                        cleansed_data_subset[
                            cleansed_data_subset[raw_data_col].astype("float")
                            == min_val
                        ],
                    )
                )

        # If there are outliers or a skewed distribution, print the distribution for the user.
        if log_info == True:
            if (min_val < q1 - whisker_factor * iqr) or (
                max_val > q3 + whisker_factor * iqr
            ):
                print("\n This is the distribution of the raw data of the indicator.")
                print(pd.to_numeric(cleansed_data_subset[raw_data_col]).hist(bins=30))

        # Define the value range that is used for the scaling (normalization)
        tot_range = max_to_use - min_to_use

        # Compute the normalized value of the raw data
        # Distinguish between indicators, whose value must be inverted
        if is_inverted == "inverted":
            cleansed_data_subset[scaled_data_col_name] = round(
                maximum_score
                - maximum_score
                * (cleansed_data_subset[raw_data_col].astype("float") - min_to_use)
                / tot_range,
                2,
            )

        elif is_inverted == "not inverted":
            cleansed_data_subset[scaled_data_col_name] = round(
                maximum_score
                * (cleansed_data_subset[raw_data_col].astype("float") - min_to_use)
                / tot_range,
                2,
            )
        else:
            raise ValueError(
                "This is a numeric indicator, so you must specify whether or not it is inverted"
            )

        # If outliers are present and max_to_use != max_value OR min_to_use != min_value
        # Scores may be out of range [0; 10], so must round them down
        cleansed_data_subset.loc[
            cleansed_data_subset[scaled_data_col_name] < 0, scaled_data_col_name
        ] = 0
        cleansed_data_subset.loc[
            cleansed_data_subset[scaled_data_col_name] > 10, scaled_data_col_name
        ] = 10

        # join normalized data to original dataframe
        cleansed_data = cleansed_data.merge(right=cleansed_data_subset, how="outer")

    # Create new column called "OBS_STATUS", which has value "O" if raw data is NaN
    result = cleansed_data.assign(
        OBS_STATUS=np.where(cleansed_data[raw_data_col].isnull(), "O", np.nan)
    )

    # For categorical variables, the value 0 also means No data, so update OBS_STATUS
    if variable_type != "Continuous variable":
        result.loc[
            (result["RAW_OBS_VALUE"] == "0") | (result["RAW_OBS_VALUE"] == 0),
            "OBS_STATUS",
        ] = "O"

    # Return result
    return result