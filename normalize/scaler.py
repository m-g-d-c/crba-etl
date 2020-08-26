import pandas as pd
import numpy as np

def normalizer(cleansed_data, indicator_raw_value, indicator_code, indicator_name, cleansed_df_iso2_col, crba_final_country_list, crba_final_country_list_iso_col, cat_var = False, cat_scoring_type = None, inverted = False, non_dim_cols = None, whisker_factor = 1.5):
    """ Transform cleansed datasets into scaled (normalized) data in long format

    This function can be applied to any cleansed data. It requires as principal
    object the dataframe containing the cleansed data of an indicator-source,
    calculates the index score values (i.e. normalized values) for an indicator,
     and returns the input dataframe with three additional columns "VALUE_TYPE",
     "value", "INDICATOR_CODE" and "INDICATOR_NAME", which hold the normalized,
    index-score for the indicator. Indicators can categorical or numerical,
    which must be specified in the function call. For some indicators, the index
     score must be inverted, which must be specified in the function call.

    Parameters:
    cleansed_data (obj): Raw dataset, pandas dataframe
    indicator_raw_value (str): Column containing the actual raw data values
    indicator_code (str): Code of the indicator as stipulated in the data dictionary
    indicator_name (str):  Name of the indicator as stipulated in the data dictionary
    cleansed_df_iso2_col (str): Column which holds the ISO2 code in 'cleansed_data'
    crba_final_country_list (obj): Final CRBA country list, should contain country ISO2 (primary key), ISO3 and country name columns.
    crba_final_country_list_iso_col (str): Column in crba_final_country_list which holds the ISO2 codes of the countries.
    cat_var (bool): is variable of type categorical or numerical?
    cat_scoring_type (str): For categorical variables, specify the scoring type (i.e. what values the categorical variable can take)
    inverted (bool): If we are dealing with a numerical variable, should the values be inverted?
    whisker_factor (float): Whisker factor which is multiplied with the interquartile range (IQR) to define outliers. Default value is 1.5, as is standard in statistics.
    non_dim_cols (list): List of columns who are required to uniquely identify a row in the dataset. The normalization will be done for each dimension-subset there is.

    Returns:
    obj: Returns pandas dataframe with normalized indicator scores in long format
    """

    # Check if an indicator is numeric of categorical
    if cat_var == True:
        # This is the section dealing with categorical indicators
        # There are various scoring_types a categorical variable can take. For each one, different labels apply to the values
        # Type 2-1-0
        if cat_scoring_type == 'Type 2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 0),
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2)
                ]

            # Specify the normalized values
            norm_values = ['No data', '0.00', '10.00']

        # Type 2-1
        elif cat_scoring_type == 'Type 2-1':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2)
                ]

            # Specify the normalized values
            norm_values = ['0.00', '10.00']

        # Type 3-2-1
        elif cat_scoring_type == 'Type 3-2-1':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2),
                (cleansed_data[indicator_raw_value] == 3)
                ]

            # Specify the normalized values
            norm_values = ['0.00', '5.00', '10.00']

        # Type 3-2-1-0
        elif cat_scoring_type == 'Type 3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 0)
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2),
                (cleansed_data[indicator_raw_value] == 3)
                ]

            # Specify the normalized values
            norm_values = ['No data', '0.00', '5.00', '10.00']

        # Type 4-3-2-1
        elif cat_scoring_type == 'Type 4-3-2-1':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2),
                (cleansed_data[indicator_raw_value] == 3),
                (cleansed_data[indicator_raw_value] == 4)
                ]

            # Specify the normalized values
            norm_values = ['0.00', '3.33', '6.67', '10.00']


        # Type 4-3-2-1-0
        elif cat_scoring_type == 'Type 4-3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 0)
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2),
                (cleansed_data[indicator_raw_value] == 3),
                (cleansed_data[indicator_raw_value] == 4)
                ]

            # Specify the normalized values
            norm_values = ['No data', '0.00', '3.33', '6.67', '10.00']


        # Type 5-4-3-2-1-0
        elif cat_scoring_type == 'Type 5-4-3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 0)
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2),
                (cleansed_data[indicator_raw_value] == 3),
                (cleansed_data[indicator_raw_value] == 4),
                (cleansed_data[indicator_raw_value] == 5)
                ]

            # Specify the normalized values
            norm_values = ['No data', '0.00', '2.50', '5.00', '7.50', '10.00']

        # Type 7-6-5-4-3-2-1-0
        elif cat_scoring_type == 'Type 7-6-5-4-3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (cleansed_data[indicator_raw_value] == 0)
                (cleansed_data[indicator_raw_value] == 1),
                (cleansed_data[indicator_raw_value] == 2),
                (cleansed_data[indicator_raw_value] == 3),
                (cleansed_data[indicator_raw_value] == 4),
                (cleansed_data[indicator_raw_value] == 5),
                (cleansed_data[indicator_raw_value] == 6),
                (cleansed_data[indicator_raw_value] == 7)
                ]

            # Specify the normalized values
            norm_values = ['No data', '0.00', '1.67', '3.33', '5.00', '6.67', '8.33', '10.00']

        else:
            raise('The scoring type you have specified does not exist. Make sure your scoring type is listed in the documentation of this fuction.')

        # create a new column and assign values to it using our lists
        cleansed_data['SCALED'] = np.select(conditions, values)

    else:
        # This is the section dealing with numerical indicators
        # If there are dimensions in the dataset (e.g. GENDER), then the normalization of the indicator score
        # must be done for all subgroups of all dimensions. For that, we must extract these subsets in the original dataset

        # Extract the subsets (defined by the dimensions) of the data
            # Exclude columns which are are not a dimension or should not be part in defining a subset (i.e. they aren't part of a unique identifier of a row)
        col_list = cleansed_data.columns.to_list() # list of all columns in the dataframe
        non_dim_cols_tuple = tuple(non_dim_cols) # parameters must be passed as list, but the following command requires a tuple
        non_essential_col = [e for e in col_list if e not in non_dim_cols_tuple]

            # Define parameters to run a loop going through all subsets
        length = cleansed_data[non_essential_col].drop_duplicates().shape[0]
        width = cleansed_data[non_essential_col].drop_duplicates().shape[1]

        # Create empty final dataframe before entering loop. The different subsets will be appended in this DF to obtain the full dataset incl. the scaled variable in the end
        cleansed_data_full = pd.DataFrame(columns = cleansed_data.columns.tolist())

        # Inform what columns (which have not previously been excluded) have several values and therefore define a subset
        print('You hve a selected a few columns, which will not be regarded as dimensions. These are the remaining columns in the dataset, along with the number of values they take in the dataset.')
        tot_num_subsets = 1
        for col in cleansed_data[non_essential_col]:
            print('The column {} has {} unique values.'.format(col, cleansed_data[non_essential_col][col].nunique()))
            tot_num_subsets *= cleansed_data[non_essential_col][col].nunique()
        print('The total number of subgroups in the dataset is therefore: {}'.format(tot_num_subsets))

        # Loop: i) defining values of subsets to create ii) subsets , iii) calculate the scaled value for all of them and iv) append the subsets in one dataframe
        for j in range(1, length):
            # i) Create the defining values of the subset
            subset = ''
            for i in range(width):
                subset += '(cleansed_data[cleansed_data[non_essential_col].columns[{}]] == cleansed_data[non_essential_col].drop_duplicates().iloc[{}, {}]) & '.format(i, j, i)
            subset = subset.rstrip('& ')

            # ii) Define the subset of the entire dataframe
            cleansed_data_subset = cleansed_data[eval(subset)]

            # Log: Inform user what subset the operations are being performed on
            print('In the loop we are currently dealing with the subset #{}, which has these defining values: {}'.format(j, cleansed_data_subset[non_essential_col].drop_duplicates()))
            print('\n The shape of the subset is: {}'.format(cleansed_data_subset.shape)) # DELETE

            # Often, the subset will be empty dataframes, because they only consists of NaN values
            try:
                # iii) Determine basic descriptive statistics of the distribution that are required for the normalization
                min_val = np.nanmin(cleansed_data_subset[indicator_raw_value].astype('float'))
                max_val = np.nanmax(cleansed_data_subset[indicator_raw_value].astype('float'))
                q1 =  cleansed_data_subset[indicator_raw_value].astype('float').quantile(q=0.25)
                q2 =  cleansed_data_subset[indicator_raw_value].astype('float').quantile(q=0.50)
                q3 =  cleansed_data_subset[indicator_raw_value].astype('float').quantile(q=0.75)
                iqr = q3 - q1

                # Define what max value to use for the normalization
                if max_val > q3 + whisker_factor * iqr:
                    max_to_use = q3 + whisker_factor * iqr
                    print('The distribution of the raw data values this subgroup contains outliers or is too skewed on the upper end. The maximum value to be used for the normalisation is: 3rd quartile or distribution + {} * IQR. It is: {} \n See histogram printed below for info. \n'.format(whisker_factor, max_to_use))
                else:
                    max_to_use = max_val
                    print('The distribution of the raw data for this subgroup does not contain outliers or is too skewed on the upper end. The maximum value used for the normalisation is the maximum value in the dataset, which is {}. This value corresponds to country: {} \n'.format(max_to_use, cleansed_data[cleansed_data[indicator_raw_value].astype('float') == max_val].COUNTRY_NAME))

                # Define what min value to use for the normalization
                if min_val < q1 - whisker_factor * iqr:
                    min_to_use = q1 - whisker_factor * iqr
                    print('The distribution of the raw data values for this subgroup contains outliers or is too skewed on the lower end. The minimum value to be used for the normalisation is 1st quartile or distribution - {} * IQR. It is: {} \n See histogram printed below for info. \n'.format(whisker_factor, min_to_use))
                else:
                    min_to_use = min_val
                    print('The distribution of the raw data for this subgroup does not contain outliers or is too skewed on the lower end. The minimum value used for the normalisation is the minimum value in the dataset, which is {}. This value corresponds to country: {} \n'.format(min_to_use, cleansed_data[cleansed_data[indicator_raw_value].astype('float') == min_val].COUNTRY_NAME))

                # If there are outliers or a skewed distribution, print the distribution for the user.
                if (min_val < q1 - whisker_factor * iqr) or (max_val > q3 + whisker_factor * iqr):
                    print('\n This is the distribution of the raw data of the indicator.')
                    print(cleansed_data_subset[indicator_raw_value].hist(bins = 30))

                # Define the value range that is used for the scaling (normalization)
                tot_range = max_val - min_val

                # Compute the normalized value of the raw data in the column "SCALED"
                    # Distinguish between indicators, whose value must be inverted
                if inverted == True:
                    cleansed_data_subset['SCALED'] = round(10 - 10 * (cleansed_data_subset[indicator_raw_value].astype('float') - min_val)/ tot_range, 2)
                else:
                    cleansed_data_subset['SCALED'] = round(10 * (cleansed_data_subset[indicator_raw_value].astype('float') - min_val)/ tot_range, 2)

                # iv) Append the subset including its scaled value to the final returned dataframe
                    # Right join to have all countries from the final crba master list
                cleansed_data_subset_rj = cleansed_data_subset.merge(
                       right = crba_final_country_list,
                      how = 'right',
                      left_on = cleansed_df_iso2_col,
                      right_on = crba_final_country_list_iso_col,
                      indicator = 'RJ_CRBA_FULL_LIST'
                  )

                    # Append the values
                cleansed_data_full = cleansed_data_full.append(cleansed_data_subset_rj)

            except:
                print('Dataframe is empty. There are no values to append.')

    # Bring the final dataframe with scaled (normalized) values from wide to long format
        # Prepare the melting of the dataframe, by defining what columns remain untouched by the melt
    kept_columns = [x for x in cleansed_data_full.columns.tolist() if x not in [indicator_raw_value, 'SCALED']]

        # Bring the dataframe from wide to long format and return it
    long_format = pd.melt(cleansed_data_full,
               id_vars = kept_columns,
               value_vars = [indicator_raw_value, 'SCALED'],
               var_name = 'VALUE_TYPE')

        # Assign indicator code and name
    long_format['INDICATOR_CODE'] = indicator_code
    long_format['INDICATOR_NAME'] = indicator_name

    #return final dataframe
    return(long_format)
