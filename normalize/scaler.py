import pandas as pd
import numpy as np
    
def scaler(raw_dataframe, indicator_raw_value, cat_var = False, cat_scoring_type = None, inverted = False, whisker_factor = 1.5):
    """ Transform raw data into scaled (normalized) data in long format
    
    Parameters:
    raw_dataframe (obj): Raw dataset, pandas dataframe
    indicator_raw_value (str): Column containing the actual raw data values 
    cat_var (bool): is variable of type categorical or numerical?
    cat_scoring_type (str): For categorical variables, specify the scoring type (i.e. what values the categorical variable can take)
    inverted (bool): If we are dealing with a numerical variable, should the values be inverted?
    whisker_factor (float): 

    Returns:
    obj: Returns numpy array with transformed values
    """
    # Check if an indicator is numeric of categorical
    if cat_var == True:
        # This is the section dealing with categorical indicators
        # There are various scoring_types a categorical variable can take. For each one, different labels apply to the values
        # Type 2-1-0
        if cat_scoring_type == 'Type 2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 0),
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2)
                ]
            
            # Specify the normalized values
            norm_values = ['No data', '0.00', '10.00']

        # Type 2-1
        elif cat_scoring_type == 'Type 2-1':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2)
                ]
            
            # Specify the normalized values
            norm_values = ['0.00', '10.00']

        # Type 3-2-1
        elif cat_scoring_type == 'Type 3-2-1':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2),
                (raw_dataframe[indicator_raw_value] == 3)
                ]
            
            # Specify the normalized values
            norm_values = ['0.00', '5.00', '10.00']

        # Type 3-2-1-0
        elif cat_scoring_type == 'Type 3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 0)
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2),
                (raw_dataframe[indicator_raw_value] == 3)
                ]
            
            # Specify the normalized values
            norm_values = ['No data', '0.00', '5.00', '10.00']            
            
        # Type 4-3-2-1
        elif cat_scoring_type == 'Type 4-3-2-1':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2),
                (raw_dataframe[indicator_raw_value] == 3),
                (raw_dataframe[indicator_raw_value] == 4)
                ]
            
            # Specify the normalized values
            norm_values = ['0.00', '3.33', '6.67', '10.00']                
            
            
            # create a new column and use np.select to assign values to it using our lists as arguments
            raw_dataframe['SCALED'] = np.select(conditions, values)

        # Type 4-3-2-1-0
        elif cat_scoring_type == 'Type 4-3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 0)
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2),
                (raw_dataframe[indicator_raw_value] == 3),
                (raw_dataframe[indicator_raw_value] == 4)
                ]
            
            # Specify the normalized values
            norm_values = ['No data', '0.00', '3.33', '6.67', '10.00'] 


        # Type 5-4-3-2-1-0
        elif cat_scoring_type == 'Type 5-4-3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 0)
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2),
                (raw_dataframe[indicator_raw_value] == 3),
                (raw_dataframe[indicator_raw_value] == 4),
                (raw_dataframe[indicator_raw_value] == 5)
                ]
            
            # Specify the normalized values
            norm_values = ['No data', '0.00', '2.50', '5.00', '7.50', '10.00']
            
        # Type 7-6-5-4-3-2-1-0
        elif cat_scoring_type == 'Type 7-6-5-4-3-2-1-0':
            # Specify the values the raw data can take
            conditions = [
                (raw_dataframe[indicator_raw_value] == 0)
                (raw_dataframe[indicator_raw_value] == 1),
                (raw_dataframe[indicator_raw_value] == 2),
                (raw_dataframe[indicator_raw_value] == 3),
                (raw_dataframe[indicator_raw_value] == 4),
                (raw_dataframe[indicator_raw_value] == 5),
                (raw_dataframe[indicator_raw_value] == 6),
                (raw_dataframe[indicator_raw_value] == 7)
                ]
            
            # Specify the normalized values
            norm_values = ['No data', '0.00', '1.67', '3.33', '5.00', '6.67', '8.33', '10.00'] 
        
        else:
            raise('The scoring type you have specified does not exist. Make sure your scoring type is listed in the documentation of this fuction.') 
        
        # create a new column and use np.select to assign values to it using our lists as arguments
        raw_dataframe['SCALED'] = np.select(conditions, values)
            
    else:
        # This is the section dealing with numerical indicators

        # Determine basic descriptive statistics of the distribution
        min_val = np.nanmin(raw_dataframe[indicator_raw_value].astype('float'))
        max_val = np.nanmax(raw_dataframe[indicator_raw_value].astype('float'))
        q1 =  raw_dataframe[indicator_raw_value].astype('float').quantile(q=0.25)
        q2 =  raw_dataframe[indicator_raw_value].astype('float').quantile(q=0.50)
        q3 =  raw_dataframe[indicator_raw_value].astype('float').quantile(q=0.75)                      
        iqr = q3 - q1
        
        # Define what max value to use for the normalization            
        if max_val > q3 + whisker_factor * iqr:
            max_to_use = q3 + whisker_factor * iqr
            print('The distribution of the raw data values contains outliers or is too skewed on the upper end. The maximum value to be used for the normalisation is: 3rd quartile or distribution + {} * IQR. It is: {} \n See histogram printed below for info. \n'.format(whisker_factor, max_to_use))
        else:
            max_to_use = max_val
            print('The distribution of the raw data does not contain outliers or is too skewed on the upper end. The maximum value used for the normalisation is the maximum value in the dataset, which is {}. This value corresponds to country: {} \n'.format(max_to_use, raw_dataframe[raw_dataframe[indicator_raw_value].astype('float') == max_val].COUNTRY_NAME))
                      
        # Define what min value to use for the normalization
        if min_val < q1 - whisker_factor * iqr:
            min_to_use = q1 - whisker_factor * iqr
            print('The distribution of the raw data values contains outliers or is too skewed on the lower end. The minimum value to be used for the normalisation is 1st quartile or distribution - {} * IQR. It is: {} \n See histogram printed below for info. \n'.format(whisker_factor, min_to_use))
        else:
            min_to_use = min_val
            print('The distribution of the raw data does not contain outliers or is too skewed on the lower end. The minimum value used for the normalisation is the minimum value in the dataset, which is {}. This value corresponds to country: {} \n'.format(min_to_use, raw_dataframe[raw_dataframe[indicator_raw_value].astype('float') == min_val].COUNTRY_NAME))
        
        # If there are outliers or a skewed distribution, print the distribution for the user.
        if (min_val < q1 - whisker_factor * iqr) or (max_val > q3 + whisker_factor * iqr):
            print('\n This is the distribution of the raw data of the indicator.')
            print(raw_dataframe[indicator_raw_value].hist(bins = 30))
                       
        # Define the value range that is used for the scaling (normalization)
        tot_range = max_val - min_val 
        
        # Distinguish between indicators, whose value must be inverted
        if inverted == True:
            temp = raw_dataframe.assign(SCALED = round(10 - 10 * (raw_dataframe[indicator_raw_value].astype('float') - min_val)/ tot_range, 2))
        else:
            temp = raw_dataframe.assign(SCALED = round(10 * (raw_dataframe[indicator_raw_value].astype('float') - min_val)/ tot_range, 2))
        
        # bring dataframe from wide to long format
        # Prepare the melting of the dataframe, by defining what columns remain untouched by the melt
    kept_columns = [x for x in raw_dataframe.columns.tolist() if x not in [indicator_raw_value, 'SCALED']]

    return(
        pd.melt(temp,
           id_vars = kept_columns,
           value_vars = [indicator_raw_value, 'SCALED'],
           var_name = 'VALUE_TYPE')
    )