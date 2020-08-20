import pandas as pd

def cleanse_sdg_api_data(raw_data):
    """Transform raw data from sdg API source to distil relevant data 

    Discard columns and aggregate data to retrieve the latest value for all countries in a given dataframe for a given SDG API series.

    Parameters:
    raw_data (obj): Return of function 'extract_sdg_api_data'. Should be a pandas dataframe 
    
    Returns:
    obj: Returns pandas dataframe

   """
    # Extract relevant columns
    stage_data = raw_data[['geoAreaCode',
            'geoAreaName',
            'timePeriodStart',
            'value']]
    # Get the latest value for each country of the series
    return(stage_data[stage_data['timePeriodStart'] == stage_data.groupby('geoAreaName')['timePeriodStart'].transform('max')])
