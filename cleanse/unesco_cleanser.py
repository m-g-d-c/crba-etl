import pandas as pd

def cleanse_unesco_api_data(uis_data, country_iso2_list, columns, most_recent_only = True):
    """< To do >

    Parameters:
    s15_raw (obj): Should be return of function s55_extract or s56_extract
    country_iso2_list (array): A numpy array of 2-character country codes.
    columns: List of the dimensions (i.e. columns) that are you want to keep for the indicator beyond the main necessary ones

    Returns:
    obj: Returns pandas dataframe with the relevant data of the indicator

   """
    # Discard unnecessay columns
    columns_kept = ['REF_AREA', 'TIME_PERIOD','OBS_VALUE'] + columns
    uis_data = uis_data[columns_kept]
    
    # Discard rows of countries that are in the master country list
    uis_data = uis_data[uis_data.REF_AREA.isin(country_iso2_list)]
    
    if most_recent_only == True:
        # Retrieve the most up-to-date number for each country
        uis_data = uis_data[uis_data['TIME_PERIOD']==uis_data.groupby('REF_AREA')['TIME_PERIOD'].transform('max')]
    else:
        pass
    
    return(uis_data)