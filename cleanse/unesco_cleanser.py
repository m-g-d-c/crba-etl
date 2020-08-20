import pandas as pd

def cleanse_unesco_api_data(uis_data, country_df, country_df_iso2_col, columns, most_recent_only = True):
    """< To do >

    Parameters:
    uis_data (obj): The return of the extract function of extract_unesco_api_data
    country_df (obj): Pandas dataframe containing the final country list to be used in CRBA including country names, and their ISO2 and ISO 3 code.
    counry_df_iso2_col (str): The column containing the ISO codes in the country_df dataframe. Specified as string.
    columns: List of the dimensions (i.e. columns) that are you want to keep for the indicator beyond the main necessary ones
    most_recent_only (bool): Execute the group by statement to only kee the most recent observation per country. 

    Returns:
    obj: Returns pandas dataframe with the relevant data of the indicator

   """
    # Discard unnecessay columns
    columns_kept = ['REF_AREA', 'TIME_PERIOD','OBS_VALUE'] + columns
    uis_data = uis_data[columns_kept]
    
    if most_recent_only == True:
        # Retrieve the most up-to-date number for each country
        uis_data = uis_data[uis_data['TIME_PERIOD']==uis_data.groupby('REF_AREA')['TIME_PERIOD'].transform('max')]
    else:
        pass

    # Discard rows of countries that are not in the master country list
    cleansed_final = uis_data.merge(
        right = country_df,
        how = 'right', 
        left_on = 'REF_AREA',
        right_on = country_df_iso2_col,
        validate = 'one_to_one').assign(indicator = 'I-15')
    
    return(cleansed_final.sort_values(by = country_df_iso2_col, axis = 0))
