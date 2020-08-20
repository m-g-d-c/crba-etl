import pandas as pd

def extract_unesco_api_data(stat_unit, unit, edu_level, subs_key, start_period = 2010, end_period = 2020):
    """Extract raw data from the UNESCO API

    The API can be called with a API key, which you must generate by creating an account. 
    The calls can be done with simple https requests, in which many parameter must be specified (e.g. indicator code, sexes, age, ...). TO understand the structure, please check the query builder https://apiportal.uis.unesco.org/query-builder

    Parameters:
    stat_unit (string): Indicatiro code, I suggest you check the query builder https://apiportal.uis.unesco.org/query-builder to retrieve the right code
    unit (string): Specify the unnit of measure, e.g. 'PT' for percentage total
    edu_level (string): Choose educational level, L2 = lower secondary, L3 = upper secondary age
    subs_key (string): Your API key. You must create one. Visit https://apiportal.uis.unesco.org/getting-started for more info
    start_period (int): Year which is the beginning of series for which you want to collect data
    end_period (ing): Year which is the end of series for which you want to collect data

    Returns:
    obj: Returns pandas dataframe

   """
    url = 'https://api.uis.unesco.org/sdmx/data/SDG4/{sunit}.{umeasure}.{elevel}._T._T.SCH_AGE_GROUP._T.INST_T._Z._T._Z._Z._Z._T._T._Z._Z._Z.?startPeriod={syear}&endPeriod={eyear}&format=csv-sdmx&subscription-key={skey}'.format(
        sunit = stat_unit,
        umeasure = unit,
        elevel = edu_level,
        syear = start_period,
        eyear = end_period,
        skey = subs_key)
    return(pd.read_csv(filepath_or_buffer = url, 
                     sep = ',',))