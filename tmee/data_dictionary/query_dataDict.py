
import pandas as pd

# Functions that operate directly from our data-dictionary in Excel using pandas
# Typically retrieve queries, useful for our ETL purposes

def get_API_code_address(excel_data_dict):
    """ filters all indicators that are extracted by API
        :param excel_data_dict: path/file to our excel data dictionary in repo
        :return: pandas dataframe (df) with code and address for API requests
    """
    # read snapshots table from excel data-dictionary
    snapshot_df = pd.read_excel(excel_data_dict, sheet_name='Snapshot')
    # read sources table from excel data-dictionary
    sources_df = pd.read_excel(excel_data_dict, sheet_name='Source')
    # join snapshot and source based on Source_Id
    # 'left' preserves key order from snapshots
    snap_source_df = snapshot_df.merge(sources_df,on='Source_Id',how='left',sort=False)
    # get list of indicator codes and address with API extraction
    logic_API = snap_source_df.Type == 'API'
    api_code_addr_df = snap_source_df[logic_API][['Code','Address']]
    # return dataframe (note its df index will correspond to that of snapshots)
    return api_code_addr_df