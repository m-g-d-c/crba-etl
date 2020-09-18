import pandas as pd
import os

# Define the export path for all cleansed data sources
cwd = os.getcwd()

data_cleansed = cwd + "\data_out\data_cleansed\\"

def save_cleansed_data(dataframe, filename, output_path = data_cleansed):
    """Save raw data

    Parameters:
    dataframe (obj): Pandas dataframe to be stored
    filename (string): The way you would like o anem your file. Must include the extension (for example .xlsx)
    output_path (string): Folder where data is stored

    Returns:
    obj:

   """

    # Save data in "cwd/ data/data_cleansed"
    dataframe.to_excel(output_path + filename)

    # Provide log for user
    print('The raw data has been saved as .xlsx file in: ' + output_path)
