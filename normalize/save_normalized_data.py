import pandas as pd
import os

# Define the export path for all normalized data sources
cwd = os.getcwd()

data_normalized = cwd + "\data_out\data_normalized\\"

def save_normalized_data(dataframe, filename, output_path = data_normalized):
    """Save raw data

    Parameters:
    dataframe (obj): Pandas dataframe to be stored
    filename (string): The way you would like o anem your file. Must include the extension (for example .xlsx)
    output_path (string): Folder where data is stored

    Returns:
    obj:

   """
   # Save normalized data
    dataframe.to_excel(output_path + filename)

    # Provide log for user
    print('The raw data has been saved as .xlsx file in: ' + output_path)
