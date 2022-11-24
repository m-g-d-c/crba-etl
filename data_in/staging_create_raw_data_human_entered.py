"""
This file reads all manually extracted 
data entered by humans (as opposed to machine, i.e. Python),
pre-processes it and saves it as staged_raw_data file in the folder
defined in the variable

data_sources_staged_raw

It calls the function preprocess_manual_human_data from
the utils package. 

You must distinguish between categorical varibles and contiuous ones. 
"""


def preprocess_manual_human_data(
    file_name,
    variable_type,
    posix_folder_in=data_sources_raw_manual_human,
    posix_folder_out=data_sources_staged_raw,
    raw_obs_value_col="RAW_OBS_VALUE",
):
    """Pre-process manually extracted, human-entered data

    Read human-entered, manually extracted data and pre-process it to
    make it ready for the ETL pipeline. This requires:

    - Cleansing RAW_OBS_VALUE (taking out trailing and leading white spaces)
    - Properly encoding NaNs (which is different for categorical and numeric variables)

    Parameters:
    file_name (str): Name of file to be read  in posix_folder
    variable_type (str): Either 'categorical' or 'continuous'
    posix_folder_in (obj): Posix folder where file resides
    posix_folder_out (obj): Posix folder where staged_raw file is saved
    raw_obs_value_col (str): Name of column containing raw observation value
    """
    # Read dataframe
    dataframe = pd.read_excel(posix_folder_in / file_name, sheet_name="Blueprint")

    # Section ealing with categorial variables
    if variable_type == "categorical":
        # Delete trailing whitespace and numbers of parentheses in raw_OBS_VALUE
        dataframe[raw_obs_value_col] = (
            dataframe[raw_obs_value_col]
            .apply(lambda x: re.sub(" \(\d+\)", "", x) if type(x) == str else x)
            .apply(lambda x: x.strip() if type(x) == str else x)
        )

        # Encode missing data as "No data"
        dataframe.loc[
            dataframe[raw_obs_value_col].isnull(), "RAW_OBS_VALUE"
        ] = "No data"

    # Section dealing with numeric variables
    elif variable_type == "continuous":
        dataframe.loc[
            (dataframe[raw_obs_value_col] == "No data")
            | (dataframe[raw_obs_value_col] == "Insufficient data")
            | (dataframe[raw_obs_value_col] == "No legal measures ")
            | (dataframe[raw_obs_value_col] == "x"),
            raw_obs_value_col,
        ] = np.nan

    else:
        print("Must specify what type of variable it is")
    # Rename COUNTRY_NAME column, to avoid trouble of ETL pipeline down the line
    dataframe = dataframe.rename(columns={"COUNTRY_NAME": "country_col_not_used"})

    # print(dataframe.RAW_OBS_VALUE.unique())

    dataframe = dataframe.dropna(axis="columns", how="all")

    # print(dataframe.RAW_OBS_VALUE.unique())

    # Save data as staged raw data
    dataframe.to_csv(
        posix_folder_out
        / (re.sub("_", "-", re.sub(".xlsx", "", file_name)) + "_staged_raw.csv"),
        sep=";",
    )


# These are all soures wth categorical variables
categorical_indicators = [
    "S_12",
    "S_69",
    "S_70",
    "S_96",
    "S_87",
    "S_106",
    "S_109",
    "S_116",
    "S_117",
    "S_118",
    "S_119",
    "S_121",
    "S_122",
    "S_123",
    "S_139",
    "S_140",
    "S_147",
    "S_148",
    "S_149",
    "S_150",
    "S_151",
    "S_152",
    "S_164",
    "S_165",
    "S_172",
    "S_175",
    "S_176",
    "S_178",
    "S_179",
    "S_213",
    "S_228",
    "S_233",
    "S_234",
    "S_235",
    "S_236",
    "S_237",
    "S_238",
]

# These are all sources with numeric
numeric_indicators = [
    "S_46",
    "S_54",
    "S_128",
    "S_138",
    "S_194",
    "S_195",
    "S_196",
    "S_197",
]


# Pre-process categorical indicators
for file in categorical_indicators:
    print("Pre-processing file {}".format(file))
    preprocess_manual_human_data(file_name=file + ".xlsx", variable_type="categorical")

# Pre-process numeric indicators
for file in numeric_indicators:
    print("Pre-processing file {}".format(file))
    preprocess_manual_human_data(file_name=file + ".xlsx", variable_type="continuous")
