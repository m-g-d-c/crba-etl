"""
Some indicators require a pre-processing before they can be stored as 
meaningful raw data. 

This script uses various artifacts in the data_in folder to perform
these pre-processing steps and store the raw data in the data/data_raw folder.

Specifically, it is about the following sources/ indicators: 

* S-157: Take number of deaths of children <5 years as indicated by raw data (absolute number) 
and calculate the number per 100.000 children <5 years to make indicator comparable 
across countries
    - Raw data from s. indicator dictionary under sheet source > S-157
    - Total number of children under age 5 data taken from: 
    https://data.unicef.org/resources/data_explorer/unicef_f/?ag=UNICEF&df=GLOBAL_DATAFLOW&ver=1.0&dq=.DM_POP_U5._T+M+F.&startPeriod=2015&endPeriod=2020 
* S-180: Take the raw data of displaces people (absolute numbers per country)
and calculate the number per 100.00 population for each country to make numbers
comparable across countries
    - Raw data from s. indicator dictionary under sheet source > S-180
    - Population data taken from UN: https://population.un.org/wpp/Download/Standard/Population/
    > click on "Total Population - Both Sexes" or directly follow this link
    https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/EXCEL_FILES/1_Population/WPP2019_POP_F01_1_TOTAL_POPULATION_BOTH_SEXES.xlsx
* S-181: equivalent to S-180
"""
# # # # # S-157


# # # # # S-180 and S-181
# Read data
un_pop_tot = pd.read_excel(
    io=data_in / "WPP2019_POP_F01_1_TOTAL_POPULATION_BOTH_SEXES.xlsx",
    sheet_name="ESTIMATES",
    header=16,
)

# Load the list of countries which contains all different variations of country names
country_full_list = pd.read_excel(
    data_in / "all_countrynames_list.xlsx", keep_default_na=False
).drop_duplicates()

# Load raw data of S-180 and S-181
S_180_S_181 = pd.read_excel(
    data_in
    / "data_raw_manually_extracted"
    / "S-180, S-181, S-189 idmc_displacement_all_dataset.xlsx"
).drop(
    0
)  # delete first row containing strings

# Cast year as string, required for merge command later
S_180_S_181["Year"] = S_180_S_181["Year"].astype(str)

# Define list of columns corresponding to year name columns
years = [str(x) for x in list(range(1950, 2021))]

# bring dataframe from wide to longformat
un_pop_tot = un_pop_tot.melt(
    id_vars=[
        "Index",
        "Variant",
        "Region, subregion, country or area *",
        "Notes",
        "Country code",
        "Type",
        "Parent code",
    ],
    value_vars=years,
    var_name="year",
    value_name="population",
)

# Add ISO3 code to the list to prepare for join
un_pop_tot = un_pop_tot.merge(
    right=country_full_list,
    how="outer",
    left_on="Region, subregion, country or area *",
    right_on="COUNTRY_NAME",
)

# - - - - - - - - - - - -
# Join raw data and population data together
s_180_s_181_raw = un_pop_tot.merge(
    right=S_180_S_181,
    how="outer",
    # on="ISO3_YEAR"
    left_on=["COUNTRY_ISO_3", "year"],
    right_on=["ISO3", "Year"],
)

# Create S_180
s_180_raw = s_180_s_181_raw

# Calculate target KPI (number of Internally displaced people per 100.000 people)
s_180_raw["RAW_OBS_VALUE"] = s_180_raw["Conflict Stock Displacement"] / (
    s_180_raw["population"] * 100
)

# Add unit measure
s_180_raw[
    "ATTR_UNIT_MEASURE"
] = "Total number of IDPs (conflict and violence) per 100.000 people. Calculated as 'Total Number of IDPs (Conflict and violence)' taken from https://www.internal-displacement.org/database/displacement-data multiplied by 100 and divided by 'Total Population (given in 1.000)' taken from https://population.un.org/wpp/Download/Standard/Population//"

# Store data
s_180_raw.to_csv(data_sources_staged_raw / "S-180_staged_raw.csv", sep=";")

# Create S_180
s_181_raw = s_180_s_181_raw

# Calculate target KPI (number of Internally displaced people per 100.000 people)
s_181_raw["RAW_OBS_VALUE"] = (
    s_181_raw["Conflict New Displacements"] / (s_181_raw["population"]) * 100
)

# Add unit measure
s_181_raw[
    "ATTR_UNIT_MEASURE"
] = "Number of new IDPs (conflict and violence) per 100.000 people for a given year. Calculated as 'Number of new IDPs (Conflict and violence)' in a given year taken from https://www.internal-displacement.org/database/displacement-data multiplied by 100 and divided by 'Total Population (given in 1.000)' taken from https://population.un.org/wpp/Download/Standard/Population//"

# Store data
s_181_raw.to_csv(data_sources_staged_raw / "S-181_staged_raw.csv", sep=";")

# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-185, S-186, S-187, S-188 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

# Create list to loop through
source_list = [
    [
        "https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode=VC_DSR_PDLN&pageSize=999999999",
        "S-185_staged_raw.csv",
        "Derived from SDG Indicator 1.5.1: Indicator 1.5.1, 11.5.1, 13.1.1, Series:  Number of people whose livelihoods were disrupted or destroyed, attributed to disasters. Instead of total, absolute number, this indicator indicates number per 100.000 population",
    ],
    [
        "https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode=VC_DSR_ESDN&pageSize=999999999",
        "S-186_staged_raw.csv",
        "Derived from SDG Indicator 11.5.2 (code VC_DSR_ESDN) Number of disruptions to educational services attributed to disasters. Instead of total, absolute number, this indicator indicates number per 100.000 population",
    ],
    [
        "https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode=VC_DSR_HSDN&pageSize=999999999",
        "S-187_staged_raw.csv",
        "Derived from SDG Indicator 11.5.2 (code VC_DSR_HSDN): Number of disruptions to health services attributed to disasters. Instead of total, absolute number, this indicator indicates number per 100.000 population",
    ],
    [
        "https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode=VC_DSR_OBDN&pageSize=999999999",
        "S-188_staged_raw.csv",
        "Derived from SDG Indicator 11.5.2 (code VC_DSR_OBDN): Number of disruptions to other basic services attributed to disasters. Instead of total, absolute number, this indicator indicates number per 100.000 population",
    ],
]

# Loop through all 4 sources
for element in source_list:
    dataframe = extract.JSONExtractor.extract(url=element[0])

    # Obtain the ISO2 and ISO3 codes
    dataframe = dataframe.merge(
        right=country_full_list,
        how="left",
        left_on="geoAreaName",
        right_on="COUNTRY_NAME",
    )

    # Cast year column as string for join
    dataframe.timePeriodStart = dataframe.timePeriodStart.astype(int).astype(str)

    # Join UN Population data to to obtain population size
    dataframe = un_pop_tot.merge(
        right=dataframe,
        how="outer",
        # on="ISO3_YEAR"
        left_on=["COUNTRY_ISO_3", "year"],
        right_on=["COUNTRY_ISO_3", "timePeriodStart"],
    )

    # Calculate target KPI (number of Internally displaced people per 100.000 people)
    dataframe["RAW_OBS_VALUE"] = dataframe["value"].astype(float) / (
        dataframe["population"] * 100
    )

    # Add unit measure
    dataframe["ATTR_UNIT_MEASURE"] = element[2]

    # Store data
    dataframe.to_csv(data_sources_staged_raw / element[1], sep=";")