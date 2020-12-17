import datetime

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
* S-11, S-120, S-124, S-134
"""
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
# # # # # # # Prepare UN population data # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
# Read data
un_pop_tot = pd.read_excel(
    io=data_in / "WPP2019_POP_F01_1_TOTAL_POPULATION_BOTH_SEXES.xlsx",
    sheet_name="ESTIMATES",
    header=16,
)

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

# Load the list of countries which contains all different variations of country names
country_full_list = pd.read_excel(
    data_in / "all_countrynames_list.xlsx", keep_default_na=False
).drop_duplicates()

# Add ISO3 code to the list to prepare for join
un_pop_tot = un_pop_tot.merge(
    right=country_full_list,
    how="outer",
    left_on="Region, subregion, country or area *",
    right_on="COUNTRY_NAME",
)

# Discard unnecessary columns
un_pop_tot = un_pop_tot[["year", "population", "COUNTRY_ISO_3"]]


# # # # # # # # # IDMC Data  # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-180, S-181, S-189, S-230 # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # #

# Load raw data of S-180 and S-181
S_180_S_181_S189_S_230 = pd.read_excel(
    data_sources_raw_manual_machine
    / "S-180, S-181, S-189 S-230 idmc_displacement_all_dataset.xlsx"
).drop(
    0
)  # delete first row containing strings

# Cast year as string, required for merge command later
S_180_S_181_S189_S_230["Year"] = S_180_S_181_S189_S_230["Year"].astype(str)

# Join raw data and population data together
S_180_S_181_S189_S_230_raw = un_pop_tot.merge(
    right=S_180_S_181_S189_S_230,
    how="right",
    # on="ISO3_YEAR"
    left_on=["COUNTRY_ISO_3", "year"],
    right_on=["ISO3", "Year"],
)

# Define ATTR_UNIT_MEASURE strings
s_180_aum = "Total number of internally displaced persons (conflict and violence) per 100.000 people. Calculated as 'Total Number of IDPs (Conflict and violence)' taken from https://www.internal-displacement.org/database/displacement-data multiplied by 100 and divided by 'Total Population (given in 1.000)' taken from https://population.un.org/wpp/Download/Standard/Population//"
s_181_aum = "Number of new internally displaced persons (conflict and violence) per 100.000 people for a given year. Calculated as 'Number of new IDPs (Conflict and violence)' in a given year taken from https://www.internal-displacement.org/database/displacement-data multiplied by 100 and divided by 'Total Population (given in 1.000)' taken from https://population.un.org/wpp/Download/Standard/Population//"
s_189_aum = "Number of new internally displaced persons (natural disasters) per 100.000 people for a given year. Calculated as 'Number of new IDPs (Conflict and violence)' in a given year taken from https://www.internal-displacement.org/database/displacement-data multiplied by 100 and divided by 'Total Population (given in 1.000)' taken from https://population.un.org/wpp/Download/Standard/Population//"
s_230_aum = "Total number of internally displaced persons (natural disasters) per 100.000 people. Calculated as 'Total Number of IDPs (Conflict and violence)' taken from https://www.internal-displacement.org/database/displacement-data multiplied by 100 and divided by 'Total Population (given in 1.000)' taken from https://population.un.org/wpp/Download/Standard/Population//"


# Create list to loop through
idmc_list = [
    ["S-180_staged_raw.csv", "Conflict Stock Displacement", s_180_aum],
    ["S-181_staged_raw.csv", "Conflict New Displacements", s_181_aum],
    ["S-189_staged_raw.csv", "Disaster New Displacements", s_189_aum],
    ["S-230_staged_raw.csv", "Disaster Stock Displacement", s_230_aum],
]

# Loop through list
for element in idmc_list:
    # Extract right columns
    dataframe = S_180_S_181_S189_S_230_raw[["ISO3", "Year", "population", element[1]]]

    # Calculate target kpi --> Normalize to per 100.000 persons
    dataframe["RAW_OBS_VALUE"] = (
        dataframe[element[1]] / (dataframe["population"]) * 100
    )  # Pop given inthousands, we want number per 100.000 pop

    # Add unit measure
    dataframe["ATTR_UNIT_MEASURE"] = element[2]

    # Save data
    dataframe.to_csv(data_sources_staged_raw / element[0], sep=";")

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
        how="right",
        # on="ISO3_YEAR"
        left_on=["COUNTRY_ISO_3", "year"],
        right_on=["COUNTRY_ISO_3", "timePeriodStart"],
    )

    # Calculate target KPI (number of Internally displaced people per 100.000 people)
    dataframe["RAW_OBS_VALUE"] = (
        dataframe["value"].astype(float) / (dataframe["population"]) * 100
    )  # Pop given inthousands, we want number per 100.000 pop

    # Add unit measure
    dataframe["ATTR_UNIT_MEASURE"] = element[2]

    # Rename columns to avoid double_naming of column, which produces error down the ETL line
    dataframe = dataframe.rename(
        columns={
            "COUNTRY": "COUNTRY_ISO_3",
            "geoAreaName": "country_col_not_used",
            "year": "year_not_used",
            "COUNTRY_NAME": "country_col_2_not_used",
            "COUNTRY_ISO_2": "country_col_3_not_used",
            "value": "raw_value_before_normalisation",
            "attributes.Units": "unit_measure_not_used",
        }
    )

    # Store data
    dataframe.to_csv(data_sources_staged_raw / element[1], sep=";")

# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-157 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
# Obtainraw data
dataframe = extract.CSVExtractor.extract(
    url="http://apps.who.int/gho/athena/api/GHO/AIR_4.csv"
)

# We only have the population data for both sexes, so discrd other dimensionsubgroups
dataframe = dataframe.loc[dataframe.SEX == "BTSX"]

# Obtain population data
population_data = extract.CSVExtractor.extract(
    url="https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/UNICEF,DM,1.0/.DM_POP_U5...?format=sdmx-csv&startPeriod=2015&endPeriod=2020"
)

# Extract ISO3 code from population data
population_data["COUNTRY_ISO_3"] = population_data["REF_AREA:Geographic area"].apply(
    lambda x: re.findall("^\w+", x)[0]
)

# Discard unnecessary columns
population_data = population_data[
    ["TIME_PERIOD:Time period", "OBS_VALUE:Observation Value", "COUNTRY_ISO_3"]
]

# Join population data to raw data
s_157 = population_data.merge(
    right=dataframe, how="right", left_on="COUNTRY_ISO_3", right_on="COUNTRY"
)

# Compute target raw observation value
s_157["RAW_OBS_VALUE"] = s_157["Numeric"] / (s_157["OBS_VALUE:Observation Value"]) * 100

# Add attribute
s_157[
    "ATTR_UNIT_MEASURE"
] = "The burden of disease attributable to ambient air pollution expressed as Number of deaths of children under 5 years per 100.000 children under 5 years. Note: Data about deaths drawm from WHO (https://apps.who.int/gho/data/node.imr.AIR_4?lang=en), refering to year 2016. Data about population under 5 years drawn from UNICEF (https://data.unicef.org/resources/data_explorer/unicef_f/?ag=UNICEF&df=GLOBAL_DATAFLOW&ver=1.0&dq=.DM_POP_U5..&startPeriod=2008&endPeriod=2018), referring to year 2018. Due to a lack of matching data, the WHO data from 2016 had to be normalized with population data from 2018."

# Rename clumns
s_157 = s_157.rename(
    columns={
        "TIME_PERIOD:Time period": "time_period_not_used",
        "OBS_VALUE:Observation Value": "obs_value_not_used",
        "COUNTRY": "country_col_not_used",
        "Display Value": "obs_value_no_used_2",
    }
)

# Store data
s_157.to_csv(data_sources_staged_raw / "S-157_staged_raw.csv", sep=";")

# # # # # # # Economist intelligence unit # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-11, S-120, S-124, S-134 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

### Economist data
S_11_S120_s_124_s_134 = pd.read_excel(
    data_sources_raw_manual_machine
    / "S-11, S-120, S-124, S-134 OOSI_Out_of_the_shadows_index_60-countries_May2019.xlsm",
    sheet_name="Ranking",
    header=17,
    usecols=[
        "Unnamed: 11",
        "Score",
        "Unnamed: 20",
        "Score.1",
        "Unnamed: 29",
        "Score.2",
        "Unnamed: 38",
        "Score.3",
        "Unnamed: 47",
        "Score.4",
    ],
)

# Define list to extract the relevant columns and save them as raw data csv
eit_list = [
    ["S-11_staged_raw.csv", ["Score.2", "Unnamed: 29"]],
    ["S-120_staged_raw.csv", ["Score.2", "Unnamed: 29"]],
    ["S-124_staged_raw.csv", ["Score.1", "Unnamed: 20"]],
    ["S-134_staged_raw.csv", ["Score.3", "Unnamed: 38"]],
    ["S-229_staged_raw.csv", ["Score.4", "Unnamed: 47"]],
]

# Loop through list
for element in eit_list:
    # Extract right columns
    dataframe = S_11_S120_s_124_s_134[element[1]]

    # Rename clumns
    dataframe = dataframe.rename(
        columns={element[1][0]: "RAW_OBS_VALUE", element[1][1]: "COUNTRY_NAME"}
    )

    # Add year column
    dataframe["TIME_PERIOD"] = 2019

    # Save data
    dataframe.to_csv(data_sources_staged_raw / element[0], sep=";")


# # # # # # # ICRC treaties # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-168, S-169, S170 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

try:
    # Try loading data from endpoint (preferred)
    S_168_169_170 = pd.read_excel(
        "http://ihl-databases.icrc.org/applic/ihl/ihl.nsf/xsp/.ibmmodres/domino/OpenAttachment/applic/ihl/ihl.nsf/40BAD58D71673B1CC125861400334BC4/%24File/IHL_and_other_related_Treaties.xls?Open",
        sheet_name="IHL and other related Treaties",
        header=1,
    )
except:
    # Load from local file if endpoint is donw
    S_168_169_170 = pd.read_excel(
        data_sources_raw_manual_machine
        / "S-168, S-169, S-170-IHL_and_other_related_Treaties.xls",
        sheet_name="IHL and other related Treaties",
        header=1,
    )

    # Log
    print(
        "Data for sources S_168, S_169, and S_170 could not be extracted URL endpoint. Loaded data from local repository."
    )


# Create list to loop through
icrc_list = [
    ["S-168_staged_raw.csv", "GC I-IV 1949"],
    ["S-169_staged_raw.csv", "AP I 1977"],
    ["S-170_staged_raw.csv", "AP II 1977"],
]


# Loop through list
for element in icrc_list:
    # Extract right columns
    dataframe = S_168_169_170[["Country", element[1]]]

    # Convert datetime format
    dataframe[element[1]] = dataframe[element[1]].apply(
        lambda x: f"{x.year}-{x.month}-{x.day}" if isinstance(x, datetime.date) else x
    )

    # Rename clumns
    dataframe = dataframe.rename(columns={element[1]: "ATTR_RATIFICATION_DATE"})

    # Add year column
    dataframe["TIME_PERIOD"] = 2020

    # Save data
    dataframe.to_csv(data_sources_staged_raw / element[0], sep=";")

    # These indicators will probably require special attention/ a disting sort of pipeline
    # The below code works just fine, but still need to include the savings part --> Where to put this loop bit?

# # # # # # # CRIN treaties # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-131, S-193 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
try:
    # Try loading data from endpoint (preferred)
    S_131_193 = pd.read_excel(
        "https://archive.crin.org/sites/default/files/access_to_justice_data.xls",
        sheet_name="All countries",
        header=1,
    ).drop(
        [0, 1]
    )  # drop rows that don't contain data
except:
    # Load from local file if endpoint is donw
    S_131_193 = pd.read_excel(
        data_sources_raw_manual_machine / "S-131, S-193-access_to_justice_data.xls",
        sheet_name="All countries",
        header=1,
    ).drop(
        [0, 1]
    )  # drop rows that don't contain data

    # Log
    print(
        "Data for sources S-131 and S-193 could not be extracted URL endpoint. Loaded data from local repository."
    )


# Create list to loop through
crin_list = [
    ["S-131_staged_raw.csv", "Unnamed: 2"],
    ["S-193_staged_raw.csv", "Sub-total"],
]

# Loop through list
for element in crin_list:
    # Extract right columns
    dataframe = S_131_193[["Unnamed: 1", element[1]]]

    # Add year column
    dataframe["TIME_PERIOD"] = 2016

    # Rename columns
    dataframe = dataframe.rename(
        columns={
            "Unnamed: 1": "COUNTRY_NAME",
            "Unnamed: 2": "RAW_OBS_VALUE",
            "Sub-total": "RAW_OBS_VALUE",
        }
    )

    # Save data
    dataframe.to_csv(data_sources_staged_raw / element[0], sep=";")

# # # # # # # FCTC data # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-89 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

# Load from local file if endpoint is donw
S_89 = pd.read_excel(
    data_sources_raw_manual_machine / "S-89 Answers_v2.xlsx",
)

# Unpivot
S_89 = S_89.melt(id_vars="Party", var_name="TIME_PERIOD", value_name="RAW_OBS_VALUE")

# save data
S_89.to_csv(data_sources_staged_raw / "S-89_staged_raw.csv", sep=";")


# # # # # # # Landmark data # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-167 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

# Read data
S_167 = pd.read_excel(
    data_sources_raw_manual_machine
    / "S_167_Pct_IP_CommunityLands"
    / "Pct_IP_CommunityLands_20170623.xls",
    sheet_name="Pct_IP_CommunityLands",
)

# The UK is split up here in its terriroties, must aggregate the data to UK-national level again

# Pandas interprets the percentage column sometimes as string, sometimes as number  --> Convert all to numbers (in %, i.e. between 0 - 100)
S_167["IC_F"] = S_167["IC_F"].apply(
    lambda x: x * 100
    if type(x) != str
    else ("No data" if x == "No data" else float(re.sub("%", "", x)))
)

# Sub selection of all GBR terriroties
country_land_gbr = S_167.loc[236:239, "Ctry_Land"]

# Calculate total area size of Great Britain
total_land_gbr = sum(country_land_gbr)

# Obtain percentage of indigenous land of all UK areas
land_percentage = S_167.loc[
    236:239, "IC_F"
]  # .apply(lambda x: float(re.sub('%', '', x)) if type(x)==str else x * 100)

# Compute total sqm rather than % of total land of indigenous land
indi_land_tot = []
for i in range(len(land_percentage)):
    indi_land_tot += [land_percentage.iloc[i] * country_land_gbr.iloc[i] / 100]

# Total sum of all indiegnous land of all UK territories
total_indi_land = sum(indi_land_tot)

# Percentage of indigenous land in ALL UK
total_indi_land_percent = total_indi_land / total_land_gbr * 100

# Store all of this data in a dataframe to append it to the existing dataframe
uk_df = pd.DataFrame(
    [
        [
            "GBR",
            np.nan,
            np.nan,
            "United Kingdom",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            total_indi_land_percent,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "According to the Eurostats study on common lands in Europe of 2010, common lands in the UK is always permanent grassland in the form of rough grazing. Much of these lands are found in the remote upland areas, and in many instance they have at least one special designation preventing their agricultural improvement. The area of these common lands across the UK are :591 901 ha in Scotland, 427 889 ha in England, 180 305 ha in Wales, 36 438 ha in Northern Ireland. Source : Eurostat, 2010. Farm structure survey - common land. Available at : http://ec.europa.eu/eurostat/statistics-explained/index.php?title=Common_land_statistics_-_background&oldid=262743<br>Jones, Gwyn. 2015. Common Grazing in Scotland - importance, governance, issues. Presentation at the EFNCP, available at : http://www.efncp.org/download/common-ground2015/Commons10Jones.pdf. There are also some lands held in in community ownership in Scotland. The latest figure of their total area is 0.19 Mha (470 094 acres). Based on a land mass of Scotland of 7.79 Mha (19.25 M acres), this would represent 2.44% of Scotland’s land mass. This figure has been calculated using the definition of Community Ownership that was agreed by the Scottish Government Short Life Working Group on community land ownership (I million acre target group) in September 2015. <br>Source: Peter Peacock, Community Land Scotland, personal communication. 2015/09/21. The component countries of  the United Kingdom have been treated separately on LandMark.",
        ]
    ],
    columns=S_167.columns,
)

# Append dataframe
S_167 = S_167.append(other=uk_df)

# Add time period
S_167["TIME_PERIOD"] = 2017

# Rename country column (to have it dsiregardedby the ETL pipeline) --> ONly rely on ISO3 column
S_167 = S_167.rename(columns={"Country": "country_name_not_used"})

# COnvert "No Data" strings into np.nan
S_167.loc[S_167["IC_F"] == "No data", "IC_F"] = np.nan

# save data
S_167.to_csv(data_sources_staged_raw / "S-167_staged_raw.csv", sep=";")

# # # # # # # UCW data # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-21 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

# Load from local file if endpoint is donw
# NB: The downloaded file was corrupted (xls), so I opene it in Excel and resaved it as xlsx file.
S_21 = pd.read_excel(
    data_sources_raw_manual_machine / "S-21-total-HIZkLiYK.xlsx", header=1
)

# Unpivot
S_21 = S_21.melt(
    id_vars="Unnamed: 0", var_name="TIME_PERIOD", value_name="RAW_OBS_VALUE"
)

# Rename column
S_21 = S_21.rename(columns={"Unnamed: 0": "COUNTRY_NAME"})

# Change value '..' to NaN
S_21.loc[S_21.RAW_OBS_VALUE == "..", "RAW_OBS_VALUE"] = np.nan

# Save dataframe
S_21.to_csv(data_sources_staged_raw / "S-21_staged_raw.csv", sep=";")

# # # # # # # Global Slavery Index # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-60 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

# Load from local file if endpoint is donw
S_60 = pd.read_excel(
    data_sources_raw_manual_machine
    / "S-60_FINAL-GSI-2018-DATA-G20-AND-FISHING-1597151668.xlsx",
    header=2,
    sheet_name="Global prev, vuln, govt table",
)

# THhs daat represents the 2018 global slavery index data. Add time period
S_60["TIME_PERIOD"] = 2018

# Add unit measure attribute
S_60[
    "ATTR_UNIT_MEASURE"
] = "Est. prevalence of population in modern slavery (victims per 1,000 population)"

# rename columns
S_60 = S_60.rename(
    columns={
        "Country ": "COUNTRY_NAME",
        "Est. prevalence of population in modern slavery (victims per 1,000 population)": "RAW_OBS_VALUE",
    }
)

# Save data
S_60.to_csv(data_sources_staged_raw / "S-60_staged_raw.csv", sep=";")

# # # # # # # Inform Risk Index data # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-190 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

try:
    # Try and pull data from endpoint if possible
    S_190 = pd.read_excel(
        "https://drmkc.jrc.ec.europa.eu/inform-index/Portals/0/InfoRM/2021/INFORM_Risk_2021_v050.xlsx?ver=2021-09-02-170624-200",
        header=1,
        sheet_name="INFORM Risk 2021 (a-z)",
    ).drop(0)
except:
    # Load from local file if endpoint is donw
    S_190 = pd.read_excel(
        data_sources_raw_manual_machine / "S-190_INFORM_Risk_2021_v050.xlsx",
        header=1,
        sheet_name="INFORM Risk 2021 (a-z)",
    ).drop(0)

    # Log
    print(
        "Data for source S-190 could not be extracted URL endpoint. Loaded data from local repository."
    )

# Rename column to avoid taking both country and iso3 column. Only take iso3 ()
S_190 = S_190.rename(
    columns={
        "COUNTRY": "country_col_not_used",  # could be anything not indcluded in column_mapping.py
        "INFORM RISK": "RAW_OBS_VALUE",
    }
)

# THhs daat represents data from 2021
S_190["TIME_PERIOD"] = 2021

# Save data
S_190.to_csv(data_sources_staged_raw / "S-190_staged_raw.csv", sep=";")


# # # # # # # Climate Watch data # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-159 and S-153  # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
# Read data
S_159 = pd.read_csv(
    data_sources_raw_manual_machine / "S-159_ghg-emissions.csv",
)

# Unpivot the data
S_159 = S_159.melt(
    id_vars=["Country/Region", "unit"],
    var_name="TIME_PERIOD",
    value_name="RAW_OBS_VALUE",
)

# save data
S_159.to_csv(data_sources_staged_raw / "S-159_staged_raw.csv", sep=";")

# Read data
S_153 = pd.read_csv(data_sources_raw_manual_machine / "S-153_ndc_content.csv")

# Cleanse target value variable (some encoding issues)
S_153.Value = S_153.Value.apply(lambda x: re.sub(";<br>.+", "", x))

# Cleanse target value variable (some encoding issues)
S_153["TIME_PERIOD"] = 2020

# Rename column so that it doesn't cause error downstream
S_153 = S_153.rename(
    columns={
        "Sector": "sector_not_used",
        "Subsector": "sub_sector_not_used",
    }
)

# Save file
S_153.to_csv(data_sources_staged_raw / "S-153_staged_raw.csv", sep=";")

# # # # # # # EITI # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-154  # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

# Extract data
s_154 = extract.JSONExtractor.extract(
    url="https://eiti.org/api/v2.0/implementing_country"
)

# This is the only indicator in this file, which is extracted with an extractor from the web
# For the sake of completeness store this data in the folder "data_raw_manually_extracted" as well
s_154.to_csv(data_sources_raw_manual_machine / "S-154_staged_raw.csv", sep=";")

# Add time period
s_154["TIME_PERIOD"] = 2020

# Rename OBS-VALUE column
s_154 = s_154.rename(
    columns={
        "status": "RAW_OBS_VALUE",
    }
)

# Save data
s_154.to_csv(data_sources_staged_raw / "S-154_staged_raw.csv", sep=";")
