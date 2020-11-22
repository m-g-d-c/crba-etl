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
    data_in
    / "data_raw_manually_extracted"
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

    # Store data
    dataframe.to_csv(data_sources_staged_raw / element[1], sep=";")


# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-157# # # # # # # # # # # # # # # # # # # #
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

# Store data
s_157.to_csv(data_sources_staged_raw / "S-157_staged_raw.csv", sep=";")

# # # # # # # Economist intelligence unit # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-11, S-120, S-124, S-134 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

### Economist data
S_11_S120_s_124_s_134 = pd.read_excel(
    data_in
    / "data_raw_manually_extracted"
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
    S_149_150_151 = pd.read_excel(
        "http://ihl-databases.icrc.org/applic/ihl/ihl.nsf/xsp/.ibmmodres/domino/OpenAttachment/applic/ihl/ihl.nsf/40BAD58D71673B1CC125861400334BC4/%24File/IHL_and_other_related_Treaties.xls?Open",
        sheet_name="IHL and other related Treaties",
        header=1,
    )
except:
    # Load from local file if endpoint is donw
    S_149_150_151 = pd.read_excel(
        data_in
        / "data_raw_manually_extracted"
        / "S-149, S-150, S-151-IHL_and_other_related_Treaties.xls",
        sheet_name="IHL and other related Treaties",
        header=1,
    )

    # Log
    print(
        "Data for sources S-149, S-150 and S-151 could not be extracted URL endpoint. Loaded data from local repository."
    )


# Create list to loop through
icrc_list = [
    ["S-149_staged_raw.csv", "GC I-IV 1949"],
    ["S-150_staged_raw.csv", "AP I 1977"],
    ["S-151_staged_raw.csv", "AP II 1977"],
]


# Loop through list
for element in icrc_list:
    # Extract right columns
    dataframe = S_149_150_151[["Country", element[1]]]

    # Rename clumns
    dataframe = dataframe.rename(columns={element[1]: "ATTR_RATIFICATION_DATE"})

    # Add year column
    dataframe["TIME_PERIOD"] = 2020

    # Save data
    dataframe.to_csv(data_sources_staged_raw / element[0], sep=";")

    # These indicators will probably require special attention/ a disting sort of pipeline
    # The below code works just fine, but still need to include the savings part --> Where to put this loop bit?
    """ 
    # Cleansing
    dataframe = cleanse.Cleanser().rename_and_discard_columns(
        raw_data=dataframe,
        mapping_dictionary=mapping_dict,
        final_sdmx_col_list=sdmx_df_columns_all
    )

    dataframe = cleanse.Cleanser().add_and_discard_countries(
        grouped_data=dataframe,
        crba_country_list=country_crba_list,
        country_list_full = country_full_list
    )

    dataframe_cleansed = cleanse.Cleanser().encode_ilo_un_treaty_data(
        dataframe = dataframe,
        treaty_source_body = "UN Treaties"
    )

    cleanse.Cleanser().create_log_report(
        cleansed_data=dataframe_cleansed
    )
    
    # Normalizing section
    dataframe_normalized = scaler.normalizer(
        cleansed_data = dataframe_cleansed,
        sql_subset_query_string=row["DIMENSION_VALUES_NORMALIZATION"],
        # dim_cols=sdmx_df_columns_dims,
        variable_type = row["VALUE_LABELS"],
        is_inverted = row["INVERT_NORMALIZATION"],
        whisker_factor=1.5,
        raw_data_col="RAW_OBS_VALUE",
        scaled_data_col_name="SCALED_OBS_VALUE",
        maximum_score=10,
        )
    """

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
        data_in
        / "data_raw_manually_extracted"
        / "S-131, S-193-access_to_justice_data.xls",
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

    # Save data
    dataframe.to_csv(data_sources_staged_raw / element[0], sep=";")


# # # # # # # FCTC data # # # # # # # # # # # # # # # # # # # #
# # # # # # # S-89 # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # #  # # # # # # # # # # # # # # # # # # # #

# Load from local file if endpoint is donw
S_89 = pd.read_excel(
    data_in / "data_raw_manually_extracted" / "S-89 Answers_v2.xlsx",
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
    data_in
    / "data_raw_manually_extracted"
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

# save data
S_167.to_csv(data_sources_staged_raw / "S-167_staged_raw.csv", sep=";")