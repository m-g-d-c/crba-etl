import pathlib
import json


""" 
This file contains the dictionaries to map column names of different data sources. 

It defines the dictionaries to rename columns in raw data and
also classifies each of the renamed columns into one of the five column types: 

    * RAW_OBS_VALUE (actual observation value)
    * TIME_PERIOD (column(s) indicating time)
    * COUNTRY (columns indicating the country, iso2 or iso3 code)
    * DIMENSIONS (dimension columns)
    * ATTRIBUTES (attriute columns)

The mapping dictionaries should be used with the Cleanser class 
to rename column names in raw data, so that 
after the renaming (and other cleansing steps), the cleansed dataframe is a 
projection (i.e. a dataframe with equally or less columns) of 
the final SDMX dataframe. The final SDMX dataframe has the following columns: 


# Overview of columns in SDMX final dataframe
* Country
    * COUNTRY_NAME
    * COUNTRY_ISO_2 
    * COUNTRY_ISO_3 
* year
    * TIME_PERIOD
* obs_value
    * RAW_OBS_VALUE
* Dim:
    * DIM_SEX
    * DIM_EDU_LEVEL level
    * DIM_AGE
    * DIM_AGE_GROUP
    * DIM_MANAGEMENT_LEVEL
    * DIM_AREA_TYPE
    * DIM_QUANTILE
    * DIM_SDG_GOAL
    * DIM_OCU_TYPE
    * DIM_REP_TYPE
* Attributes:
    * ATTR_SOURCE_NOTE
    * ATTR_SOURCE_INDICATOR_NOTE
    * ATTR_UNIT_MEASURE
    * ATTR_SOURCE_OBS_STATUS
    * ATTR_SOURCE_COMMENTS
    * ATTR_SDG_INDICATOR
    * ATTR_SDG_INDICATOR_DESCRIPTION
    * ATTR_SOURCE_OF_SOURCE
    * ATTR_FOOTNOTE_OF_SOURCE
    * ATTR_SOURCE_OBS_TYPE

The above definition is the result of looking at all indicators and seeing what
dimensions and attributes are present and relevant. Some of the attributes
 and dimensions (if they only had one value) were not deemed relevant and
 exluded from the final SDMX dataframe structure. These are:

* Worldbank income group
* REGION
* Indicator code/ dataflow in the source API
* Wealth quantile

Q: What if I want to add an indicator to the CRBA, which has a new dimension/ attribute 
or if this dimension/ attribute is called different in that dataframe?

A: If an attribute or dimension is present and relevant in at least one dataframe, 
it should be added to the respective tuple (or you have to define a new tuple + 
dictionary), and you have to add it to the variable "sdmx_df_columns_*", and 
then rerun the main file. 
"""

# # # #  Define tuples for column mapping
# # # # # # # # # # # # #
# # # # Country # # # #
# # # # # # # # # # # # #
# Country name
country_tuple = ("geoAreaName",)
country_mapper = {key: "COUNTRY_NAME" for key in country_tuple}

# Iso 2
iso_2_tuple = ("REF_AREA",)
iso_2_mapper = {key: "COUNTRY_ISO_2" for key in iso_2_tuple}

# Iso 3
iso_3_tuple = ("REF_AREA", "COUNTRY")
iso_3_mapper = {key: "COUNTRY_ISO_3" for key in iso_3_tuple}

# # # # # # # # # # # # #
# # # # Year # # # #
# # # # # # # # # # # # #
# Year column
year_tuple = ("TIME_PERIOD", "YEAR", "timePeriod", "timePeriodStart")
year_mapper = {key: "TIME_PERIOD" for key in year_tuple}

# # # # # # # # # # # # #
# # # # Observation value # # # #
# # # # # # # # # # # # #

# Observation value
obs_value_tuple = (
    "OBS_VALUE",
    # "Numeric", How to deal with this?
    "Display Value",
    "value",
)
obs_value_mapper = {key: "RAW_OBS_VALUE" for key in obs_value_tuple}

# # # # # # # # # # # # #
# # # # Dimensions # # # #
# # # # # # # # # # # # #

# Gender
dim_sex_tuple = ("SEX", "dimensions.Sex")
dim_sex_mapper = {key: "DIM_SEX" for key in dim_sex_tuple}

# Education level
dim_edu_level_tuple = ("EDU_LEVEL",)
dim_edu_level_mapper = {key: "DIM_EDU_LEVEL" for key in dim_edu_level_tuple}

# Age
dim_age_tuple = ("AGE",)
dim_age_mapper = {key: "DIM_AGE" for key in dim_age_tuple}

# Age group
dim_age_group_tuple = (
    "AGEGROUP",
    "dimensions.Age",
)
dim_age_group_mapper = {key: "DIM_AGE_GROUP" for key in dim_age_group_tuple}

# Management level
dim_management_level_tuple = ("OCU",)
dim_management_level_mapper = {
    key: "DIM_MANAGEMENT_LEVEL" for key in dim_management_level_tuple
}

# Area type (rural vs urban)
dim_area_type_tuple = ("RESIDENCEAREATYPE",)
dim_area_type_mapper = {key: "DIM_AREA_TYPE" for key in dim_area_type_tuple}

# Quantile
dim_quantile_tuple = ("dimensions.Quantile",)
dim_quantile_mapper = {key: "DIM_QUANTILE" for key in dim_quantile_tuple}

# SDG Goal --> Indicators S-183, S-184 and S-185 have the SDG indicator as dimension,
# This is because in the SDG api the indicators is mapped to three SDG goals
# The actual values in these datasets are simply duplicates, so any of them can be taken
dim_sdg_goal_tuple = ("goal",)
dim_sdg_goal_mapper = {key: "DIM_SDG_GOAL" for key in dim_sdg_goal_tuple}

# Type of occupation
dim_ocu_type_tuple = ("dimensions.Type of occupation",)
dim_ocu_type_mapper = {key: "DIM_OCU_TYPE" for key in dim_ocu_type_tuple}

# Reporting type
dim_reporting_type_tuple = ("dimensions.Reporting Type",)
dim_reporting_type_mapper = {key: "DIM_REP_TYPE" for key in dim_reporting_type_tuple}

# Activity (sector)
dim_sector_tuple = ("dimensions.Activity",)
dim_sector_mapper = {key: "DIM_SECTOR" for key in dim_sector_tuple}


# # # # # # # # # # # # #
# # # # Attributes # # # #
# # # # # # # # # # # # #

# Source note
source_note_tuple = ("SOURCE_NOTE",)
source_mapper = {key: "ATTR_SOURCE_NOTE" for key in source_note_tuple}

# Indicator note from source
source_indicator_note_tuple = ("INDICATOR_NOTE",)
source_indicator_mapper = {
    key: "ATTR_SOURCE_INDICATOR_NOTE" for key in source_indicator_note_tuple
}

# Unit measure
unit_measure_tuple = ("UNIT_MEASURE", "attributes.Units")
unit_measure_mapper = {key: "ATTR_UNIT_MEASURE" for key in unit_measure_tuple}

# Observation status
obs_status_tuple = ("OBS_STATUS",)
obs_status_mapper = {key: "ATTR_SOURCE_OBS_STATUS" for key in obs_status_tuple}

# Source comments
source_comments_tuple = ("Comments",)
source_comments_mapper = {key: "ATTR_SOURCE_COMMENTS" for key in source_comments_tuple}

# SDG indicator
sdg_indicator_tuple = ("indictor",)
sdg_indicator_mapper = {key: "ATTR_SDG_INDICATOR" for key in sdg_indicator_tuple}

# SDG indicator description
sdg_indicator_desc_tuple = ("seriesDescription",)
sdg_indicator_desc_mapper = {
    key: "ATTR_SDG_INDICATOR_DESCRIPTION" for key in sdg_indicator_desc_tuple
}

# Source of source
source_of_source_tuple = ("source",)
source_of_source_mapper = {
    key: "ATTR_SOURCE_OF_SOURCE" for key in source_of_source_tuple
}

# Source of source footnotes
footnotes_of_source_tuple = ("footnotes",)
footnotes_of_source_mapper = {
    key: "ATTR_FOOTNOTE_OF_SOURCE" for key in footnotes_of_source_tuple
}

# Observation type
obs_type_tuple = ("attribute.Nature",)
obs_type_mapper = {key: "ATTR_SOURCE_OBS_TYPE" for key in obs_type_tuple}


# Create list of all mapper dictionaries
mapper_tuple_list = [
    country_mapper,
    iso_2_mapper,
    iso_3_mapper,
    year_mapper,
    obs_value_mapper,
    dim_sex_mapper,
    obs_value_mapper,
    dim_sex_mapper,
    dim_edu_level_mapper,
    dim_age_mapper,
    dim_age_group_mapper,
    dim_management_level_mapper,
    dim_area_type_mapper,
    dim_quantile_mapper,
    dim_sdg_goal_mapper,
    dim_ocu_type_mapper,
    dim_reporting_type_mapper,
    dim_sector_mapper,
    source_mapper,
    source_indicator_mapper,
    unit_measure_mapper,
    obs_status_mapper,
    source_comments_mapper,
    sdg_indicator_mapper,
    sdg_indicator_desc_mapper,
    source_of_source_mapper,
    footnotes_of_source_mapper,
    obs_type_mapper,
]

# Define the mapping dictionary
mapping_dict = {}

for mapper_tuple in mapper_tuple_list:
    mapping_dict.update(mapper_tuple)

with open("mapping_dict.json", "w") as fp:
    json.dump(mapping_dict, fp)


# Define list of columns in the final dataframe
# Country columns
sdmx_df_columns_country = ["COUNTRY_NAME", "COUNTRY_ISO_2", "COUNTRY_ISO_3"]

# Time
sdmx_df_columns_time = ["TIME_PERIOD"]

# Observation value
sdmx_df_columns_obs = ["RAW_OBS_VALUE"]

# Dimensions
sdmx_df_columns_dims = [
    "DIM_SEX",
    "DIM_EDU_LEVEL",
    "DIM_AGE",
    "DIM_AGE_GROUP",
    "DIM_MANAGEMENT_LEVEL",
    "DIM_AREA_TYPE",
    "DIM_QUANTILE",
    "DIM_SDG_GOAL",
    "DIM_OCU_TYPE",
    "DIM_REP_TYPE",
    "DIM_SECTOR",
]
# Attributes
sdmx_df_columns_attr = [
    "ATTR_SOURCE_NOTE",
    "ATTR_SOURCE_INDICATOR_NOTE",
    "ATTR_UNIT_MEASURE",
    "ATTR_SOURCE_OBS_STATUS",
    "ATTR_SOURCE_COMMENTS",
    "ATTR_SDG_INDICATOR",
    "ATTR_SDG_INDICATOR_DESCRIPTION",
    "ATTR_SOURCE_OF_SOURCE",
    "ATTR_FOOTNOTE_OF_SOURCE",
    "ATTR_SOURCE_OBS_TYPE",
]

# All columns list
sdmx_df_columns_all = (
    sdmx_df_columns_country
    + sdmx_df_columns_time
    + sdmx_df_columns_obs
    + sdmx_df_columns_dims
    + sdmx_df_columns_attr
)
