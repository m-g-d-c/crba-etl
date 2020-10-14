import pathlib
import json


""" 
This file contains the following things:

* Column mapping
    --> Be able to relate columns from different data source together, which 
    contain the same data but have different names (e.g. "GENDER" vs"SEX")
* Specify which columns in a dataset are:
    * OBS_VALUE
    * YEAR
    * COUNTRY
    * DIMENSIONS
    * ATTRIBUTES
    * to be thrown away
* Contain target dimension-subgroup for indicator score
"""

"""
sdmx_target_structure = {
    "TMEE": [
        {"id": "Dataflow", "type": "string"},
        {"id": "REF_AREA", "type": "enum", "role": "dim"},
        # {"id": "UNICEF_INDICATOR", "type": "string", "role": "dim"},
        {"id": "SEX", "type": "string", "role": "dim"},
        # {"id": "AGE", "type": "string", "role": "dim"},
        # {"id": "RESIDENCE", "type": "string", "role": "dim"},
        # {"id": "WEALTH_QUINTILE", "type": "string", "role": "dim"},
        {"id": "TIME_PERIOD", "type": "string", "role": "time"},
        {"id": "OBS_VALUE", "type": "string"},
        # {"id": "UNIT_MEASURE", "type": "string"},
        # {"id": "OBS_FOOTNOTE", "type": "string"},
        # {"id": "FREQ", "type": "string"},
        # {"id": "DATA_SOURCE", "type": "string"},
        # {"id": "UNIT_MULTIPLIER", "type": "string"},
        # {"id": "OBS_STATUS", "type": "string"},
        #
        {"id" : "EDU_LEVEL", "type" : "string", "role" : "dim"} 
    ]
}


# Mapping tables for data sources
unesco_mapping = {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},

        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "EDU_LEVEL" : {"type": "col", "role": "attrib", "value": "EDU_LEVEL"}
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
}

# Overview of columns in SDMX final dataframe

* Country
    * REF_AREA
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
    * DIME_AREA_TYPE
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

Some columns that might be interesting, but which I haven't included: 

* Worldbank income group
* REGION
* Indicator code/ dataflow in the source API
* Wealth quantile

"""

# # # #  Define tuples for column mapping
# # # # # # # # # # # # #
# # # # Country # # # #
# # # # # # # # # # # # #
# Country name
country_tuple = ("geoAreaName",)
country_mapper = {key: "REF_AREA" for key in country_tuple}

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
dim_edu_level_tuple = ("DIM_EDU_LEVEL",)
dim_edu_level_mapper = {key: "DIM_EDU_LEVEL" for key in dim_edu_level_tuple}

# Age
dim_age_tuple = ("AGE", "dimensions.Age")
dim_age_mapper = {key: "DIM_AGE" for key in dim_age_tuple}

# Age group
dim_age_group_tuple = ("AGEGROUP",)
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
sdmx_df_columns_country = ["REF_AREA", "COUNTRY_ISO_2", "COUNTRY_ISO_3"]

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
    "DIME_AREA_TYPE",
    "DIM_QUANTILE",
    "DIM_SDG_GOAL",
    "DIM_OCU_TYPE",
    "DIM_REP_TYPE",
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
