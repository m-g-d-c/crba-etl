import numpy as np
import pandas as pd

"""
This file contains the list of values to map values to each other
during when using the Cleanser class. 



"""
value_mapper = {
    "DIM_SEX": {
        "BOTH_SEXES": ["_T", "BOTHSEX", "BTSX", "SEX_T"],
        "MALE": ["M", "MALE", "MLE", "SEX_M"],
        "FEMALE": ["F", "FEMALE", "FMLE", "SEX_F"],
    },
    "DIM_EDU_LEVEL": {
        "LOWER SECONDARY EDUCATION": ["L2"],
        "UPPER SECONDARY EDUCATION": ["L3"],
        "PRE-PRIMARY EDUCATION": ["L02"],
    },
    "DIM_AGE": {
        "SCHOOL_AGE_POPULATION": ["SCH_AGE_GROUP"],
    },
    "DIM_AGE_GROUP": {
        "_T": ["_T"],
        "10-19 YEARS": ["YEARS10-19"],
        "5-19 YEARS": ["YEARS05-19"],
        "5-09 YEARS": ["YEARS05-19"],
        "5-17 YEARS": ["5-17"],
        "5-14 YEARS": ["5-14"],
        "18-29 YEARS": ["18-29"],
        "18-74 YEARS": ["18-74"],
        "ALL AGES": ["ALLAGE"],
    },
    "DIM_MANAGEMENT_LEVEL": {
        "OCU_MGMT_SENIOR": ["OCU_MGMT_SENIOR"],
        "OCU_MGMT_TOTAL": ["OCU_MGMT_TOTAL"],
    },
    "DIM_AREA_TYPE": {"TOTAL": ["TOTL"], "RURAL": ["RUR"], "URBAN": ["URB"]},
    # "DIM_QUANTILE" : {},
    # "DIM_SDG_GOAL" : {},
    # "DIM_OCU_TYPE" : {},
    # "DIM_REP_TYPE" : {},
    "DIM_SECTOR": {
        "NO BREAKDOWN": ["TOTAL"],
        "Agriculture, forestry and fishing": ["ISIC4_A"],
        "Non-agriculture": ["NONAGR"],
    },
}

"""
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
]






- - - - - 


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

"""
