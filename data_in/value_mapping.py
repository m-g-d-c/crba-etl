import numpy as np
import pandas as pd

"""
This file contains the list of values to map values to each other
during when using the Cleanser class. 

It defines one large, nested dictionary called value_mapper. 

This dictionary contains as keys the columns of the final SDMX data structure. 
The values of those keys are dictionaries. 

These dictionaries contain as key the target value (i.e. the value 
that a the column is supposed to be able to take). The value of 
the keys is a list containing all values in the raw dataframes
which are supposed to be mapped into that value. 

The schema of the dictionar is thus: 

value_mapper = {
    <column name in final sdmx df> : {
        <target value> : <list of values to be converted into the target value> 
    }
}

This is a list of the columns in the final SDMX dataframe: 

* "DIM_SEX",
* "DIM_EDU_LEVEL",
* "DIM_AGE",
* "DIM_AGE_GROUP",
* "DIM_MANAGEMENT_LEVEL",
* "DIM_AREA_TYPE",
* "DIM_QUANTILE",
* "DIM_SDG_GOAL",
* "DIM_OCU_TYPE",
* "DIM_REP_TYPE",
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
        "_T": [""],
    },
    "DIM_AGE": {
        "_T": ["_T"],
        "SCHOOL_AGE_POPULATION": ["SCH_AGE_GROUP"],
    },
    "DIM_AGE_GROUP": {
        "_T": ["_T"],
        "10-19 YEARS": ["YEARS10-19"],
        "5-19 YEARS": ["YEARS05-19"],
        "5-09 YEARS": ["YEARS05-09"],
        "5-17 YEARS": ["5-17"],
        "5-14 YEARS": ["5-14"],
        "7-17 YEARS": ["7-17"],
        "6-17 YEARS": ["6-17"],
        "10-17 YEARS": ["10-17"],
        "18-29 YEARS": ["18-29"],
        "18-74 YEARS": ["18-74"],
        "ALL AGES": ["ALLAGE"],
    },
    "DIM_MANAGEMENT_LEVEL": {
        "OCU_MGMT_SENIOR": ["OCU_MGMT_SENIOR"],
        "OCU_MGMT_TOTAL": ["OCU_MGMT_TOTAL"],
        "_T": [""],
    },
    "DIM_AREA_TYPE": {
        "TOTAL": ["TOTL"],
        "RURAL": ["RUR"],
        "URBAN": ["URB"],
        "_T": [""],
    },
    # "DIM_QUANTILE" : {},
    "DIM_SDG_INDICATOR": {
        "1.5.4": ["['1.5.4']"],
        "1.5.1": ["['1.5.1']"],
        "11.5.2": ["['11.5.2']"],
    },
    # "DIM_OCU_TYPE" : {},
    # "DIM_REP_TYPE" : {},
    "DIM_SECTOR": {
        "NO BREAKDOWN": ["TOTAL"],
        "Agriculture, forestry and fishing": ["ISIC4_A"],
        "Non-agriculture": ["NONAGR"],
        "_T": [""],
    },
}