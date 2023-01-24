"""
Module 


"""
import argparse
import csv
import importlib
import io
import logging
import re
import uuid
import warnings
from pathlib import Path
from typing import Type

import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint.types.checkpoint_result import \
    CheckpointResult
from great_expectations.core.batch import RuntimeBatchRequest
from tqdm.autonotebook import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from crba_project.conf import Config
from crba_project.extractor import ExtractionError

log = logging.getLogger(__name__)

def dynamic_load(class_path) -> Type:
    mod = ".".join(class_path.split(".")[:-1])
    _class = class_path.split(".")[-1]
    module = importlib.import_module(mod)
    return getattr(module, _class)


def build_combined_normalized_csv(config):
    ##TODO Parallelisiere . Prozess vs Thread. Probably Thread because of heavy IO Tasks concurrent.futures
    extractions_data = []
    extraction_errors_source_ids =[]

    validation_batches = []

    stats = {}

    with logging_redirect_tqdm():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore") ## TODO:Store Warnings istead of jus upressing them
            for index, row in tqdm(list(config.source_config.iterrows()), dynamic_ncols=True):
                try:
                    extractor = dynamic_load(row["EXTRACTOR_CLASS"])(config,**row)
                    df = extractor.get()
                    buf = io.StringIO()
                    df.info(buf=buf)
                    stats[row["SOURCE_ID"]] = {"stats":buf.getvalue()}
                    # More IMportant then simple INFO logs but less importend the download infos
                    log.log(
                        level=25,
                        msg=f"Source {row['SOURCE_ID']} extract with {df.shape if df is not None else 0}::: {extractor.__class__.__name__}",
                    )
                    extractions_data.append(df)

                    validation_batches.append(
                        {"batch_request": 
                            RuntimeBatchRequest(
                            datasource_name="default_datasource",
                            data_connector_name="default_runtime_data_connector",
                            data_asset_name=row["SOURCE_ID"],  # This can be anything that identifies this data_asset for you
                            runtime_parameters={"batch_data": df},  # df is your dataframe
                            batch_identifiers={"default_identifier_name": "default_identifier"},
                        )
                        }
                    )

                except ExtractionError as ex:
                    extraction_errors_source_ids.append(row["SOURCE_ID"])
                    stats[row["SOURCE_ID"]] = {"error":str(ex)}
                    log.warning(
                        f"{str(ex)}", exc_info=True
                    )
    # run GX validation
    try:
        result: CheckpointResult = config.ge_context.run_checkpoint(
            checkpoint_name="indicator_sdmx_checkpoint",
            validations=validation_batches,
            run_name=f"{config.run_id}",
        )
        if not result["success"]:
            log.warn("Validation failed!")
    except gx.exceptions.exceptions.CheckpointError as ex:
        log.warn(f"Checkint validation Failed by Exception:{ex}",exc_info=ex)

    return pd.concat(extractions_data, axis=0, ignore_index=True) ,extraction_errors_source_ids,stats

def aggregate_combined_normalized_csv(config,combined_normalized_csv):
   # Idenify all dimension columns in combined dataframe


    available_dim_cols = []
    for col in combined_normalized_csv.columns:
        dim_col = re.findall("DIM_.+", col)
        # print(dim_col)
        if len(dim_col) == 1:
            available_dim_cols += dim_col

    # Fill _T for all NA values of dimension columns
    combined_normalized_csv[available_dim_cols] = combined_normalized_csv[
        available_dim_cols
    ].fillna(value="_T")

    # Double check if there are duplicate countries
    print("This is the number of duplicate rows:")
    print(sum(combined_normalized_csv.duplicated()))
    #print(combined_normalized_csv.loc[combined_normalized_csv.duplicated(), ['COUNTRY_ISO_3','INDICATOR_NAME', 'INDICATOR_CODE']])
    #combined_normalized_csv = combined_normalized_csv.drop_duplicates() # uncomment if want to delete duplicates (but check where they come from first)

    # Check that all indicators have been processed
    # TODO Probably will allways fail as long there are any error with the sources
    # assert config.source_config.shape[0] == len(build_combined_normalized_csv.INDICATOR_CODE.unique())

    # Create category score
    aggregated_scores_dataset = combined_normalized_csv.loc[
        combined_normalized_csv['COUNTRY_ISO_3'] != 'XKX', 
        [
            'COUNTRY_ISO_3',
            'SCALED_OBS_VALUE',
            'INDICATOR_INDEX',
            'INDICATOR_ISSUE',
            'INDICATOR_CATEGORY'
        ]
    ].groupby(
        by = [
            'COUNTRY_ISO_3',
            'INDICATOR_CATEGORY', 
            'INDICATOR_ISSUE',
            'INDICATOR_INDEX'
        ],
        as_index = False
    ).mean().rename(
        columns={
            'SCALED_OBS_VALUE' : 'CATEGORY_ISSUE_SCORE'
        }
    )

    # # # # Introduce weighting: duplicate all index_issues who belong to category outcome or enforcement
    aggregated_scores_dataset = aggregated_scores_dataset.append(
        aggregated_scores_dataset.loc[
            (aggregated_scores_dataset['INDICATOR_CATEGORY'] == 'Outcome') |
            (aggregated_scores_dataset['INDICATOR_CATEGORY'] == 'Enforcement')
        ]
    )

    # # # # # # # Issue score
    temp = aggregated_scores_dataset.groupby(
        by = [
            'COUNTRY_ISO_3',
            'INDICATOR_ISSUE',
            'INDICATOR_INDEX'
        ],
        as_index = False
    ).mean().rename(
        columns={
            'CATEGORY_ISSUE_SCORE' : 'ISSUE_INDEX_SCORE'
        }
    )

    # Drop duplicates again
    temp = temp.drop_duplicates()

    # # Add risk category
    # Define list of percentiles
    percentile_33 = temp[
        'ISSUE_INDEX_SCORE'
    ].quantile(
        0.333
    )

    percentile_66 = temp[
        'ISSUE_INDEX_SCORE'
    ].quantile(
        0.667
    )

    # Add column indicating risk category
    temp.loc[
        temp['ISSUE_INDEX_SCORE'] < percentile_33,
        'ISSUE_INDEX_RISK_CATEGORY'
        ] = 'High risk' 

    temp.loc[
        temp['ISSUE_INDEX_SCORE'] > percentile_66,
        'ISSUE_INDEX_RISK_CATEGORY'
        ] = 'Low risk'

    temp.loc[
        (temp['ISSUE_INDEX_SCORE'] > percentile_33) & 
        (temp['ISSUE_INDEX_SCORE'] < percentile_66),
        'ISSUE_INDEX_RISK_CATEGORY'
        ] = 'Medium risk'



    # # # # # #  Index score
    temp_2 = temp.groupby(
        by = [
            'COUNTRY_ISO_3',
            'INDICATOR_INDEX'
        ],
        as_index = False
    ).mean().rename(
        columns={
            'ISSUE_INDEX_SCORE' : 'INDEX_SCORE'
        }
    )

    # # Add risk category
    # Define list of percentiles
    percentile_33 = temp_2[
        'INDEX_SCORE'
    ].quantile(
        0.333
    )

    percentile_66 = temp_2[
        'INDEX_SCORE'
    ].quantile(
        0.667
    )

    # Add column indicating risk category
    temp_2.loc[
        temp_2['INDEX_SCORE'] < percentile_33,
        'INDEX_RISK_CATEGORY'
        ] = 'High risk' 

    temp_2.loc[
        temp_2['INDEX_SCORE'] > percentile_66,
        'INDEX_RISK_CATEGORY'
        ] = 'Low risk'

    temp_2.loc[
        (temp_2['INDEX_SCORE'] > percentile_33) & 
        (temp_2['INDEX_SCORE'] < percentile_66),
        'INDEX_RISK_CATEGORY'
        ] = 'Medium risk'

    # # # # # Overall score
    temp_3 = temp_2.groupby(
        by = [
            'COUNTRY_ISO_3',
        ],
        as_index = False
    ).mean().rename(
        columns={
            'INDEX_SCORE' : 'OVERALL_SCORE'
        }
    )

    # Join all aggregated score together
    aggregated_scores_dataset = aggregated_scores_dataset.merge(
        right = temp,
        on = [
            'COUNTRY_ISO_3',
            'INDICATOR_ISSUE',
            'INDICATOR_INDEX',
        ]
    ).merge(
        right = temp_2,
        on = [
            'COUNTRY_ISO_3',
            'INDICATOR_INDEX'
        ]
    ).merge(
        right = temp_3,
        on = [
            'COUNTRY_ISO_3',
        ]
    ).merge(
        right = config.country_crba_list,
        on = 'COUNTRY_ISO_3'
    ).drop(
        [
            'COUNTRY_NAME',
            'COUNTRY_ISO_2'
        ],
        axis=1
    )

    crba_final = combined_normalized_csv.merge(
        right=aggregated_scores_dataset,
        on=[
            'COUNTRY_ISO_3',
            'INDICATOR_CATEGORY',
            'INDICATOR_ISSUE',
            'INDICATOR_INDEX'
        ],
        how='left' 
    )


    # Did not join on entire composite key on left, must drop dupliates
    crba_final = crba_final.drop_duplicates()

    # Export combined cleansed dataframe as a sample
    crba_final.to_csv(
        path_or_buf = config.output_dir / config.run_id  / 'crba_final.csv',
        sep = ";",
        index=False
    )
        
    aggregated_scores_dataset.to_csv(
        path_or_buf = config.output_dir / config.run_id / 'aggregated_scores.csv',
        sep = ";",
        quoting=csv.QUOTE_ALL
    )


    return crba_final,aggregated_scores_dataset 

def generate_stats(config,final_crba):
    config.source_config

def run(config):
    combined_normalized_csv, extraction_errors_source_ids,stats = build_combined_normalized_csv(config)

    combined_normalized_csv.to_csv(
        path_or_buf = config.output_dir / config.run_id / 'combined_normalized.csv',
        sep = ";",
        quoting=csv.QUOTE_ALL
    )

    print(combined_normalized_csv.info())

    crba_final,aggregated_scores_dataset = aggregate_combined_normalized_csv(config,combined_normalized_csv)
