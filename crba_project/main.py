import argparse
from dataclasses import dataclass
import inspect
from typing import Type
import uuid
from pathlib import Path
import logging
import pandas as pd
from conf import Config
import importlib
from crba_project.extractor import ExtractionError
from crba_project.extractor.csv import DefaultCSVExtractor
from utils import utils
from multiprocessing.pool import ThreadPool
from tqdm import tqdm
import traceback
import sys
import os

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.ERROR)
root_error_handler = logging.FileHandler("{0}/{1}".format("logs", "error"))
root_logger = logging.getLogger()
root_logger.setLevel(logging.WARNING)
root_logger.addHandler(root_error_handler)


log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)



def dynamic_load(class_path) -> Type:
    mod = ".".join(class_path.split(".")[:-1])
    _class = class_path.split(".")[-1]
    module = importlib.import_module(mod)
    return getattr(module, _class)

def resource_path(relative_path: str) -> str:
    try:
        
        base_path = sys._MEIPASS

    except Exception:
        base_path = os.path.dirname(__file__)

    return os.path.join(base_path, relative_path)

def build_source_config(config: Config):
    """

    Build dataframe which hold all needed information about sources
    This dataframe is the foundation on which the sources get pulled. 

    Put more logic in the input sheets. And less in Code.!??!?!
    """
    # sources sheet
    crba_data_dictionary_source = pd.read_excel(
        config.input_dir / "indicator_dictionary_CRBA.xlsx",
        sheet_name="Source",
        keep_default_na=False,
    )
    # Delete sources that are deprecated
    crba_data_dictionary_source = crba_data_dictionary_source[
        crba_data_dictionary_source.STATUS != "Deleted"
    ]

    # indicator sheet
    crba_data_dictionary_indicator = pd.read_excel(
        config.input_dir / "indicator_dictionary_CRBA.xlsx",
        sheet_name="Indicator",
        keep_default_na=False,
    )
    # Delete indicators that are deprecated
    crba_data_dictionary_indicator = crba_data_dictionary_indicator[
        crba_data_dictionary_indicator.STATUS != "Deleted"
    ]

    # snapshot sheet. Link between Indicator and Source
    crba_data_dictionary_snapshot = pd.read_excel(
        config.input_dir / "indicator_dictionary_CRBA.xlsx",
        sheet_name="Snapshot",
        keep_default_na=False,
    )
    # Delete snapshots which aren't used in 2020
    crba_data_dictionary_snapshot = crba_data_dictionary_snapshot[
        crba_data_dictionary_snapshot.YEAR_USED == 2020
    ]
    # Pre Edit the excel sheets to be clean
    #    # Pandas also reads rows with no content (empty strings)
    #    crba_data_dictionary_indicator = crba_data_dictionary_indicator.loc[
    #        crba_data_dictionary_indicator.INDICATOR_NAME != "", :
    #    ]

    # Input lists
    crba_data_dictionary_input_list = pd.read_excel(
        config.input_dir / "indicator_dictionary_CRBA.xlsx",
        sheet_name="Input_Lists",
        keep_default_na=False,
    )

    # Add 2-digit shortcodes of index, issue and category to indicators sheet
    crba_data_dictionary_indicator = (
        crba_data_dictionary_indicator.merge(
            right=crba_data_dictionary_input_list[["INDEX", "INDEX_CODE"]],
            left_on="INDEX",
            right_on="INDEX",
        )
        .merge(
            right=crba_data_dictionary_input_list[["ISSUE", "ISSUE_CODE"]],
            left_on="ISSUE",
            right_on="ISSUE",
        )
        .merge(
            right=crba_data_dictionary_input_list[["CATEGORY", "CATEGORY_CODE"]],
            left_on="CATEGORY",
            right_on="CATEGORY",
        )
    )

    # Create indicator code prefix (INDEX-ISSUE_CAEGORY CODE)
    crba_data_dictionary_indicator = crba_data_dictionary_indicator.assign(
        INDICATOR_CODE_PREFIX=crba_data_dictionary_indicator.INDEX_CODE
        + "_"
        + crba_data_dictionary_indicator.ISSUE_CODE
        + "_"
        + crba_data_dictionary_indicator.CATEGORY_CODE
        + "_"
    )

    # Create indicator code
    crba_data_dictionary_indicator = crba_data_dictionary_indicator.assign(
        INDICATOR_CODE=crba_data_dictionary_indicator.INDICATOR_CODE_PREFIX
        + crba_data_dictionary_indicator.INDICATOR_NAME.apply(
            lambda x: utils.create_ind_code(x)
        )
    )

    # Check if there are indicators which have been assigned the same indicator code:
    duplicate_codes = crba_data_dictionary_indicator[
        crba_data_dictionary_indicator.duplicated(subset="INDICATOR_CODE", keep=False)
    ][["INDICATOR_CODE", "INDICATOR_ID"]]

    if len(duplicate_codes) != 0:
        raise Exception(
            f"WARNING: Theese are indicator names that have been assigned with the same indicator code {duplicate_codes} \n Please change the names to avoid duplicates"
        )
    else:
        print("No duplicate indicator codes present. You can proceed.")

    source_config = crba_data_dictionary_source.merge(
        right=crba_data_dictionary_snapshot, on="SOURCE_ID"
    ).merge(right=crba_data_dictionary_indicator, on="INDICATOR_ID")
    log.info(f"The Source Config Loaded sucessfully. Numer of Sources{source_config.shape[0]}")
    return source_config


if __name__ == "__main__":
    # Initialize parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o-dir", "--OutputDir", help="output_directory", default="data_out"
    )
    parser.add_argument(
        "-i-dir", "--InputDir", help="Directory with input files", default="data_in"
    )
    parser.add_argument(
        "-f", "--Filter", help="Expect a CSV File where the first col is the Source ID. The error.csv can be used", required=False,
    )

    args = parser.parse_args()

    run_id = str(uuid.uuid4())

    print(f"Run with run_id {run_id}")
    conf = Config(Path(args.OutputDir), Path(args.InputDir),run_id)

    source_config = build_source_config(conf)
    print(source_config.shape)
    print(source_config.head())
    
    #Filter Source Config
    if args.Filter:
        filter_df = pd.read_csv(args.Filter,index_col=0)
        print(f"Filter {filter_df.shape}")
        source_config = source_config.merge(filter_df,how='inner',left_on="SOURCE_ID",right_index=True)
        print(source_config.shape)


    print(f"Number Sources:{source_config.shape}")
    ##TODO Parallelisiere . Prozess vs Thread. Probably Thread because of heavy IO Tasks concurrent.futures
    extractions =[]
    error_sources = {}
    for index, row in tqdm(list(source_config.iterrows())):
        try:
            extractor =  dynamic_load(row["EXTRACTOR_CLASS"])(config=conf, **row)
            df = extractor.extract()
            print(f"Source {row['SOURCE_ID']} extract with {df.shape if df is not None else 0}::: {extractor.__class__.__name__}")
            extractions.append(df)
        except ExtractionError as ex:
            log.warning(f"The Extraction of {row['SOURCE_ID']} failed",exc_info=ex)
            error_sources[row['SOURCE_ID']] = [traceback.format_exc()]
    pd.DataFrame.from_dict(error_sources,orient='index').to_csv(conf.output_dir / conf.run_id /"error.csv")

    print(len(extractions))
    pd.concat(extractions,axis = 0,ignore_index=True).to_csv(conf.output_dir / conf.run_id /"mid.csv") #20956
    
    # load_configuration(input_dir) #rename configuration
