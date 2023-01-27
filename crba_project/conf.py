from typing import List, Union
import uuid
import logging
from importlib.resources import path as res_path
import os.path
import io
import csv

from pathlib import Path
import pandas as pd
import requests_cache
import great_expectations as gx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from crba_project.utils import utils


log = logging.getLogger(__name__)

class Config:
    """
    Make Config Gloabal Sigelton?!?!?!?
    """

    def __init__(self, output_dir,input_dir, run_id=None,filter=None, caching=False, remote_source_config=False,**kwargs):
        
        if run_id==None:
        #TODO replace by datetime string
            run_id = str(uuid.uuid4())
        # In future Versions use input dir to override default package resources
        self.input_dir = Path(input_dir) # Path(args.InputDir)
        self.output_dir =Path(output_dir)  # Path(args.OutputDir)
        self.run_id = run_id
        self.kwargs = kwargs

        self.bootstrap(caching=caching)
        if not remote_source_config:
            self.build_source_config(source_configuration_excel= self.input_dir / "indicator_dictionary_CRBA.xlsx",filter=filter)
        else: 
            self.build_source_config(source_configuration_excel= self.download_file(real_file_id="1mhjOdObYEOt35xxy2T7hm-4zDxIz0LkKHa-XRjn7LF4"),filter=filter)
        print(f"Configuration initialized with run_id:{run_id}")

    def bootstrap(self, caching=False):
        self.create_output_dir()
        self.input_files()
        self.load_country_list_and_mapping_dictionary()
        self.load_un_pop_tot()
        self.load_ge_context()
        if caching:
            self.use_caching()


    def create_output_dir(self):
        # Folder containing manually extracted raw data, ready to be put in the loop
        # self.data_sources_staged_raw =  self.output_dir / self.run_id / 'data_staged_raw'
        # self.data_sources_staged_raw.mkdir(parents=True, exist_ok=True)

        # Folder to export raw data
        self.data_sources_raw = self.output_dir / self.run_id / 'data_raw'
        self.data_sources_raw.mkdir(parents=True, exist_ok=True)

        # Folder to export cleansed data
        self.data_sources_cleansed = self.output_dir / self.run_id / 'data_cleansed'
        self.data_sources_cleansed.mkdir(parents=True, exist_ok=True)

        # Folder to export normalized data
        self.data_sources_normalized = self.output_dir / self.run_id / 'data_normalized'
        self.data_sources_normalized.mkdir(parents=True, exist_ok=True)

        # Folder to export validation results
        self.validation_and_analysis = self.output_dir / self.run_id / 'data_validation'
        self.validation_and_analysis.mkdir(parents=True, exist_ok=True)
        # Folder to export validation results
        self.log_folder = self.output_dir / self.run_id / 'logs'
        self.log_folder.mkdir(parents=True, exist_ok=True)
        # Error Folder. In this folder the dataframes get pushed which failed to get processed.
        self.error_folder = self.output_dir / self.run_id / 'error'
        self.error_folder.mkdir(parents=True, exist_ok=True)

    def input_files(self):
        # Folder containing data, which has been extracted manually, but entered by a machine
        self.data_sources_raw_manual_machine = self.input_dir / 'data_raw_manually_extracted' / 'machine_entered'
        self.data_sources_raw_manual_machine.mkdir(parents=True, exist_ok=True)

        # Folder containing data, which has been extracted manually, and entered by a human
        self.data_sources_raw_manual_human = self.input_dir / 'data_in' / 'data_raw_manually_extracted' / 'human_entered'
        self.data_sources_raw_manual_human.mkdir(parents=True, exist_ok=True)

    def load_country_list_and_mapping_dictionary(self):
        # Load the list of countries which contains all different variations of country names 


        self.country_full_list = pd.read_excel(
            self.input_dir / "all_countrynames_list.xlsx",
            keep_default_na=False).drop_duplicates()

        # Create a version of the list with unique ISO2 and ISO3 codes
        self.country_iso_list = self.country_full_list.drop_duplicates(subset='COUNTRY_ISO_2')

        # Country CRBA list, this is the list of the countries that should be in the final CRBA indicator list
        self.country_crba_list = pd.read_excel(
            self.input_dir / 'crba_country_list.xlsx',
            header=None,
            usecols=[0, 1],
            names=['COUNTRY_ISO_3', 'COUNTRY_NAME']).merge(
            right=self.country_iso_list[['COUNTRY_ISO_2', 'COUNTRY_ISO_3']],
            how='left',
            on='COUNTRY_ISO_3',
            validate='one_to_one')

        # Run the column mapper script to load the mapping dictionary
        locals_ = list(locals().keys())
        with open(self.input_dir / 'column_mapping.py') as file:
            exec(file.read())

        # Run the column mapper script to load the mapping dictionary
        with open(self.input_dir / 'value_mapping.py') as file:
            exec(file.read())

        # Run the column mapper script to load the mapping dictionary
        with open(self.input_dir / 'value_mapping_sdmx_encoding.py') as file:
            exec(file.read())

        keys_to_add = locals().keys() - locals_

        for key in keys_to_add:
            setattr(Config, key, locals()[key])

    def load_un_pop_tot(self):
        un_pop_tot = pd.read_excel(
            io=self.input_dir / "WPP2019_POP_F01_1_TOTAL_POPULATION_BOTH_SEXES.xlsx",
            sheet_name="ESTIMATES",
            header=16,
        )

        # Define list of columns corresponding to year name columns
        years = [x for x in list(range(1950, 2021))]

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
            self.input_dir / "all_countrynames_list.xlsx", keep_default_na=False
        ).drop_duplicates()

        # Add ISO3 code to the list to prepare for join
        un_pop_tot = un_pop_tot.merge(
            right=country_full_list,
            how="outer",
            left_on="Region, subregion, country or area *",
            right_on="COUNTRY_NAME",
        )
        un_pop_tot = un_pop_tot[un_pop_tot['COUNTRY_ISO_3'].notnull()]

        # Discard unnecessary columns
        self.un_pop_tot = un_pop_tot[["year", "population", "COUNTRY_ISO_3"]]

    def build_source_config(self, source_configuration_excel:Union[io.BytesIO, Path],filter: Union[Path, List[str]]):
        """

        Build dataframe which hold all needed information about sources
        This dataframe is the foundation on which the sources get pulled.

        Put more logic in the input sheets. And less in Code.!??!?!

        :param filter: Kan be a path to an csv File where the first Column needs to be a Source ID or a List of Source Id's
        """
        # sources sheet
        crba_data_dictionary_source = pd.read_excel(
            source_configuration_excel,
            sheet_name="Source",
            keep_default_na=False,
        )
        # Delete sources that are deprecated
        crba_data_dictionary_source = crba_data_dictionary_source[
            crba_data_dictionary_source.STATUS != "Deleted"
            ]

        # indicator sheet
        crba_data_dictionary_indicator = pd.read_excel(
            source_configuration_excel,
            sheet_name="Indicator",
            keep_default_na=False,
        )
        # Delete indicators that are deprecated
        crba_data_dictionary_indicator = crba_data_dictionary_indicator[
            crba_data_dictionary_indicator.STATUS != "Deleted"
            ]

        # snapshot sheet. Link between Indicator and Source
        crba_data_dictionary_snapshot = pd.read_excel(
            source_configuration_excel,
            sheet_name="Snapshot_2023",
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
            source_configuration_excel,
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
        # log.info(
        #    f"The Source Config Loaded sucessfully. Numer of Sources{source_self.shape[0]}"
        # )

        # All Input Files need to be in Folder defined by Input_dir
        # The the relative Path in configuration file get adjusted
        # TODO Exclude absolute Path
        source_config["ENDPOINT_URL"] = source_config["ENDPOINT_URL"].apply(
            lambda endpoint: endpoint.replace("file:data_in", f"file:{self.input_dir}"))

        if filter:
            if isinstance(filter, str):
                filter = Path(filter)

            if isinstance(filter, Path):
                filter_df = pd.read_csv(filter, index_col=0, delimiter=";", quotechar="'")
                include_source_id = list(filter_df.index)
            elif isinstance(filter, List):
                include_source_id = filter
            else:
                log.error(f"Filter is of no known type:{type(filter)}")

            source_config = source_config[source_config['SOURCE_ID'].isin(include_source_id)]

        self.source_config = source_config
        
        self.source_config.to_csv(
            path_or_buf = self.output_dir / self.run_id / 'source_config.csv',
            sep = ";",
            quoting=csv.QUOTE_ALL
        )

        

    def use_caching(self):
        """
        Configure gloabl caching
        TODO Move to Extractor?!?!
        """
        log.info("Use Cahcing for request lib")
        requests_cache.install_cache(
            "crba_downloads", backend="filesystem", use_cache_dir=True
        )
    
    def load_ge_context(self):
        with res_path("crba_project.resources","great_expectations") as p:
            self.ge_context = gx.get_context(context_root_dir=p,runtime_environment={'output_dir':f"{os.path.abspath(self.output_dir)}"})

        #self.ge_context.variables.data_docs_sites["local_site"]["store_backend"]["base_directory"] = self.output_dir.resolve() / "data_docs"
        #self.ge_context.config.data_docs_sites["local_site"]["store_backend"]["base_directory"] = self.output_dir.resolve() / "data_docs"

    @property
    def get_gcred(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        SCOPES =  ['https://www.googleapis.com/auth/drive.file',
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/drive.file',
                    'https://www.googleapis.com/auth/drive.metadata'
                ]
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                with res_path("crba_project.resources","client_secret_162354146661-8v7r60qk4rkkqoho3hrto82si92hgnvi.apps.googleusercontent.com.json") as p:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        p, SCOPES)
                    creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        return creds
    
    def download_file(self,real_file_id):
        """
        https://developers.google.com/drive/api/guides/manage-downloads#download_blob_file_content
        Downloads a file
        Args:
            real_file_id: ID of the file to download
        Returns : IO object with location.

        """

        try:
            # create drive api client
            service = build('drive', 'v3', credentials=self.get_gcred)

            file_id = real_file_id

            # pylint: disable=maybe-no-member
            request = service.files().export_media(fileId=file_id,
                                               mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(F'Download {int(status.progress() * 100)}.')

        except HttpError as error:
            print(F'An error occurred: {error}')
            file = None

        return file.getvalue()

