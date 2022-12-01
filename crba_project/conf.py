from dataclasses import dataclass
import uuid
import pandas as pd

@dataclass
class Config():
    """
    Make Config Gloabal Sigelton?!?!?!?
    """

    output_dir: str
    input_dir: str 
    run_id:str 

    def __post_init__(self):
        self.create_output_dir()
        self.input_files()
        self.load_country_list_and_mapping_dictionary()


    def create_output_dir(self):
        # Folder containing manually extracted raw data, ready to be put in the loop
        self.data_sources_staged_raw =  self.output_dir / self.run_id / 'data_staged_raw'
        self.data_sources_staged_raw.mkdir(parents=True, exist_ok=True)

        # Folder to export raw data
        self.data_sources_raw = self.output_dir / self.run_id / 'data_out' / 'data_raw'
        self.data_sources_raw.mkdir(parents=True, exist_ok=True)

        # Folder to export cleansed data
        self.data_sources_cleansed = self.output_dir / self.run_id / 'data_out' / 'data_cleansed'
        self.data_sources_cleansed.mkdir(parents=True, exist_ok=True)

        # Folder to export normalized data
        self.data_sources_normalized = self.output_dir / self.run_id /'data_out' / 'data_normalized'
        self.data_sources_normalized.mkdir(parents=True, exist_ok=True)

        # Folder to export validation results
        self.validation_and_analysis = self.output_dir / self.run_id / 'data_out' / 'data_validation'
        self.validation_and_analysis.mkdir(parents=True, exist_ok=True)
    
    def input_files(self):
        # Folder containing data, which has been extracted manually, but entered by a machine
        self.data_sources_raw_manual_machine = self.input_dir / 'data_raw_manually_extracted' / 'machine_entered'
        self.data_sources_raw_manual_machine.mkdir(parents=True, exist_ok=True)

        # Folder containing data, which has been extracted manually, and entered by a human
        self.data_sources_raw_manual_human = self.input_dir  / 'data_in' / 'data_raw_manually_extracted' / 'human_entered'
        self.data_sources_raw_manual_human.mkdir(parents=True, exist_ok=True)


    def load_country_list_and_mapping_dictionary(self):
        # Load the list of countries which contains all different variations of country names 
        self.country_full_list = pd.read_excel(
            self.input_dir / 'all_countrynames_list.xlsx',
            keep_default_na = False).drop_duplicates()

        # Create a version of the list with unique ISO2 and ISO3 codes
        self.country_iso_list = self.country_full_list.drop_duplicates(subset = 'COUNTRY_ISO_2')

        # Country CRBA list, this is the list of the countries that should be in the final CRBA indicator list
        self.country_crba_list = pd.read_excel(
            self.input_dir / 'crba_country_list.xlsx',
            header = None,
            usecols = [0, 1], 
            names = ['COUNTRY_ISO_3', 'COUNTRY_NAME']).merge(
                right = self.country_iso_list[['COUNTRY_ISO_2', 'COUNTRY_ISO_3']],
                how = 'left',
                on='COUNTRY_ISO_3',
                validate = 'one_to_one')

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

        keys_to_add = locals().keys()-locals_
        
        for key in keys_to_add:
            setattr(self, key, locals()[key])


        