'''
Configuration file

Keys must be included for each dataset:
    - name: (str) a short hand for the input data
    - filetype: (str) type of the input file, e.g. 'csv', 'fwf', 'xlsx', etc.
    - vars: (dict)
        key: shared variable name referred to by the standard match logic
            and any ground truth ID listed
        value: specific column name of that variable in the input dataset
        example:
            'vars' = {
                'common_id' = 'SSN',
                'fname' = 'FirstName'
                'lname' = 'LastName'
                'sacwis_id' = 'sacwis_id' # ground truth ID 1
                'cli_id' = 'cli_id' # ground truth ID 2
            }
      standard shared variables include: common_id, fname, mname, lname, altlname,
      xf, xl, byear, bmonth, bday, [other ground truth ids])

Optional keys:
    - fwf_args: (dict) fixed-width-file-specific arguments
    - dtype: (dict) column to dtype mappings

***
If this match is a deduplication, set data_param['df_b'] = None***
'''

import json
import os
import shutil

# Directory of input data (i.e. where raw data are stored)
input_dir = ''

# Directory to output data (i.e. where match results are saved)
output_dir = ''

# A config.json file will be saved in the current working directory.
# Additionally, a copy of this file will also be saved in input_dir.
# Define the filename of the copy saved in input_dir.
json_file_name = 'example.json'

# Match type (Options: 121, M21, 12M, M2M, dedup)
matchtype = ""

# Database info for a schema to store intermediary tables.
# If using the preprocessing function, this is also where preprocessed datasets
# are saved.
db_info = {
    "schema": "match",
    "dbname": "postgres",
    "host": "etldev"
}

### Input dataset parameters --------------------------------------------------

# Information about the first data set
df_a =  {
        ### Parameters for using preprocessed data for match ###

        # Name to refer to dataset - this is used in naming outputs
        'name': '',
        # Path to *preprocessed* data file - skip if dataset is in Postgres
        'filepath':  '',
        # Filetype of *preprocessed* data file - options include: fwf, csv, db
        #   fwf: fixed-width file
        #   csv: csv file
        #   db: Postgres table
        'filetype': '',
        # Fixed-width-file-specific arguments for loading preprocessed data as a Pandas DataFrame
        # Skip if not applicable
        'fwf_args': {
            'names': [],
            'widths': [],
            'encoding': ''
            },
        # Postgres specific arguments for loading preprocessed data as a Pandas DataFrame
        # Skip if not applicable
        'db_args': {
            'tablename': '',
            'schema': '',
            'host': '',
            'dbname': ''
        },
        # Specify data type of any *preprocessed* variable as needed.
        # e.g. 'dtype': {
        #   'child_id': 'str',
        #   'bday': 'float'
        #  }
        # Options include: "str": string, "int": integer, "float": float
        # Note that you cannot have integer data type for any numeric column
        # with missing data
        'dtype': {
        },
        # Map each standard match variable listed below to the *preprocessed*
        # variable name. If a match variable is not included in your data, delete
        # the key-value pair for it.
        'vars' :{
            ### ID that represents an individual in this dataset
            'indv_id': '',
            ### Comparison variables
            # ID that is shared with the other dataset (e.g. SSN, student ID)
            'common_id': '',
            # Name components
            'fname': '',
            'mname': '',
            'lname': '',
            'altlname': '',
            # middle name initial, created in preprocessing, only remove if
            # middle name is not included in the data
            'minitial': 'minitial',
            # Name soundex
            'xf': '', # soundex of first name
            'xl': '', # soundex of last name
            # birthdate
            'byear': '',
            'bmonth': '',
            'bday': '',
            # Geography
            'zipcode': '',
            'county': ''
                },

        ### Parameters for preprocessing raw data ###

        # Path to the research project's repo, where preprocessing scripts are saved.
        # Enter the path from user's home directory (e.g. gitlab/analysis123/)
        'project_repo': '',
        # If to combine with previously preprocessed data stored as a table in
        # the *same schema* (defined in db_info), include the table name as
        # string in the list below.
        # Leave empty otherwise.
        'combine_prev_tbl': [],
        # If to combine with previously preprocessed data files that are csv,
        # add the filepath to the list below.
        'combine_prev_csv': []
        # Otherwise, create a preprocessing script from template
        # for the additional files.
        }

# Information about the second data set (leave as empty brackets if the match is a dedup)
df_b =  {
        ### Parameters for using preprocessed data for match ###

        # Name to refer to dataset - this is used in naming outputs
        'name': '',
        # Path to *preprocessed* data file - skip if dataset is in Postgres
        'filepath':  '',
        # Filetype of *preprocessed* data file - options include: fwf, csv, db
        #   fwf: fixed-width file
        #   csv: csv file
        #   db: Postgres table
        'filetype': '',
        # Fixed-width-file-specific arguments for loading preprocessed data as a Pandas DataFrame
        # Skip if not applicable
        'fwf_args': {
            'names': [],
            'widths': [],
            'encoding': ''
            },
        # Postgres specific arguments for loading preprocessed data as a Pandas DataFrame
        # Skip if not applicable
        'db_args': {
            'tablename': '',
            'schema': '',
            'host': '',
            'dbname': ''
        },
        # Specify data type of any *preprocessed* variable as needed.
        # e.g. 'dtype': {
        #   'child_id': 'str',
        #   'bday': 'float'
        #  }
        # Options include: "str": string, "int": integer, "float": float
        # Note that you cannot have integer data type for any numeric column
        # with missing data
        'dtype': {
        },
        # Map each standard match variable listed below to the *preprocessed*
        # variable name. If a match variable is not included in your data, delete
        # the key-value pair for it.
        'vars' :{
            ### ID that represents an individual in this dataset
            'indv_id': '',
            ### Comparison variables
            # ID that is shared with the other dataset (e.g. SSN, student ID)
            'common_id': '',
            # Name components
            'fname': '',
            'mname': '',
            'lname': '',
            'altlname': '',
            # middle name initial, created in preprocessing, only remove if
            # middle name is not included in the data
            'minitial': 'minitial',
            # Name soundex
            'xf': '', # soundex of first name
            'xl': '', # soundex of last name
            # birthdate
            'byear': '',
            'bmonth': '',
            'bday': '',
            # Geography
            'zipcode': '',
            'county': ''
                },

        ### Parameters for preprocessing raw data ###

        # Path to the research project's repo, where preprocessing scripts are saved.
        # Enter the path from user's home directory (e.g. gitlab/analysis123/)
        'project_repo': '',
        # If to combine with previously preprocessed data stored as a table in
        # IN THE SAME SCHEMA, include the table name as string in the list below.
        # Leave empty otherwise.
        'combine_prev_tbl': [],
        # If to combine with previously preprocessed data files that are csv,
        # add the filepath to the list below.
        'combine_prev_csv': []
        # Otherwise, create a preprocessing script from template
        # for the additional files.
        }


### "Ground truth" IDs --------------------------------------------------------

# List of IDs that we consider as "ground truth" for identifying a person.
# If a pair have the same ground truth IDs, we consider them a match
# *regardless* of differences in other match fields. We will scan the data to
# group any records sharing the same ground truth ID before running the formal
# match algorithm.

ground_truth_ids = []


### Blocking strategy ---------------------------------------------------------

# Lists of blocking variables by blocking pass
# - For any inverted blocking passes, add '_inv' to the end of each of
# the two variables to invert (e.g. xf_inv, xl_inv)
# - If you want to skip a pass in the default blocking strategy, leave
# the list for that pass as empty brackets.
blocks_by_pass = [
    ['common_id', 'fname', 'lname', 'byear', 'bmonth', 'bday'], # pass 0
    ['common_id'], # pass 1
    ['xf', 'xl'] , # pass 2
    ['xf_inv', 'xl_inv'] , # pass 3, inverted soundex
    ['byear', 'bmonth', 'bday'] # pass 4
]

### Comparison variables and similarity measures  -----------------------------

# Lists of comparison names by blocking pass
# These comparison names will have corresponding similarity measurements defined
# in sim_param below.
comp_names_by_pass = [
  [], # pass 0, N/A
  ['fname', 'mname', 'lname', 'altlname',
      'bmonthbday', 'byear', 'fnamelname', 'lnamefname'], # pass 1
  ['fname', 'mname', 'lname', 'altlname',
      'bmonthbday', 'byear', 'common_id', 'minitial',
      'zipcode', 'county'] , # pass 2
  ['fnamelname', 'mname', 'lnamefname', 'altlname',
      'bmonthbday', 'byear', 'common_id', 'minitial',
      'zipcode', 'county'] , # pass 3
  ['fname', 'mname', 'lname', 'altlname',
   'common_id', 'minitial', 'zipcode', 'county'] # pass 4
]

# Parameters for similarity measures corresponding to each comparison name
# listed in comp_names_by_pass above. See match_helpers.prepare_comparers()
# for all how they are used to construct the similarity algorithms
#
# Options for similarity measures: 'jarowinkler', 'levenshtein', 'exact', 'numeric', 'date'
# Other similarity measures can be customized.
# See recordlinkage documentation on the similarity measures for more.

sim_param = {
  'fname':{
      'missing_value': 0.5,
      'comparer': 'jarowinkler'
      },
  'lname':{
      'missing_value': 0.5,
      'comparer': 'jarowinkler'
      },
  'mname':{
      'missing_value': -1,
      'comparer': 'jarowinkler'
      },
  'altlname':{
      'missing_value': 0.5,
      'comparer': 'jarowinkler'
      },
  'fnamelname':{
      'missing_value': 0.5,
      'comparer': 'inv_jarowinkler'
      },
  'lnamefname':{
      'missing_value': 0.5,
      'comparer': 'inv_jarowinkler'
      },
  'bmonthbday': {
      'swap_month_day': 0.8,
      'either_month_day': 0.6,
      'missing_value': 0.5,
      'comparer': 'bmonthbday'
      },
  'byear': {
      'within_1y': 0.7,
      'missing_value': 0.5,
      'comparer': 'byear'
      },
  'common_id':{
      'missing_value': -1,
      'comparer': 'levenshtein'
      },
  'zipcode':{
      'missing_value': 0.5,
      'comparer': 'exact'
  },
  'county':{
      'missing_value': 0.5,
      'comparer': 'exact'
  },
   'minitial':{
      'minit_match_mname_unclear': 0.7,
      'missing_value': 0.5,
      'comparer': 'minitial'
   }
  }


### Custom criteria for accepting matches -------------------------------------

# The acceptance criteria are the set of rules we use to evaluate the similarity
# scores between comparison variables for each blocking pass and strictness level,
# to decide whether a pair is a match or not. If you would like to use your own
# acceptance criteria for a match, please:
#     1. Create a custom script named `accept_functions.py` in a separate
#        directory, by making a copy of ./shared/record_linkage_shared/accept_functions.py
#        and editing the functions to your project needs.
#        There should be one function for each level of strictness (strict,
#        moderate, relaxed and review) for each pass
#     2. Update `alt_acceptance_dir` below with the directory where your custom
#        `accept_functions.py` is saved.
#     3. If using new variables or comparisons for the custom match, update all
#        configuration variables under "Blocking strategy" section above accordingly


# Path (from your home user directory) to the directory where
# accept_functions.py is saved
alt_acceptance_dir = ''


# Parallelization metrics -----------------------------------------------------

# The metrics used to complete parallelization
parallelization_metrics = {
  "chunk_sizes": {
  # The number of pairs pulled at a time to compare by pass. 
  # Recommend around 500000 for passes that contain less than 5 million pairs
  # and 1000000 otherwise.
    0: 50000,
    1: 50000,
    2: 300000,
    3: 100000,
    4: 300000
  },
  # The number of processes the matching will take up on the server.
  # Contact your IT support to determine the appropriate range of numbers of 
  # processes to use.
  "num_processes": 5

}

#################### DO NOT CHANGE ANYTHING BELOW ############################

config_json = {}

config_json["input_dir"] = input_dir
config_json["output_dir"] = output_dir
config_json["data_param"] = {}
config_json["data_param"]["df_a"] = df_a
config_json["data_param"]["df_b"] = df_b
config_json["database_information"] = db_info
config_json["alt_acceptance_dir"] = alt_acceptance_dir

config_json["matchtype"] = matchtype
config_json["ground_truth_ids"] = ground_truth_ids
config_json["blocks_by_pass"] = blocks_by_pass
config_json["comp_names_by_pass"] = comp_names_by_pass
config_json["sim_param"] = sim_param
config_json["parallelization_metrics"] = parallelization_metrics

config_json["cutoff_scores"] =  {
                                    "name_high_score": 0.88,
                                    "name_very_high_score": 0.90,
                                    "id_high_score": 0.75,
                                    "name_review_score": 0.8,
                                    "id_review_score":0.65
                                 }

output_file = os.path.join(input_dir, json_file_name)

with open('config.json', 'w') as f:
    json.dump(config_json, f, indent=4)

shutil.copy('config.json', output_file)
