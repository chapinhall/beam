'''
Template designed to preprocess data.

To use:
    For each file that needs to be preprocessed,
    - Copy this template to a project repository and rename
        preprocess_file_{datasource}{file description}.py
      Where file description is any differentiation between multiple files.
      This can be removed if only one file is being processed.
    Fill in the information below for the file.
'''
import psycopg2
import jellyfish
import pandas as pd
import numpy as np

from record_linkage_shared import preprocess_functions

######################### THINGS FOR USER TO CHANGE ##########################

# RAW DATA INFORMATION
filetype = "" # Options: csv, excel, fwf, db

if filetype in ("csv", "excel", "fwf"):
    filename = ""
    if filetype == "fwf":
        fwf_args = {} # Only for fixed width files
elif filetype == "db":
    table = ""
    schema = ""
    host = ""
    database = ""
else:
    raise Exception(f'Unable to read in this type of file: {filetype}')

# Other source data info
datasource = "" # Name of data set (not specific file)
year = 2022 # Year of current data set (only if we need to create a source id)
file_num = "1" # assign different values for multiple files

# COLUMNS
# input the raw column name of each preprocessed column name as a string
# If not included, leave as empty string ie ''

# If using source_id, it should be included and unique for all distinct
# records across each file processed.
# Otherwise, if there is no unique identifier for each record,
# leave as empty string and a source_id will be created
preprocessed_to_raw_col_names = {
                'source_id': '',
                'fname': '',
                'mname': '',
                'lname': '',
                'suffix': '',
                'altlname': '',
                # Label all the same field if birthdays are in one field
                'bmonth': '',
                'bday': '',
                'byear': '',
                'zipcode': '',
                'county': ''
            }
# For any other id fields (to use as common_id or ground truth ids) or fields
# that should be kept after preprocessing, map out the wanted column name
# to the raw column name, such as (e.g. {"sid": "Student ID"})
other_preprocessed_to_raw_vals = {}

# If you want to assign a single value for any of the preprocessed columns
# list them here.
# (e.g. to assign all records as Cook county, enter {"county": "031"} )
preprocessd_col_to_assigned_val = {}

# BAD INPUTS
# for each, list any value that should be ignored (such as '99999999' for ids)
# Please input all as strings
bad_inputs = {
    "source_id": ['99', '9900', '0', 'NULL', 'n/a', '99999999'],
    "fname": [],
    "mname": [],
    "lname": [],
    "suffix": [],
    "altlname": [],
    "bmonth": ['00', '0'],
    "bday": ['00', '0'],
    "byear": [],
    "ids": ['99', '9900', '0', 'NULL', 'n/a', '99999999'],
    "zipcode": [],
    "county": []
}

##############################################################################

# Try not to change below, but you can.

# Flip col_dict for renaming
raw_to_preprocessed_col_names = {}

for key, val in preprocessed_to_raw_col_names.items():
    if not val:
        continue # skip empty strings
    if val not in raw_to_preprocessed_col_names:
        raw_to_preprocessed_col_names[val] = key
    else:
        raw_to_preprocessed_col_names[val] = "dob"

for key, val in other_preprocessed_to_raw_vals.items():
    raw_to_preprocessed_col_names[val] = key

# Read in raw data, only specified columns
raw_cols = raw_to_preprocessed_col_names.keys()

if filetype == 'fwf':
    data = pd.read_fwf(filename, **fwf_args, dtype=str)
    data = data[raw_cols]
elif filetype == 'csv':
    data = pd.read_csv(filename, usecols=raw_cols, dtype=str)
elif filetype == 'excel':
    data = pd.read_excel(filename, usecols=raw_cols, dtype=str)
else:
    conn = psycopg2.connect(host=host, dbname=dbname)
    select_cols_str = ','.join(raw_cols)
    cmd = f"""SELECT {select_cols_str} FROM {schema}.{table}"""
    data = pd.read_sql(cmd, conn)
    conn.close()

# Rename columns
data = data.rename(raw_to_preprocessed_col_names, axis=1).fillna('')

# Format birthday if needed
if "dob" in data:
    data['dob'] =  pd.to_datetime(data.dob, errors='coerce')
    data['byear'] = data['dob'].dt.strftime("%Y")
    data['bmonth'] = data['dob'].dt.strftime("%-m").str.lstrip("0")
    data['bday'] = data['dob'].dt.strftime('%-d').str.lstrip("0")

# Assign row id (orig_id) based on year/file/row if needed
if "source_id" not in data.columns:
    data = data.assign(source_id=[f'{year}{file_num}{str(i).zfill(7)}'
                                  for i in range(0, len(data))])

# Assign single value to specified columns
for key, value in preprocessd_col_to_assigned_val.items():
    data[key] = value

# Add placeholder columns
keep_cols = list(preprocessed_to_raw_col_names.keys())
other_vals = list(other_preprocessed_to_raw_vals.keys())
keep_cols.extend(other_vals)
for col in keep_cols:
    if col not in data.columns:
        data[col] = ''

# Replace bad values (NAs, 0s, etc) with None (or empty string?)
for key, to_replace in bad_inputs.items():
    if key != 'ids':
        data[key] = data[key].replace(to_replace, '')
    else:
        data[other_vals] = data[other_vals].replace(to_replace, '')

# Clean names
data[['fname', 'mname', 'altfname', 'suffix_f']] = data.apply(
    lambda row: preprocess_functions.fix_fname(row["fname"],
                                               row["mname"],
                                               bad_inputs["fname"]),
    axis=1).to_list()

data[["lname",  "suffix_l","altlname"]] = data.apply(
    lambda row: preprocess_functions.fix_lname(row["lname"],
                                               row["altlname"],
                                               bad_inputs["lname"]),
    axis=1).to_list()

# Update suffix based on cleaning results
data["suffix"] = np.where(data["suffix_l"] != '',
                          data["suffix_l"],
                          data["suffix_f"])

# Shorten zipcodes if needed
data["zipcode"] = np.where(data["zipcode"].str.len() > 5,
                           data["zipcode"].str[:5],
                           data["zipcode"])

# Create aliases and get soundex
data = preprocess_functions.get_aliases(data, lname=False)
data = preprocess_functions.get_aliases(data, lname=True)
data = preprocess_functions.get_mname_aliases(data)

# Create middle initial
data["minitial"] = np.where(data["mname"].str.len() > 0,
                            data["mname"].str[:1],
                            '')

data["xf"] = data["fname"].apply(lambda x: jellyfish.soundex(x).strip('0')
                                           if x is not None else None)
data["xl"] = data["lname"].apply(lambda x: jellyfish.soundex(x).strip('0')
                                           if x is not None else None)

# Fix column order for consistency
keep_cols = ["source_id",
             "fname",
             "xf",
             "mname",
             "minitial",
             "lname",
             "xl",
             "altlname",
             "suffix",
             "byear",
             "bmonth",
             "bday",
             "zipcode",
             "county"]
keep_cols.extend(other_vals)
data = data[keep_cols].drop_duplicates()

# Save results to temp csv
data.to_csv(f"preprocess_{datasource}_{file_num}.csv", index=False)