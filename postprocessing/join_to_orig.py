"""
This is a script for joining the final ID crosswalk from the match back to the
preprocessed version of an original dataset. This is mostly used for an
intermediary match (such as a deduplication before linking to another dataset),
and the output can directly be used for another match without further processing.

The default output will be a Postgres table:
    <schema listed in config>.<orig_dataset_name>_xwalked
This script has an option to save the output as csv.

Usage:
    join_to_orig.py [-h] -s {strict,moderate,relaxed} [-d {df_a,df_b}] [-csv]

    Arguments:
        -s, --strict_type: strictness level of crosswalk used
                Valid values: ('strict', 'moderate', 'relaxed')
        -d, --dataset_key: (Optional, skip if this is a dedup)
                Key of orginal dataset. Valid values: ('df_a', 'df_b').
        -csv, --save_as_csv: (Optional) flag to save the output as csv

"""
import os
import re
import argparse
import psycopg2
import json
import pandas as pd
from io import StringIO

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--strict_type", required=True,
    choices=['strict', 'moderate', 'relaxed'],
    help="Strictness level of crosswalk used")
parser.add_argument("-d", "--dataset_key", required=False,
    choices=['df_a', 'df_b'],
    help="Skip if this is a dedup - key of orginal dataset")
parser.add_argument("-csv", "--save_as_csv", action='store_true',
    help="Flag to save the output as csv")
args = parser.parse_args()

orig_ds_key = args.dataset_key
stricttype = args.strict_type
save_as_csv = args.save_as_csv

CONFIG_FILE = "config.json"

# Load in configuration file.
with open(CONFIG_FILE, "r") as f:
    config = json.loads(f.read())

# Set variables for creating crosswalk tables
matchtype = config["matchtype"]
df_a_info = config["data_param"]["df_a"]
indv_id_a = df_a_info['vars']["indv_id"]
name_a = df_a_info["name"]

if matchtype == "dedup":
    name_b = "dedup"
    xwalk_columns = [indv_id_a, "ch_id"]
    # Create dictionary of wanted data types.
    datatypes = {"ch_id": str, "orig_id": str}

else:
    df_b_info = config["data_param"]["df_b"]
    name_b = df_b_info["name"]
    indv_id_b = df_b_info['vars']["indv_id"]
    # Create dictionary of wanted data types.
    datatypes = {"passnum": float}
    if matchtype == "M2M":
        xwalk_columns = [indv_id_a, indv_id_b, "ch_id", "passnum"]
        datatypes["ch_id"] = str
    else:
        xwalk_columns = [indv_id_a, indv_id_b, "passnum"]

xwalk_table = name_a + "_" + name_b + "_xwalk"

# Find latest version of wanted crosswalk.
paths = []
cw_format = f"final_xwalk_{name_a}_{name_b}_\d{{4}}-\d{{2}}-\d{{2}}_{stricttype}.csv"
directory = os.path.join(config["output_dir"], "postprocessing")
print(f'All crosswalks found for this match ({stricttype}) in {directory}')
for basename in os.listdir(directory):
    if re.match(cw_format, basename):
        print(f'\t{basename}')
        paths.append(os.path.join(directory, basename))
crosswalk = max(paths, key=os.path.getctime)
print(f'Crosswalk file used (latest version): {crosswalk}')

database_config = config["database_information"]
host = database_config['host']
dbname = database_config['dbname']
schema = database_config['schema']
conn = psycopg2.connect(host=host, dbname=dbname)
conn.autocommit = True
cursor = conn.cursor()

admin_role = schema + "admin"
cursor.execute(f"SET ROLE {admin_role};")

# All column but last should be TEXT
column_types = " TEXT, ".join(xwalk_columns)
columns_alone = ", ".join(xwalk_columns)

# Last column is numeric if pass number, otherwise TEXT
create_table = f'''CREATE TABLE IF NOT EXISTS {schema}.{xwalk_table} (
 {column_types} {"TEXT" if xwalk_columns[-1] != "passnum" else "NUMERIC(2,1)"});'''

# Create the crosswalk table.
print(create_table, "\n")
cursor.execute(create_table)

# Read in crosswalk and update with original columns.
data = pd.read_csv(crosswalk, dtype='str')
data.columns = xwalk_columns
print("NUMBER OF ROWS IN CROSSWALK: ", data.shape[0])
print(data.dtypes)
cursor.execute(f"TRUNCATE {schema}.{xwalk_table}")

# Copy the crosswalk into postgres.
with StringIO() as buffer:
    data.to_csv(buffer, sep=',', na_rep=None, header=False, index=False)
    buffer.seek(0)
    copy_crosswalk = f'''COPY {schema}.{xwalk_table}
                        FROM STDIN CSV;'''

    print(copy_crosswalk, "\n")
    cursor.copy_expert(copy_crosswalk, buffer)
    rows = cursor.rowcount
    print(f"ROWS ADDED to {xwalk_table}: {rows}")

if matchtype == 'dedup':
   orig_ds_key = 'df_a'

# Specify new ID to be joined back to orig data
if matchtype in ('M2M', 'dedup'):
    new_id = 'ch_id'
else: # 12M, 121, M21
    if orig_ds_key == 'df_a':
        new_id = xwalk_columns[1] # indv_id_b
    elif orig_ds_key == 'df_b':
        new_id = xwalk_columns[0] # indv_id_a
    else:
        raise ValueError(f'orig_ds_key is not valid for this {matchtype} match')

# Set up other components for the join
orig_table = config['data_param'][orig_ds_key]['name']
orig_table_with_new_id = f'{orig_table}_xwalked_to_{new_id}_{stricttype}'
orig_indv_id = config['data_param'][orig_ds_key]['vars']['indv_id']
if matchtype == "M2M":
    # need to consider if left join/not unique crosswalk will result
    # in multiple entries
    leftjoin = f'''(SELECT DISTINCT {orig_indv_id}, {new_id}
                FROM {schema}.{xwalk_table})'''
else:
    leftjoin = f"{schema}.{xwalk_table}"

# Create new table by joining old table with crosswalk.
join_statement = f'''
    CREATE TABLE {schema}.{orig_table_with_new_id} AS
    SELECT orig_table.*,
    xwalk.{new_id} AS {new_id}
    FROM {schema}.{orig_table} orig_table
    LEFT JOIN {leftjoin} xwalk
    ON orig_table.{orig_indv_id} = xwalk.{orig_indv_id}'''
print(join_statement, "\n")

cursor.execute(f'DROP TABLE IF EXISTS {schema}.{orig_table_with_new_id}')
cursor.execute(join_statement)
print(f"ROWS ADDED to {orig_table_with_new_id}: {cursor.rowcount}")

# if wanted, pull that new table to a csv file.
if save_as_csv:
    save_query = f'''COPY (select * from {schema}.{orig_table_with_new_id})
                    TO STDOUT DELIMITER ','
                    CSV HEADER;'''
    cw_dir = os.path.join(config['output_dir'], "crosswalked_data")
    if not os.path.isdir(cw_dir):
        os.makedirs(cw_dir)
    file_path = os.path.join(cw_dir, f"{orig_table_with_new_id}.csv")
    with open(file_path, "w") as outfile:
        cursor.copy_expert(save_query, outfile)
    print(f"SAVED AS CSV: {file_path}")

conn.close()
