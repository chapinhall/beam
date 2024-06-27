'''
This module prepares data for linkage by running all preprocessing scripts for
input datasets, combining relevant preprocessed datasets (if multiple), and
saves the prepared data to Postgres.

Usage:
    From the top level of the record linkage repository, run:
        python preprocessing/run_preprocess.py [-c CONFIG_JSON_FILEPATH]

To use:
    - For each dataset to be preprocessed, create a script based on
      preprocess_file_template.py. If multiple files need to be processed, a
      preprocessing script must be created for each file.
    - For the project for which the linkage is being completed, store the
      preprocessing script(s) in the corresponding project repository. Update
      the filepath "project_repo" in the config path with the path from your
      home directory to the directory the preprocessing scripts are stored
      for each dataset. Make sure to include the datasource's name in the
      name of each script created.
    - Note that any csvs created from these scripts will be deleted in the
      process of creating the complete preprocessed dataset. Run the scripts
      on their own if you would like to keep each individual preprocessed file.
    - For any dataset that is in csv format and already processed to be
      combined with others, include the filepath in the config file under the
      "combine_prev_csv" for each dataset.
    - Include any dataset currently in the database schema that has already been
      preprocessed in the config file under "combine_prev_tbl" for each dataset.

'''
import os
import re
import csv
import json
import argparse
import shutil
import pathlib
import psycopg2
import subprocess
import pandas as pd
from io import StringIO

from record_linkage_shared.match_functions import connect_to_db

# Copy a specified config.json filepath to the working directory
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config_json_filepath", required=False,
help="Filepath to a copy of the config.json created by config.py")
args = parser.parse_args()

if args.config_json_filepath:
    print(f'Copying config.json from {args.config_json_filepath}')
    shutil.copy(args.config_json_filepath, "./config.json")
else:
    print('Using existing config.json in the working directory')

# Loads config file
CONFIG_FILE = 'config.json' # Filepath to generated match configuration
cwd = os.getcwd()
with open(CONFIG_FILE, "r") as f:
    config = json.loads(f.read())

# Preprocess each dataset
for ds_key in ["df_a", "df_b"]:
    all_files = []
    ds_config = config['data_param'][ds_key]
    if not ds_config:
        continue

    print(f"PREPROCESSING FOR: {ds_config['name'].upper()}")
    database_config = config["database_information"]
    conn, schema, _ = connect_to_db(database_config)
    admin_role = schema + "admin"
    tablename = schema + "." + ds_config["name"]
    cursor = conn.cursor()

    # Read in any previous data in database
    if ds_config.get("combine_prev_tbl"):
        for table in ds_config["combine_prev_tbl"]:
            print(f"Reading in previously combined table: {table}")
            cmd = f"""SELECT * FROM {schema}.{table}"""
            df = pd.read_sql(cmd, conn)
            all_files.append(df)
    if ds_config.get("project_repo"):
        project_repo = os.path.join(pathlib.Path.home(), ds_config["project_repo"])
        print(f'Accessing project repo for preprocessing scripts: {project_repo}')
        # Run any preprocessing scripts for this data source and read in the results
        for script in os.listdir(project_repo):
            if ds_config['name'] in script and "preprocess" in script:
                print(f"\tRunning: {script}")
                subprocess.call(['python3', script], cwd=project_repo)
        for file in os.listdir(project_repo):
            if f"preprocess_{ds_config['name'].lower()}" in file.lower() and "csv" in file:
                print(f"\tReading in results: {file}")
                df = pd.read_csv(os.path.join(project_repo, file), dtype=str)
                all_files.append(df)
                os.remove(os.path.join(project_repo, file))
    # Read in all the previously processed csvs for this data source
    if ds_config.get("combine_prev_csv"):
        for file in ds_config["combine_prev_csv"]:
            print(f"\tReading in previously combined file (csv): {file}")
            df = pd.read_csv(file, dtype=str)
            all_files.append(df)

    # Combine and add datasets to postgres
    if all_files:
        # Combine all datasets
        data = pd.concat(all_files)
        # Remove duplicates and add idx
        data = data.drop_duplicates().reset_index()
        data['idx'] = data.index
        data.drop(columns="index", inplace=True)
        # Make command to create table
        print("Creating table {}".format(tablename))
        create = 'SET ROLE {admin_role};\n'\
                 + 'CREATE TABLE IF NOT EXISTS {target_table} (\n'\
                 + '    {} TEXT,\n' * len(data.columns)
        create = create[:-2] + '\n);'  # remove comma on last column
        create = create.format(admin_role=admin_role,
                               target_table=tablename,
                               *data.columns)
        with conn.cursor() as cursor:
            cursor.execute(create)
            # Truncate the table
            cursor.execute('TRUNCATE {target_table}'.format(
                           target_table=tablename))
            with StringIO() as buffer:
                # Add the data to the table
                data.to_csv(buffer, sep=',', na_rep=None, header=False,
                            index=False, quoting=csv.QUOTE_NONNUMERIC)
                buffer.seek(0)  # move cursor back to start

                copy_cmd = 'COPY {} FROM STDIN CSV;'.format(tablename)
                print(copy_cmd)
                cursor.copy_expert(copy_cmd, buffer)
                print('Rows inserted: {:,}'.format(cursor.rowcount))
        # Save csv to input directory
        save_path = f"{config['input_dir']}preprocess_{ds_config['name']}.csv"
        data.to_csv(save_path, index=False)
        conn.commit()
        conn.close()
