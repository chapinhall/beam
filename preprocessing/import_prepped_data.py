"""
Load preprocessed tables for match into postgres
"""
import csv
import json
import psycopg2
from io import StringIO

import pandas as pd

CONFIG_FILE = "config.json"

# Read in configuration file.
with open(CONFIG_FILE, "r") as f:
    config = json.loads(f.read())

# set database information
schema = config['database_information']["schema"]
admin_role = schema + "admin"
dbname = config["database_information"]["dbname"]
host = config["database_information"]["host"]

# declare variables
filenames = {}
tables = {}
target_tables = {}

# add information on table a
config_a = config["data_param"]["df_a"]
if config_a.get("filepath"):
    filenames["df_a"] = config_a["filepath"]
    tables["df_a"] = config_a["name"]
    target_tables["df_a"] = schema + '.' + tables["df_a"]
else:
    print(f"{config_a['name']} is already in the database, skipping")

# if table b exists, add information
config_b = config["data_param"]["df_b"]
if config["matchtype"] != "dedup" and config_b.get("filepath"):
    filenames["df_b"] = config_b["filepath"]
    tables["df_b"] = config_b["name"]
    target_tables["df_b"] = schema + '.' + tables["df_b"]
elif config["matchtype"] != "dedup":
    print(f"{config_b['name']} is already in the database, skipping")

# read and import each table for the match
for df, filename in filenames.items():
    print(f"Reading data for {tables[df]}")
    dtype = {}

    # specify dtypes if needed
    for var, typ in config["data_param"][df]['dtype'].items():
        if typ == 'str':
            dtype[var] = str
        elif typ == 'float':
            dtype[var] = float
        elif typ == 'int':
            dtype[var] = int

    # only read fwf or csv (assumes db tables already in same db)
    filetype = config["data_param"][df]["filetype"]
    if filetype == "fwf":
        fwf_args = config["data_param"][df]["fwf_args"]
        data = pd.read_fwf(filename, **fwf_args, dtype=dtype)
    elif filetype == "csv":
        data = pd.read_csv(filename, dtype=dtype)
    else:
        raise Exception('import not yet built for this filetype')
    print("Raw file row count: {:,}".format(data.shape[0]))

    data = data.rename_axis('idx').reset_index()

    # convert birthday strings if necessary
    if "dob_str" in data.columns:
        data['byear'] = data.dob_str.str[:4].astype(float)
        data['bmonth'] = data.dob_str.str[4:6].astype(float)
        data['bday'] = data.dob_str.str[6:8].astype(float)

    # Replace null characters
    data.replace(to_replace='[\0\r]', value='',
                 regex=True, inplace=True)

    # Make command to create table in resdb01
    print("Creating table {}".format(target_tables[df]))
    create = 'SET ROLE {admin_role};\n'\
             + 'CREATE TABLE IF NOT EXISTS {target_table} (\n'\
             + '    {} TEXT,\n' * len(data.columns)
    create = create[:-2] + '\n);'  # remove comma on last column
    create = create.format(admin_role=admin_role,
                           target_table=target_tables[df],
                           *data.columns)
    conn = psycopg2.connect(dbname=dbname, host=host)

    with conn.cursor() as cursor:
        cursor.execute(create)
        cursor.execute('TRUNCATE {target_table}'.format(
                       target_table=target_tables[df]))
        conn.commit()
        with StringIO() as buffer:

            data.to_csv(buffer, sep=',', na_rep=None, header=False,
                        index=False, quoting=csv.QUOTE_NONNUMERIC)
            buffer.seek(0)  # move cursor back to start

            copy_cmd = 'COPY {} FROM STDIN CSV;'.format(target_tables[df])
            print(copy_cmd)
            cursor.copy_expert(copy_cmd, buffer)
            conn.commit()
            print('Rows inserted: {:,}'.format(cursor.rowcount))

    conn.close()
