'''
Core match module

This module loads preprocessed data, calculates similarities, accept matches,
and save all pairs evaluated and their match results for postprocessing.

Config.json includes all parameters of the input data and the match,
including the following:

- Match type (1:1, 1:M, M:M, dedup)
- Input file parameters
- Output directory
- Blocking strategies
- Similarity scores for partial matches

Usage:

From top-level repository, run
    python matching/match.py

'''

import json
import time
import datetime
import numpy as np
import pandas as pd
import recordlinkage

from record_linkage_shared import block_functions
from record_linkage_shared import match_functions
from record_linkage_shared.match_functions import run_match_parallelized

CONFIG_FILE = 'config.json' # Filepath to generated match configuration

with open(CONFIG_FILE, "r") as f:
    config = json.loads(f.read())

matchtype = config['matchtype']

### Load preprocessed data and match parameters ###  --------------------------
print(f'Start time: {datetime.datetime.now()}')
last_time = time.time()

# Identify input data
name_a = config['data_param']['df_a']['name']
if matchtype == 'dedup':
    name_b = 'dedup'
    print(f'Loading preprocessed data for deduplication: {name_a}...')
else:
    name_b = config['data_param']['df_b']['name']
    print(f'Loading preprocessed data... \n(A) {name_a}\n(B) {name_b}')

# Load data (tablename_a referring to name of table in database)
df_a, tablename_a = match_functions.load_data('df_a', config)
vars_a = config['data_param']['df_a']['vars']

if matchtype == 'dedup':
    df_b = None
    tablename_b = tablename_a
    vars_b = vars_a
else:
    df_b, tablename_b = match_functions.load_data('df_b', config)
    vars_b = config['data_param']['df_b']['vars']

print('Check column data types')
print(f'{name_a}:\n', df_a.dtypes)
if matchtype != 'dedup':
    print(f'{name_b}:\n', df_b.dtypes)

match_functions.print_runtime(last_time)

### Initialize variables and database connection ###  -------------------------
# Dataframe to store index pairs' similarity scores
df_sim = pd.DataFrame(columns=['indv_id_a', 'indv_id_b', 'idx_a', 'idx_b'])

# String representing part of the SQL join conditions used for blocking.
# This condition is updated dynamically each pass to prevent subsequent
# passes from blocking on repeated pairs from previous passes.
past_join_cond_str = ''

# Connection to database
db_info = config["database_information"]
conn, schema, _ = match_functions.connect_to_db(db_info)
table_a = f'{schema}.{tablename_a}'
table_b = f'{schema}.{tablename_b}'

# Dataframe containing counts of matched pairs in each pass by strictness level
counts = pd.DataFrame(columns=["passnum", "strictness", "match"])

# Pre-match: find pairs sharing ground truth IDs ### --------------------------
last_time = time.time()
if config["ground_truth_ids"]:
    # Cursor used to create, read and drop ground truth ids candidates tables
    gid_cursor = conn.cursor()
    gid_cursor.execute(f'SET ROLE {schema}admin;')
    past_join_cond_str = block_functions.run_ground_truth_ids_passes(
                                                        config["ground_truth_ids"],
                                                        vars_a, vars_b, schema,
                                                        name_a, name_b,
                                                        past_join_cond_str,
                                                        gid_cursor,
                                                        table_a, table_b)
    conn.commit()
    for g_id in config["ground_truth_ids"]:
        # Read in candidate table for each ground truth ID to save pairs
        matches = match_functions.read_in_pairs_sharing_gid(
            name_a=name_a,
            name_b=name_b,
            gid=g_id,
            cursor=gid_cursor,
            schema=schema)
        conn.commit()
        matches['passnum'] = f'dup_{g_id}'
        # Assign ground truth ids weight to be greater than all other passes
        matches["weight"] = 10 ** (len(config["blocks_by_pass"]) + 1)
        df_sim = pd.concat([df_sim, matches], axis=0)

    # Calculate the counts of each ground truth ID matches by strictness level
    counts = match_functions.calculate_pass_match_counts(df_sim, counts)

    # Store results to temporary file and create new empty dataframe
    df_sim.to_csv("temp_match_gid.csv", index=False)
    df_sim = pd.DataFrame(columns=['indv_id_a', 'indv_id_b', 'idx_a', 'idx_b'])
    match_functions.print_runtime(last_time)

# Start Matching ### ----------------------------------------------------------
# Compare similarities and accept pairs as matches for each pass
print("Starting matching: ", datetime.datetime.now())
counts = run_match_parallelized(
    df_a=df_a,
    df_b=df_b,
    vars_a=vars_a,
    vars_b=vars_b,
    name_a=name_a,
    name_b=name_b,
    config=config,
    past_join_cond_str=past_join_cond_str,
    counts=counts,
    table_a=table_a,
    table_b=table_b,
    conn=conn,
    schema=schema
    )
counts = counts.reset_index(drop=True)

print("Ending matching: ", datetime.datetime.now())
print("All similarity calculation complete.")

### Print match counts ### -------------------------------------------------
gid_passes = ['dup_' + gid for gid in config['ground_truth_ids']]
reg_passes = list(range(len(config['blocks_by_pass'])))
all_passes = gid_passes + reg_passes
# Per pass
for p in all_passes:
    match_functions.print_match_count(counts, passnum=p)
# Entire match
match_functions.print_match_count(counts)

### Save output ###  -------------------------------------------------------
print('Saving output...')
match_functions.save_output(name_a, name_b, config)

print(f'End time: {datetime.datetime.now()}')
