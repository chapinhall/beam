'''
This module produces text files to review match results.

To run, run the following line from the main directory:
    python3 clerical_review/create_clerical_review_files.py

This script produces 3 text files, for the following threshold groups:
    - strict and moderate
    - moderate and relaxed
    - relaxed and review

For each pass in the match file, the 100 matches with the lowest scores for
the higher threshold and the 100 matches with the highest scores for the lower
threshold are printed, along with a line indicating the cutoff point.

Note that the record linkage module does not use a score cutoff to determine
matches. The clerical review files are not to be used to determine what
score values to cutoff at, but instead ensure that the logic used is correctly
identifying matches.
'''
import os
import csv
import heapq
import glob
import json
import numpy as np
import pandas as pd
from pathlib import Path

from record_linkage_shared import match_functions

CONFIG_FILE = "config.json"

def get_sql_query(schema, name, idxes, name_a, columns):
    '''
    Create the SQL query for the given table

    Inputs:
        schema (str): schema table is stored
        name (str): name of table
        record (int): the index of the wanted record
        common_id (str): column name of the common id
        name_a (str): name of table a in case of dedup
    Returns str'''
    if name == "dedup":
        name = name_a
    cols = ','.join(columns) + ",idx"
    cmd = '''SELECT {cols}
            FROM {schema}.{name}
            WHERE idx in ('{idxes}') '''
    return cmd.format(cols=cols,
                      schema=schema,
                      name=name,
                      idxes="','".join(idxes))


def limit_results(df, strict_1, strict_2, passes):
    '''
    Limits the results to keep only the 100 lowest scoring records of
    strictness 1 and 100 highest scoring records of strictness 2 for
    each pass.

    Inputs:
        df (pd dataframe)
        strict_1 (str): strictness to keep lowest scored results
        strict_2 (str): strictness to keep highest scored results
    Returns dataframe
    '''
    keep = []
    strict_1_results = df[df[f"match_{strict_1}"]]
    strict_2_results = df[(df[f"match_{strict_2}"]) & (~df[f'match_{strict_1}'])]

    for pnum in range(passes):
        if strict_1:
            strict_1_pass = strict_1_results[strict_1_results.passnum == pnum]
            keep.append(strict_1_pass.tail(100))
        if strict_2:
            strict_2_pass = strict_2_results[strict_2_results.passnum == pnum]
            keep.append(strict_2_pass.head(100))

    return pd.concat(keep)


def pull_and_prep_data(strict_1, strict_2, config, name_a, name_b, results):
    '''
    Set up files and dataset to create clerical review files
    Inputs:
        strict_1 (str): the higher strictness level
        strict_2 (str): the lower strictness level
        config (dict): matching specifications
        name_a (str): Name of first dataset
        name_b (str): Name of second dataset
        results (df): Dataframe of the match results

    Returns none, creates txt file
    '''
    # Create subdirectory and output file for clerical reviews
    output_dir = config["output_dir"]
    clerical_review = os.path.join(output_dir, "clerical_review")
    os.makedirs(clerical_review, exist_ok=True)
    output_file = f"{clerical_review}/{name_a}_{name_b}_{strict_1}_{strict_2}.txt"

    # Define number of passes and reduce results to highest and lowest of each pass
    passes = len(config["blocks_by_pass"])
    keep = limit_results(results, strict_1, strict_2, passes)
    keep = keep[["idx_a", "idx_b", "weight", "passnum",
                  f"match_{strict_1}", f"match_{strict_2}"]].reset_index(drop=True)

    # List out indexes needed from each dataset
    idxes_a = keep["idx_a"].to_list()
    idxes_b = keep["idx_b"].to_list()

    # Connect to database and read in the datasets. Rename columns as needed.
    conn, schema, _ = match_functions.connect_to_db(config["database_information"])
    vars_a = config["data_param"]["df_a"]["vars"]
    tbl_name_a = name_a
    if config["data_param"]["df_a"]["filetype"] == "db":
        tbl_name_a =config["data_param"]["df_a"]["db_args"]["tablename"]
    if name_b == "dedup":
        vars_b = vars_a
        tbl_name_b = tbl_name_a
    else:
        vars_b = config["data_param"]["df_b"]["vars"]
        tbl_name_b = name_b
        if config["data_param"]["df_b"]["filetype"] == "db":
            tbl_name_b =config["data_param"]["df_b"]["db_args"]["tablename"]

    a_raw_cols_to_standard = {v:k for k, v in vars_a.items() if k != "minitial"}
    b_raw_cols_to_standard = {v:k for k, v in vars_b.items() if k != "minitial"}
    query_a = get_sql_query(schema, tbl_name_a, idxes_a, tbl_name_a, a_raw_cols_to_standard)
    table_a = pd.read_sql(query_a, conn).rename(columns=a_raw_cols_to_standard)
    query_b = get_sql_query(schema, tbl_name_b, idxes_b, tbl_name_a, b_raw_cols_to_standard)
    table_b = pd.read_sql(query_b, conn).rename(columns=b_raw_cols_to_standard)

    # Merge match data to each dataset
    merge_a = pd.merge(keep, table_a, left_on="idx_a", right_on="idx"
                       ).sort_values(["idx_a", "idx_b"]).reset_index(drop=True)
    merge_b = pd.merge(keep, table_b, left_on="idx_b", right_on="idx"
                       ).sort_values(["idx_a", "idx_b"]).reset_index(drop=True)

    # Concatenate dataset, sorting on index to maintain order, and restrict
    # only to columns seen in both datasets
    cols_a = merge_a.columns
    cols_b = merge_b.columns
    merge_a.index = merge_a.index.astype(str) + "a"
    merge_b.index = merge_b.index.astype(str) + "b"
    cols = [a for a in cols_a if a in cols_b and a not in ('idx', 'minitial')]
    merge_dataset = pd.concat([merge_a, merge_b]).sort_index()
    merge_dataset.index.name = "index"
    merge_dataset = merge_dataset[cols]
    merge_dataset["passnum"] = merge_dataset["passnum"].astype(float)

    conn.close()
    return (merge_dataset, output_file)


def create_file_for_review(merge_dataset, output_file, strict_1, strict_2):
    '''
    Creates clerical review files for the given strictness levels

    Inputs:
        merge_dataset (df): dataset containing match results w/ col values
        output_file (str): where to store final clerical review
        strict_1 (str): the higher strictness level
        strict_2 (str): the lower strictness level

    Returns none, creates txt file
    '''
    # For each comparison column, set col width to max string length seen
    comp_cols = [x for x in merge_dataset.columns if x not in ("idx_a", "idx_b", "weight")
                 and not x.startswith("match_")]
    maxes = merge_dataset[comp_cols].astype(str).apply(lambda s: s.str.len()).max()
    maxes = {col: max(len(col), v) for (col, v) in maxes.items()}
    s = pd.DataFrame([[''] * len(merge_dataset.columns)], columns=merge_dataset.columns) # DataFrame to create space between results
    for col, length in maxes.items():
        merge_dataset[col] = merge_dataset[col].astype(str).str.ljust(length)
        s[col] = s[col].str.ljust(length)
    f = lambda d: pd.concat([d, s], ignore_index=True)

    # Split dataset into record for strict_1 and records for strict_2
    matched_records = merge_dataset[merge_dataset[f"match_{strict_1}"]]
    unmatched_records = merge_dataset[~merge_dataset[f"match_{strict_1}"]]
    max_strictness = max(len(strict_1), len(strict_2)) # To use for ljust on strictness

    # Iterate over passes
    passes = merge_dataset["passnum"].unique()
    passes.sort()
    with open(output_file, "w") as w:
        for p in passes:
            w.write(f'{"-"*74} PASS {str(p)[:1]} {"-"*74}\n')
            # Select wanted rows from strict_1 matches
            r_1 = matched_records[matched_records.passnum == p].sort_values(["weight",
                                                                            "idx_a",
                                                                            "idx_b",
                                                                            "index"],
                                                                            ascending=[
                                                                                False,
                                                                                False,
                                                                                False,
                                                                                True])
            # Replace every 2nd index with pass/strictness info
            r_1.iloc[1::2, r_1.columns.get_loc("idx_a")] = f"Pass {str(p)[:1]}"
            r_1.iloc[1::2, r_1.columns.get_loc("idx_b")] = strict_1.ljust(max_strictness)
            r_1["weight"] = r_1.weight.astype(str).str[:8].str.ljust(8)

            # Repeat above for strict_2 matches
            r_2 = unmatched_records[unmatched_records.passnum == p].sort_values(["weight",
                                                                                "idx_a",
                                                                                "idx_b",
                                                                                "index"],
                                                                                ascending=[
                                                                                    False,
                                                                                    False,
                                                                                    False,
                                                                                    True])
            r_2.iloc[1::2, r_2.columns.get_loc("idx_a")] = f"Pass {str(p)[:1]}"
            r_2.iloc[1::2, r_1.columns.get_loc("idx_b")] = strict_2.ljust(max_strictness)
            r_2["weight"] = r_2.weight.astype(str).str[:8].str.ljust(8)

            # For both dataframes, insert a blank row between every pair of matches
            grp = np.arange(len(r_1)) // 2
            r_1 = r_1.groupby(grp, group_keys=False).apply(f).reset_index(drop=True)
            grp = np.arange(len(r_2)) // 2
            r_2 = r_2.groupby(grp, group_keys=False).apply(f).reset_index(drop=True)

            # Write the strict_1 dataframe to the output file
            w.write(r_1.drop(["passnum",
                             f"match_{strict_1}",
                             f"match_{strict_2}"],
                             axis=1).to_string(index=False, justify="left"))
            w.write(f'\n{"-"*69} THRESHOLD CUTOFF {"-"*69}\n\n')
            # If there are results for the strict_2 matches, write to output file
            if not r_2.empty:
                w.write(r_2.drop(["passnum",
                                 f"match_{strict_1}",
                                 f"match_{strict_2}"],
                                 axis=1).to_string(index=False, justify="left", header=False))
            w.write("\n")
    print("Clerical review saved to:", output_file)


if __name__ == "__main__":
    with open(CONFIG_FILE, "r") as f:
        config = json.loads(f.read())

    # Set variables.
    matchtype = config["matchtype"]
    name_a = config["data_param"]["df_a"]["name"]
    if matchtype == "dedup":
        name_b = "dedup"
    else:
        name_b = config["data_param"]["df_b"]["name"]

    # Find, sort and read in latest match file
    directory = config["output_dir"]
    match_file = match_functions.get_latest_file_in_dir(directory=directory,
                                                        table_a=name_a,
                                                        table_b=name_b)
    results = pd.read_csv(match_file,
                          dtype={"idx_a": str,
                                 "idx_b": str}
                          ).fillna(False).sort_values(["weight", "fname", "lname"],
                                                      ascending=False)
    # Only keep first case of two source IDs matching
    results.drop_duplicates(["indv_id_a", "indv_id_b"], inplace=True)

    # Create clerical review between each strictness level
    strictnesses = ["strict", "moderate", "relaxed", "review"]
    for i, strict in enumerate(strictnesses[:-1]):
        strict_2 = strictnesses[i + 1]
        print(f"Running clerical review for: {strict}, {strict_2}")
        merge_dataset, output_file = pull_and_prep_data(strict, strict_2, config,
                                                        name_a, name_b, results)
        create_file_for_review(merge_dataset, output_file, strict, strict_2)