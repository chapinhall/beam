'''
Postprocessing script

This script runs postprocessing for a match. It should be run after
running match.py.

The script reads in the filepath to a csv file of raw pairs (the output from
running match.py) and the type of match to determine
the number of final matches found in the raw pairs. The logic of selecting
best match follows the logic outlined here:
https://chapinhall.sharepoint.com/:w:/s/ETL/Ef35daXBSfFAucfuUpT92cYB8Rf0KIT5FaS7PmlEMk9SiA?e=81ltxe

All of the following outputs are saved in:
    /wd2/match/record-linkage-v0/output_data/postprocessing/

Usage:
    python3 postprocess.py

The matchtype is set in the configuration, where matchtype is either:
    121: each record from file A can match to at most one and only one
         record from file B. Output is a crosswalk csv matching id a to
         id b, along with the pass number where id a was matched to id b.
    12M: a record from file A can match to multiple records in file B
         but a record from file B can match to at most one record from
         file A. Output is a crosswalk csv matching id a to id b, where
         id b is only seen once, and the pass number where id a was
         matched to id b.
    M21: a record from file B can match to multiple records in file A
         but a record from file A can match to at most one record from
         file B. Output is a crosswalk csv matching id a to id b, where
         id a is only seen once, and the pass number where id a was
         matched to id b.
    M2M: each record from file A is allowed to match to multiple records
         from file B and vice versa. A new CH_ID is created to identify
         the unique individuals. Output is a crosswalk csv matching each
         matched pair from the matching file to a new CH_ID, along with
         the pass number the pair was matched in.
    dedup: a single file is used, where each id in the file is allowed
         to match to any other id in the file. A new CH_ID is created to
         identify the unique individuals. Output is a crosswalk csv
         matching each id seen in the match file to a new CH_ID
         (no pass number saved)

If the matchtype is 121, 12M or M21, a sorted file is also created that
sorts the matches based on the calculated weights
'''
import re
import glob
import heapq
import csv
import json
import psycopg2
import os
import record_linkage_shared.union_find


CONFIG_FILE = "config.json"


def one_to_one_matching(filename, strictness=None):
    '''
    Creates a 1-to-1 matching of a given file. Only the pairs with the
    highest weights are considered. If an index matches with two different
    indexes with the same weight, neither match is taken and the index is
    unmatched.

    Inputs:
        filename (str) name of file to be post-processed, with at least the
            columns: indv_id_a, indv_id_b and weight
        strictness (str) name of column in file with boolean to determine
            whether a pair in the file should be accepted.
            If None, all pairs in file considered

    Returns dictionary matching "indv_id_a" values to a tuple of the indv_id_b,
        weight, a delete marker, and pass number the match was from
    '''
    idx_match_a_to_b = {}
    idx_match_b_to_a = {}
    with open(filename, "r") as input:
        reader = csv.DictReader(input, delimiter=',')
        for line in reader:
            if strictness is None or line[strictness].upper() != "FALSE":
                a, b, weight = (line['indv_id_a'], line['indv_id_b'],
                                line['weight'])
                # weight is the same of previous match
                if ((a in idx_match_a_to_b and
                     weight == idx_match_a_to_b[a][1]) or
                   (b in idx_match_b_to_a and
                    weight == idx_match_b_to_a[b][1])):
                    if a in idx_match_a_to_b and b != idx_match_a_to_b[a][0]:
                        del_b = idx_match_a_to_b[a][0]
                        idx_match_a_to_b[a][2] = True
                        if idx_match_b_to_a[del_b][0] == a:
                            idx_match_b_to_a[del_b][2] = True
                    elif a not in idx_match_a_to_b:
                        idx_match_a_to_b[a] = [b, weight, True,
                                               line['passnum']]
                    if b in idx_match_b_to_a and a != idx_match_b_to_a[b][0]:
                        del_a = idx_match_b_to_a[b][0]
                        if idx_match_a_to_b[del_a][0] == b:
                            idx_match_a_to_b[del_a][2] = True
                        idx_match_b_to_a[b][2] = True
                    elif a not in idx_match_b_to_a:
                        idx_match_b_to_a[b] = [a, weight, True,
                                               line['passnum']]
                # match should be added
                elif (a not in idx_match_a_to_b and b not in idx_match_b_to_a):
                    idx_match_a_to_b[a] = [b, weight, False, line['passnum']]
                    idx_match_b_to_a[b] = [a, weight, False, line['passnum']]
    return idx_match_a_to_b


def mone_or_onem_matching(filename, strictness=None, onecol="indv_id_a"):
    '''
    Creates a 1-to-many or many-to-one matching of a given file. Only the
    pairs with the highest weights are considered. If a many index matches
    with two different one-indexes with the same weight, neither match is
    taken and the many index is unmatched.

    Inputs:
        filename (str) name of file to be post-processed, with at least the
            columns: indv_id_a, indv_id_b and weight
        strictness (str) name of column in file with boolean to determine
            whether a pair in the file should be accepted.
            If None, all pairs in file considered
        onecol (str) the name of the column with the one ids, defaults to
            "indv_id_a"

    Returns dictionary matching the many indexes to a tuple of the one-id,
        weight, a delete marker, and pass number the match was from
    '''
    if onecol == 'indv_id_a':
        manycol = 'indv_id_b'
    else:
        manycol = 'indv_id_a'
    idx_match_many_to_one = {}
    with open(filename, 'r') as input:
        reader = csv.DictReader(input, delimiter=',')
        for line in reader:
            if strictness is None or line[strictness].upper() != "FALSE":
                one = line[onecol]
                many = line[manycol]
                if (many in idx_match_many_to_one and
                    line['weight'] == idx_match_many_to_one[many][1] and
                    one != idx_match_many_to_one[many][0]):
                    idx_match_many_to_one[many][2] = True
                elif many not in idx_match_many_to_one:
                    idx_match_many_to_one[many] = [one, line['weight'], False,
                                                   line['passnum']]
    return idx_match_many_to_one


def mtom_or_dedup_matching(filename, matchtype, strictness=None, config=None):
    '''
    Creates a many-to-many or dedup matching of a given file.

    Inputs:
        filename (str) name of file to be post-processed, with at least the
            columns: indv_id_a and indv_id_b
        matchtype (str): whether the match is a dedup or M2M
        strictness (str) name of column in file with boolean to determine
            whether a pair in the file should be accepted.
            If None, all pairs in file considered
    Returns:
        dictionary of item/new ids. If dedup, the dictionary matches
        each id seen to a new id. If M2M, the dictionary matches the pair
        of ids matched to their corresponding new id, along with the pass
        the pair was matched in
    '''
    unionfind = record_linkage_shared.union_find.UnionFind()
    rowid = unionfind.add_csv(filename, matchtype, strictness)
    if matchtype == "dedup" and config:
        df_a_info = config["data_param"]["df_a"]

        indv_id_a = df_a_info['vars']["indv_id"]
        table_a = df_a_info["name"]

        schema = config['database_information']["schema"]
        dbname = config["database_information"]["dbname"]
        host = config["database_information"]["host"]

        conn = psycopg2.connect(host=host, dbname=dbname)
        cursor = conn.cursor()
        cmd = f'''SELECT distinct {indv_id_a} from {schema}.{table_a}'''
        cursor.execute(cmd)
        all_ids = cursor.fetchall()

        for id_a in all_ids:
            unionfind.add_item_dedup(rowid, id_a[0])
            rowid += 1
    return unionfind.group_to_item_set


def write_to_csv(matches, filename, matchtype, key_flag=0):
    '''
    Writes a dictionary of matches to csv

    Inputs:
        matches (dict) dictionary of the matches found
        filename (str) filename the csv will be written to
        matchtype (str) the type of match in order to parse the
            matches dictionary correctly
        key_flag (int) in the dictionary, whether the key or value
            is the column "indv_id_a". Only necessary for M21 matches

    Returns: None, writes to csv
    '''
    weights = matchtype in ("121", "12M", "M21")
    with open(filename, 'w') as f:
        csvwriter = csv.writer(f)
        if weights:
            # no new ids are created, id_a's are only matched to id_b's
            csvwriter.writerow(["indv_id_a", "indv_id_b", "passnum"])
        elif matchtype == "dedup":
            # each id is matched to a new id
            csvwriter.writerow(["orig_id", "CH_id"])
        else:
            # pairs of ids are matched to a new id
            csvwriter.writerow(["indv_id_a", "indv_id_b", "CH_id", "passnum"])
        for i, match in enumerate(matches.items()):
            id_1, row = match
            if type(row) != int:
                match = list(row)
            if weights and not match[2]:
                # check if match was deleted
                passnum = match[3]
                match = [id_1, match[0]]
                csvwriter.writerow([match[key_flag],
                                    match[abs(key_flag - 1)],
                                    passnum])
            elif matchtype in ("M2M", "dedup"):
                for match_ids in row:
                    if type(match_ids) == tuple and matchtype == "M2M":
                        # write each pair in list to csv with the new id
                        csvwriter.writerow([match_ids[0][2:],
                                            match_ids[1][2:],
                                            i, id_1])
                    elif matchtype == "dedup":
                        csvwriter.writerow([match_ids, i])


if __name__ == "__main__":
    # load in configuration file
    with open(CONFIG_FILE, "r") as f:
        config = json.loads(f.read())

    # Set variables.
    matchtype = config["matchtype"]
    paths = []
    table_a = config["data_param"]["df_a"]["name"]
    if matchtype == "dedup":
        table_b = "dedup"
    else:
        table_b = config["data_param"]["df_b"]["name"]

    # Find most recent match results.
    match_format = f"match_results_with_pairwise_scores_{table_a}_{table_b}_\d{{4}}-\d{{2}}-\d{{2}}.csv"
    for basename in os.listdir(config["output_dir"]):
        if re.match(match_format, basename):
            paths.append(os.path.join(config["output_dir"], basename))
    filename = max(paths, key=os.path.getctime)

    # Create post-processing subdirectory to save results
    save_file = os.path.basename(filename)
    postproc_dir = os.path.join(config['output_dir'], "postprocessing")
    if not os.path.isdir(postproc_dir):
        os.makedirs(postproc_dir)

    # Flag for whether indv_id_a will be the key of the dictionary or not
    key_flag = 0

    # Run postprocessing for each strictness level.
    for strictness in ("strict", "moderate", "relaxed", "review"):
        strict = "match_" + strictness
        sfile_short = save_file.replace(
            'match_results_with_pairwise_scores', 'final_xwalk'
            ).replace('.csv', '')
        sfile_w_strictness = os.path.join(config["output_dir"],
                                          "postprocessing",
                                          f"{sfile_short}_{strictness}.csv")
        if strictness == "review":
            strict = None
        if matchtype == '121':
            matches = one_to_one_matching(filename, strict)
        elif matchtype == '12M':
            matches = mone_or_onem_matching(filename, strict, "indv_id_a")
            key_flag = 1
        elif matchtype == 'M21':
            matches = mone_or_onem_matching(filename, strict, "indv_id_b")
        elif matchtype in ('M2M', 'dedup'):
            matches = mtom_or_dedup_matching(filename, matchtype,
                                             strict, config)
        write_to_csv(matches, sfile_w_strictness, matchtype, key_flag)
        print(f"matching for {strictness} completed.")
