'''
Functions to calculate the match rates and statistics for
postprocessed results from the record linkage tool.

These functions are used in the script, get_match_rates.py.

Each function includes a docstring describing its purpose,
inputs and outputs.
'''
import os
import csv
import psycopg2

from record_linkage_shared import match_functions

def get_our_final_matches(filename, matchtype):
    '''
    Pulls our results from postprocessing crosswalk. If dedup,
    returns a list of the groupings found and all individual ids found.
    If 121,12M, or M21, returns dictionary matching id_a to id_b and
    list of id_b seen

    Input:
        filename (str):name of the postprocessing file
        matchtype (str): the matchtype of the postprocessed file
    Returns tuple (dictionary or list, set)
    '''
    our_final_matches = {}
    our_ids = set()
    with open(filename, "r") as f:
        reader = csv.DictReader(f, delimiter=',')
        for line in reader:
            if matchtype == "dedup":
                # Save all ids found in crosswalk, regardless of group size.
                if line["CH_id"] not in our_final_matches:
                    our_final_matches[line["CH_id"]] = set()
                our_final_matches[line["CH_id"]].add(line["orig_id"])
            elif matchtype == "M2M":
                if line["CH_id"] not in our_final_matches:
                    our_final_matches[line["CH_id"]] = set()
                our_final_matches[line["CH_id"]].add(line["indv_id_a"] + "_a")
                our_final_matches[line["CH_id"]].add(line["indv_id_b"] + "_b")
                our_ids.add(line["indv_id_a"] + "_a")
            elif matchtype in ("M21", "121"):
                our_final_matches[line["indv_id_a"]] = line["indv_id_b"]
                our_ids.add(line["indv_id_b"])
            elif matchtype == "12M":
                our_final_matches[line["indv_id_b"]] = line["indv_id_a"]
                our_ids.add(line["indv_id_a"])
    if matchtype == 'dedup':
        # Remove ids that did not match to any other id (sets of size 1).
        our_final_matches = [frozenset(v) for v in our_final_matches.values() if len(v) > 1]
        our_ids = set().union(*our_final_matches)
    return (our_final_matches, our_ids)


def determine_dedup_rates(tot_ids, final_matches, ids):
    '''
    For dedup, calculate the total number of unique individuals found, and
    the percentage of original individuals found to be duplicates.

    Inputs:
        tot_ids (int): The number of original ids
        final_matches (list): list of ids grouped together
        ids (set): Set of all ids seen in the final_matches

    Returns None, prints to screen:
        total unique individuals
        total unmatched individuals
        total matched individuals
        number of individuals found to be duplicates
        percentage of total individuals found to be duplicates
    '''
    unmatched_ids = tot_ids - len(ids)
    total_unique_ids = len(final_matches) + unmatched_ids
    print(f"Final matches had {total_unique_ids} total unique individuals")
    print(f"                  {unmatched_ids} unmatched ids")
    print(f"                  {len(ids)} matched individuals")
    dupes = (tot_ids - (len(final_matches) + unmatched_ids))
    print(f"Final matches found {dupes} to be dupes, or")
    print(f"    {round(dupes * 100 / tot_ids, 2)}% of the original ids\n")


def get_our_raw_matches(filename, matchtype):
    '''
    Reads in our match file, saving all matches found and the matches
    found for this strictness. If dedup, also saves the matches in the
    reverse order in case the match is seen in another file the other way around

    Input:
        filename (str): file path to our match file
        matchtype (str): the matchtype of the postprocessed file
    Returns tuple (dict of sets, dict of dict of sets)
    '''
    all_matches = {}
    strict_matches = {'strict':{},
                      'moderate': {},
                      'relaxed': {},
                      'review': {}}

    with open(filename, "r") as f:
        reader = csv.DictReader(f, delimiter=",")
        for line in reader:
            if matchtype == "dedup" and line["indv_id_a"] > line["indv_id_b"]:
                key_id = line["indv_id_b"]
                val_id = line["indv_id_a"]
            else:
                key_id = line["indv_id_a"]
                val_id = line["indv_id_b"]
            if key_id not in all_matches:
                all_matches[key_id] = set()
            all_matches[key_id].add(val_id)
            for stricttype in strict_matches.keys():
                if line[f"match_{stricttype}"].upper() != "FALSE":
                    if key_id not in strict_matches[stricttype]:
                        strict_matches[stricttype][key_id] = set()
                    strict_matches[stricttype][key_id].add(val_id)
    return (all_matches, strict_matches)


def find_total_ids(database_information, table, id):
    '''
    Finds the number of ids originally in a table
    Takes in the table name in match schema and the id

    Returns int
    '''
    schema = database_information['schema']
    dbname = database_information['dbname']
    host = database_information['host']

    conn = psycopg2.connect(host=host, dbname=dbname)
    cursor = conn.cursor()
    cmd = '''SELECT COUNT(DISTINCT {id})
            FROM {schema}.{table}
            '''.format(id=id,
                schema=schema,
                table=table)
    cursor.execute(cmd)
    total = cursor.fetchone()[0]
    conn.close()
    return total


def calc_metrics_for_threshold(output_dir, table_a, table_b, threshold, matchtype, total):
    '''
    Function to call all functions that calculate metrics for a threshold

    Inputs:
        output_dir (str): directory to find files
        table_a (str): name of table_a
        table_b (str): name of table_b
        threshold (str): strictness level (strict, moderate, relaxed, review)
        matchtype (str): type of match (121, M21, 12M, M2M, dedup)
        total (int): number of ids originally in table

    Returns (list) of our matches
    '''
    print(f"*** CHECKING {threshold.upper()} ***")
    # get our final matches filename
    final_matches_filename = match_functions.get_latest_file_in_dir(
                                                    output_dir + "postprocessing/",
                                                    table_a, threshold,
                                                    table_b)
    # get dict of results and all ids seen
    final_matches, ids = get_our_final_matches(final_matches_filename, matchtype)
    print(f"Total final matches: {len(final_matches)}")

    if matchtype == "dedup":
        determine_dedup_rates(total, final_matches, ids)
    elif matchtype in ("M21", "121", "12M"):
        print(f"The total number of unmatched ids was {total - len(final_matches)}")
        print(f"The match rate was {round(len(final_matches) * 100 / total, 2)}%\n")
    if matchtype not in ("dedup", "M2M"):
        final_matches = final_matches.items()

    return final_matches


### Functions used to compare with BigMatch results during testing ------------

def get_bigmatch_final_matches(filename, matchtype):
    '''
    Pulls the results from the Big Match Crosswalk. If dedup,
    returns a list of the groupings found and all individual ids found.
    If 121 or M21, returns dictionary matching id_a to id_b and
    list of id_b seen (id_b to id_a for 12M)

    Input:
        filename (str):name of the Big Match crosswalk file
        matchtype (str): the matchtype of the postprocessed file
    Returns tuple (list, set)
    '''
    bm_final_matches = {}
    bm_ids = set()

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            if matchtype == "dedup":
                if line[1] not in bm_final_matches:
                    bm_final_matches[line[1]] = set()
                bm_final_matches[line[1]].add(line[0])
                bm_ids.add(line[0])
            elif matchtype == "M2M":
                if line[2] not in bm_final_matches:
                    bm_final_matches[line[2]] = set()
                bm_final_matches[line[2]].add(line[0] + "_a")
                bm_final_matches[line[2]].add(line[1] + "_b")
                bm_ids.add(line[0] + "_a")
            elif matchtype in ("M21", "121"):
                # this ordering might not be true for all BM files
                bm_final_matches[line[2]] = line[1]
                bm_ids.add(line[2])
            elif matchtype == "12M":
                bm_final_matches[line[1]] = line[2]
                bm_ids.add(line[1])
    if matchtype == 'dedup':
        bm_final_matches = [frozenset(v) for v in bm_final_matches.values() if len(v) > 1]
    else:
        bm_final_matches = bm_final_matches.items()
    return (bm_final_matches, bm_ids)


def compare_bm_ours_final_matches(bm_final_matches, our_final_matches, matchtype):
    '''
    Conducts set comparisons of results from Big Match to our
    results
    Inputs:
        bm_final_matches (list): list of Big Match results
            if None, does not complete the Big Match metrics
        our_final_matches (list): dictionary of our results
        matchtype (str): the matchtype of the postprocessed file
    Return:
        None,prints to screen:
            - no of total final_matches in Big Match but not our results
            - no of total final_matches in our results but not Big Match
            - no of total final_matches in both results
            - percent of matching groupings in our results
    '''
    bm_set = set(bm_final_matches)
    ours_set = set(our_final_matches)
    bm_not_ours = bm_set - ours_set
    ours_not_bm = ours_set - bm_set

    print("Final Matches in Big Match not in Ours: ", len(bm_not_ours))
    print("Final Matches in Ours not in Big Match: ", len(ours_not_bm))
    match = bm_set.intersection(ours_set)
    print("Number of Final Matches in both Big Match and Ours: ", len(match))
    print(f"{round(len(match) * 100 /len(our_final_matches), 2)}% of Our Final Matches matched Big Match's")


def find_bm_pairs_lost_in_ours(bm, all_matches, strict_matches, matchtype):
    '''
    Compares the matches seen in Big Match to the matches seen for this level
    of strictness. Prints amount of the matches lost during acceptance.

    Inputs:
        bm (list): the pairs Big Match matched
        all_matches (dict): the pairs we matched
        strict_matches (dict): the pairs we matched for the given strictness
        matchtype (str): the matchtype

    Returns dictionary of pairs lost in acceptance by strictness level
    '''
    accept_loss = {"strict":set(),
                   "moderate":set(),
                   "relaxed":set(),
                   "review":set()}
    for k, v in bm:
        if k in all_matches and v in all_matches[k]:
            for strict in accept_loss.keys():
                if k not in strict_matches[strict] or \
                   v not in strict_matches[strict][k]:
                    accept_loss[strict].add((k, v))
                # elif k not in ours or v != ours[k]:
                #     pp_loss[strict].add((k, v))
    return accept_loss


def get_bigmatch_raw_matches(filename):
    '''
    Reads in the raw matches from the Big Match dat file
    (originally Bigmatch keys.dat file before renamed).
    Specifically used for dedup and not set up to take in other
    keys.dat files

    Input:
        filename (str): path to Big Match match results
    Returns list of pairs
    '''
    bm_pairs = []
    with open(filename, "r") as f:
        for row in f.readlines():
            id_a = row[19:29].strip()
            id_b = row[10:19].strip()
            key = min(id_a, id_b)
            val = max(id_a, id_b)
            bm_pairs.append((key, val))
    return bm_pairs


def calc_metrics_for_bigmatch(bm_name, matchtype, output_dir, table_a, table_b, total):
    '''
    Function to call all functions that calculate metrics for big match, and
    to find the Big Match pairs lost in our match file

    Inputs:
        bm_name (str): string to locate big match files
        matchtype (str): type of match (121, M21, 12M, M2M, dedup)
        output_dir (str): directory to find our raw match file
        table_a (str): name of table_a
        table_b (str): name of table_b
        total (int): number of ids originally in table

    Returns (list, dict) matchings from BM and dictionary of lost pairs, by strictness
    '''
    print("*** CHECKING BIG MATCH ***")
    results = f"/wd2/match/record-linkage-v0/reference_file/{bm_name}_bigmatch_postprocessed_xwalk.csv"
    # get name of our raw matches file
    matchfile = get_latest_file_in_dir(output_dir, table_a, '', table_b)
    # get results from Big Match and our raw matches
    bm_final_matches, bm_ids = get_bigmatch_final_matches(results, matchtype)
    print(f"Total final matches: {len(bm_final_matches)}")
    all_matches, strict_matches = get_our_raw_matches(matchfile, matchtype)

    if matchtype == 'dedup':
        determine_dedup_rates(total, bm_final_matches, bm_ids)
        bm_raw = f"/wd2/match/record-linkage-v0/reference_file/{bm_name}_bigmatch_raw_matches.dat"
        bm_comp = get_bigmatch_raw_matches(bm_raw)

    elif matchtype in ("M21", "121", "12M"):
        print(f"The total number of unmatched ids was {total - len(bm_final_matches)}")
        print(f"The match rate was {round(len(bm_final_matches) * 100 / total, 2)}%\n")
        # don't need big match raw matches for pairs, so use final matches
        bm_comp = bm_final_matches

    accept_loss = find_bm_pairs_lost_in_ours(bm_comp, all_matches, strict_matches,
                                             matchtype)
    return (bm_final_matches, accept_loss)