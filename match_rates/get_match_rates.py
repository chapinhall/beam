'''
Comparison script that compares results from Big Match to those of our
Record Linkage tool.

To run:
    python3 get_match_rates.py -bm
    where bm is an optional flag on whether to also compare results
        to Big Match files

The script will grab the most recent raw matches files, if there are multiple
versions, for the match specified by the config_input.

Prints match stats for each threshold level (strict, moderate, relaxed, review)
to screen. For all, prints:
    Total number of original ids (table a for 121, M21, M2M and dedup,
                                  table b for 12M)
    Total pairs/groupings

For dedup, prints:
    Total unique individuals
        Number of unmatched ids
        Number of matched individuals
    The number of ids found to be duplicates
        the percentage of duplicate ids

For 121, 12M or M21, prints:
    The total number of unmatched ids
    The match rates (total pairs / # of original ids)

If comparing with Big Match, also prints:
    # of groups/pairs in Big Match but not Ours
    # of groups/pairs in Ours but not Big Match
    # of groups/pairs found in both
    % of our groups/pairs that matched Big Match
    # pairs lost by threshold level

Currently has not been updated with specific stats for M2M
'''
import argparse
import json
from record_linkage_shared.match_rates_functions import (find_total_ids,
                                                calc_metrics_for_bigmatch,
                                                calc_metrics_for_threshold,
                                                compare_bm_ours_final_matches)

CONFIG_FILE = "config.json"

# Get arguments
parser = argparse.ArgumentParser(description='Get stats of results')
parser.add_argument('-bm', action='store_true',
                    help='Flag to include Big Match results/comparisons')
args = parser.parse_args()

# Set up variables
THRESHOLDS = ["strict", "moderate", "relaxed", "review"]

with open(CONFIG_FILE, 'r') as json_file:
    config = json.load(json_file)

data_param = config["data_param"]
output_dir = config["output_dir"]

name_a = data_param["df_a"]["name"]
name_b = ''

if data_param["df_a"]["filetype"] == "db":
    table_a = data_param["df_a"]["db_args"]["tablename"]
else:
    table_a = name_a
id_a = data_param["df_a"]["vars"]["indv_id"]
matchtype = config["matchtype"]

if data_param["df_b"]:
    name_b = data_param["df_b"]["name"]
    if data_param["df_b"]["filetype"] == "db":
        table_b = data_param["df_b"]["db_args"]["tablename"]
    else:
        table_b = name_b
    id_b = data_param["df_b"]["vars"]["indv_id"]
else:
    table_b = None
    id_b = None

database_information = config['database_information']

# Get amount of original ids for later calculations
if matchtype == "12M":
    total = find_total_ids(database_information, table_b, id_b)
else:
    total = find_total_ids(database_information, table_a, id_a)

print(f"There were originally {total} ids\n")

# If comparing big match, get final matches for Big Match and BM pairs lost
if args.bm:
    if matchtype == "dedup":
        bigmatch_name = name_a
    else:
        bigmatch_name = name_a + "-" + name_b
    bm_final_matches, accept_loss = calc_metrics_for_bigmatch(bigmatch_name,
                                                              matchtype,
                                                              output_dir,
                                                              name_a, name_b,
                                                              total)


# print stats for each threshold level
for threshold in THRESHOLDS:
    final_matches = calc_metrics_for_threshold(output_dir,
                                               name_a, name_b,
                                               threshold, matchtype,
                                               total)
    if args.bm:
        # If comparing with Big Match, print no. of pairs lost & comparisons
        compare_bm_ours_final_matches(bm_final_matches,
                                      final_matches,
                                      matchtype)
        print("Raw pairs in Big Match lost in acceptance:" +
              f" {len(accept_loss[threshold])}\n")
