''' Function to accept matches from candidate pairs
by blocking pass based on similarity scores.

This function is called by the main script, match.py.

The acceptance criteria for each pass is included in the
comments preceding each code section below.

'''
import os
import json
import sys

home_dir = os.path.expanduser('~')

CONFIG_FILE = 'config.json' # Filepath to generated match configuration

with open(CONFIG_FILE, "r") as f:
    config = json.loads(f.read())

if config.get("alt_acceptance_dir"):
    sys.path.insert(0, os.path.join(home_dir, config["alt_acceptance_dir"]))
    import accept_functions
else:
    from record_linkage_shared import accept_functions

def is_between(x, a, b):
    return (x >= a) & (x <= b)


def accept_matches(df_match, passnum, config):
    '''
    Evaluate pairs under a given blocking pass based on their similarity scores
    and flag whether they meet the following thresholds:
        - strict match
        - moderate match
        - relaxed match
        - unmatched but might be considered for clerical review
    See in-line comments for specific match criteria.

    Input:
        - df_match: output dataframe from match.py, incl. pair indices and scores
        - passnum: the pass number
        - config (dict)

    Returns the dataframe with four additional boolean columns:
        match_strict, match_moderate, match_relaxed, match_review
    '''
    thresholds = {}
    masks = {}

    ##### Define similarity score cutoffs -------------------------------------
    cutoff_scores = config['cutoff_scores']
    partial_sim = config['sim_param']

    thresholds['name_high_score'] = cutoff_scores.get('name_high_score', 1)
    name_very_high_score = cutoff_scores.get('name_very_high_score', 1)
    thresholds['id_high_score']= cutoff_scores.get('id_high_score', 1)

    # review thresholds to identify unmatched pairs close to cutoff for review
    thresholds['name_review_score'] = cutoff_scores.get('name_review_score', 1)
    id_review_score = cutoff_scores.get('id_review_score', 1)

    ##### Prep ----------------------------------------------------------------
    this_pass = df_match.passnum == passnum

    # create filters for high-similarity common_id so that it will skip
    # if the match does not compare common_id similarities
    if 'common_id' in df_match.columns:
        masks['common_id_null'] = df_match.common_id == -1
        masks['id_high_mask'] = df_match.common_id >= thresholds['id_high_score']
        masks['id_review_mask'] = df_match.common_id >= id_review_score
    else:
        masks['common_id_null'] = True
        masks['id_high_mask'] = False
        masks['id_review_mask'] = False

    # mname filters
    if 'minitial' in df_match.columns:
        minit_unclear_match_score = partial_sim['minitial']['minit_match_mname_unclear']
        masks['minit_match_mname_veryhighsim_mask'] = (
            # minitial is a match and one of them is only one letter, OR
            (df_match.minitial == 1) |
            # minitial is a match and neither of them is only one letter
            # but mname are exact matches or almost exact matches
            (
                (df_match.minitial == minit_unclear_match_score) &
                (df_match.mname >= name_very_high_score)
            )
            )

    # birthdates filters
    if all(x in df_match.columns for x in ['byear', 'bmonthbday']):
        # Define birthday-specific score cutoffs
        thresholds['byear_within1_score'] = partial_sim['byear']['within_1y']
        thresholds['bmonthbday_either_score'] = partial_sim['bmonthbday']['either_month_day']
        thresholds['bmonthbday_inv_score'] = partial_sim['bmonthbday']['swap_month_day']

        masks['dob_exact_mask'] = ((df_match.bmonthbday == 1) & (df_match.byear == 1))
        masks['dob_partial_mask'] = ((
            # bmm matches & bdd matches & byear w/in 1
            (df_match.bmonthbday == 1) & (df_match.byear >= thresholds['byear_within1_score'])
            ) | (
            # byear matches & (one of bmm/bdd matches | bmm bdd are inverted)
            is_between(
                df_match.bmonthbday,
                thresholds['bmonthbday_either_score'],
                thresholds['bmonthbday_inv_score']
                ) & (df_match.byear == 1)
            )
            )

    # location filters
    masks['loc_exact_mask'] = False
    if 'zipcode' in df_match.columns:
        masks['loc_exact_mask'] = df_match.zipcode == 1
    if 'county' in df_match.columns:
        masks['loc_exact_mask'] = (df_match.county == 1) | masks['loc_exact_mask']

    ##### Evaluate and accept -------------------------------------------------
    this_pass = df_match.passnum == passnum
    accepted_in_prev_strictness = False
    for strictness in ('strict', 'moderate', 'relaxed', "review"):
        accept_fn = getattr(accept_functions, f"accept_p{passnum}_{strictness}")
        df_match.loc[this_pass, f"match_{strictness}"] = accept_fn(df_match,
                                                                   masks,
                                                                   thresholds) | \
                                                        accepted_in_prev_strictness
        accepted_in_prev_strictness = df_match.loc[this_pass, f"match_{strictness}"]

    df_match = df_match[df_match.loc[this_pass, 'match_review']]

    return df_match
