'''
Helper functions for match.py, including:

1. Customized similarity metrics (comparison algorithms)
2. Functions for running match in parallelization
3. Other utility functions for the match process, such as loading input, saving
    output, and printing terminal statements.

'''
import time
import gc
import os
import csv
import glob
import json
import heapq
import psutil
import psycopg2
import itertools
import multiprocessing
import recordlinkage
import numpy as np
import pandas as pd
from datetime import date
from recordlinkage.compare import Exact, String, Numeric, Date
from recordlinkage.base import BaseCompareFeature
from functools import partial
from sqlalchemy import create_engine

from record_linkage_shared import accept
from record_linkage_shared import block_functions



### Customized similarity metrics (comparison algorithms) ---------------------

# based on https://recordlinkage.readthedocs.io/en/latest/ref-compare.html#user-defined-algorithms

class CompareByear(BaseCompareFeature):

    def __init__(self, left_on, right_on, within_year_dif, missing_value, year_dif,
        *args, **kwargs):

        super().__init__(left_on, right_on, *args, **kwargs)

        self.within_year_dif = within_year_dif
        self.missing_value = missing_value
        self.year_dif = year_dif

    def _compute_vectorized(self, s1, s2):
        '''Compare byear and assign scores to these tiers:
            - byear is exact match (sim = 1)
            - byear is within one
            - byear has missing values
            - all else (sim = 0)
        '''
        sim = (s1 == s2).astype(float)
        sim[(sim == 0) & (abs(s1 - s2) <= self.year_dif)] = self.within_year_dif
        sim[(sim == 0) & (np.isnan(s1) | np.isnan(s2))] = self.missing_value

        return sim


class CompareBmonthBday(BaseCompareFeature):

    def __init__(self, left_on, right_on, swap_month_day, either_month_day,
        missing_value, *args, **kwargs):

        super().__init__(left_on, right_on, *args, **kwargs)

        self.swap_month_day = swap_month_day
        self.either_month_day = either_month_day
        self.missing_value = missing_value

    def _compute_vectorized(self, s1_m, s1_d, s2_m, s2_d):
        '''Compare bmonth, bday and assign scores to these tiers:
            - bmonth and bday are exact match (sim = 1)
            - bmonth and bday are inverted
            - one of bmonth and bday is a match
            - one of bmonth and bday is missing
            - all_else (sim = 0)
        '''
        sim = ((s1_m == s2_m) & (s1_d == s2_d)).astype(float)
        sim[(sim == 0) & (s1_d == s2_m) & (s1_m == s2_d)] = self.swap_month_day
        sim[(sim == 0) & ((s1_m == s2_m) | (s1_d == s2_d))] = self.either_month_day
        sim[(sim == 0) & (
            np.isnan(s1_m) | np.isnan(s1_d) |
            np.isnan(s2_m) | np.isnan(s2_d))] = self.missing_value

        return sim


class CompareMinitial(BaseCompareFeature):

    def __init__(self, left_on, right_on, minit_match_mname_unclear,
        missing_value, *args, **kwargs):

        super().__init__(left_on, right_on, *args, **kwargs)
        self.minit_match_mname_unclear = minit_match_mname_unclear
        self.missing_value = missing_value

    def _compute_vectorized(self, s1_minit, s1_mname, s2_minit, s2_mname):
        '''Compare minitial following the logic:
            - minitial is a good quality match (sim = 1)
                i.e. minitial matches and one of mname is one single letter
            - minitial is a questionable quality match (minit_match_mname_unclear)
                i.e. minitial matches but neither of manme is one single letter
            - one of minitial is missing
            - minitial does not match (sim = 0)

        '''
        # good minit match
        sim = ((s1_minit == s2_minit) &
            ((s1_mname.str.len() == 1) | (s2_mname.str.len() == 1))
            ).astype(float)

        # questionable init match
        sim[(sim == 0) & (s1_minit == s2_minit)] = self.minit_match_mname_unclear

        sim[(sim == 0) & (
            s1_minit.isnull() | s2_minit.isnull())] = self.missing_value

        return sim


### Functions for running match -----------------------------------------------

def run_match_parallelized(df_a, df_b, vars_a, vars_b, name_a, name_b,
    config, past_join_cond_str, counts, table_a, table_b, conn, schema):
    '''
    Run the matching algorithm (including blocking) for each pass number,
    using parallelization to divide the work. Takes in the two dataframes,
    their variables, and the configuration for the match. For each pass,
    blocking is run in Postgres, and a table containing candidate pairs for
    that pass is created (named using the dataframes' names in config).
    Then, by a set chunk size, each pair of candidates in that pass is
    compared. The similarity score results are appended to df_sim, the output
    dataframe, and stored in temporary csvs.

    Inputs:
        - df_a, df_b: (pandas dataframes) original datasets to compare
        - vars_a, vars_b: (dict) mappings of shared names of blocking variables
            to the column name in each dataset. Pulled from config['data_param']
        - df_sim: (Pandas DataFrame) dataset for storing calculated similarity
            scores and corresponding inputs
        - name_a, name_b: (string) names of the dataframes
        - config: (dict) Match specifications
        - past_join_cond_str: (str) string of past join combinations
        - counts: (df) dataframe of number of matches in each pass
        - table_a: (str) database table name of df_a
        - table_b: (str) database table name of df_b
        - conn (psycopg2 connection obj): connection to database
        - schema (str): schema database tables are stored in

    Returns updated counts
    '''
    blank_df = pd.DataFrame(columns=['idx_a', 'idx_b'])
    # create dictionary of (comparer name: comparer object) for calculating
    # similarities based on comparers listed in config
    comps = prepare_comparers(vars_a=vars_a, vars_b=vars_b, config=config)

    # set up process pool and DB connection
    chunk_sizes = config['parallelization_metrics']['chunk_sizes']
    num_processes = config['parallelization_metrics']['num_processes']
    process_pool = multiprocessing.Pool(processes=num_processes)
    run_match = partial(
        run_match_for_candidate_set,
        df_a=df_a,
        df_b=df_b,
        df_sim=blank_df,
        std_varnames=list(vars_a),
        comps=comps,
        config=config
        )
    cursor = conn.cursor()
    cursor.execute(f'SET ROLE {schema}admin;')

    output_vars = ["indv_id_a", "indv_id_b", "idx_a", "idx_b",
                   "passnum", "match_strict", "match_moderate",
                   "match_relaxed", "match_review", "weight"]
    for comp_vars in config["comp_names_by_pass"]:
        for var in comp_vars:
            if var not in output_vars:
                output_vars.append(var)

    # Run match in parallel
    all_cands = []
    tot_pass_cnt = len(config['blocks_by_pass'])
    last_time = time.time()
    i = 0
    for passnum in range(tot_pass_cnt):
        # Complete blocking for pass
        past_join_cond_str  = block_functions.run_blocking_pass(
                                                config["blocks_by_pass"],
                                                passnum,
                                                vars_a, vars_b,
                                                schema,
                                                name_a, name_b,
                                                past_join_cond_str,
                                                cursor,
                                                table_a, table_b)
        cand_table = f"candidates_{name_a}_{name_b}_p{passnum}"
        print(f'Looking up {cand_table}...')
        # Check if this blocking pass is skipped
        cursor.execute(
            f'''SELECT EXISTS (SELECT * FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{cand_table}');''')
        if not cursor.fetchone()[0]: # Table not found
            print(f'No candidate table for pass {passnum}, skipping')
            continue
        conn.commit()

        # Read in match pairs and complete similarity checks
        with conn.cursor(f"passes_cursor_{passnum}") as cur:
            cmd = f"SELECT indv_id_a, indv_id_b, idx_a, idx_b FROM {schema}.{cand_table}"
            cur.execute(cmd)
            # Read in block by chunks, running similiarity check once threshold hit
            while chunk := cur.fetchmany(size=chunk_sizes[str(passnum)]):
                if not chunk:
                    break
                candidates = pd.DataFrame(chunk,
                                          columns=['indv_id_a', 'indv_id_b',
                                                   'idx_a', 'idx_b'])
                all_cands.append([candidates, passnum])
                if len(all_cands) == num_processes * 2:
                    dfs = process_pool.starmap(run_match, tuple(all_cands))
                    dfs = pd.concat(dfs, ignore_index=True)
                    counts = calculate_pass_match_counts(dfs, counts)
                    calculate_weights(dfs, tot_pass_cnt)
                    # Save out sorted dataframe to temp file
                    dfs = dfs.sort_values("weight", ascending=False)
                    dfs.reindex(columns=output_vars).to_csv(f"temp_match_{i}.csv",
                                                            index=False)
                    all_cands = []
                    i += 1
                    gc.collect()
        # Drop candidates table for current pass once done using
        cmd = f'''DROP TABLE IF EXISTS {schema}.{cand_table}'''
        cursor.execute(cmd)
        conn.commit()

    # Compute similarity check for any remaining candidates not yet processed
    if all_cands:
        dfs = process_pool.starmap(run_match, tuple(all_cands))
        dfs = pd.concat(dfs, ignore_index=True)
        counts = calculate_pass_match_counts(dfs, counts)
        calculate_weights(dfs, tot_pass_cnt)
        dfs = dfs.sort_values("weight", ascending=False)
        dfs.reindex(columns=output_vars).to_csv(f"temp_match_{i}.csv", index=False)
        all_cands = []
        gc.collect()
    conn.close()
    print_runtime(last_time)

    print("All passes completed.")
    last_time = time.time()
    print_runtime(last_time)
    process_pool.close()
    gc.collect()

    return counts


def run_match_for_candidate_set(candidates, passnum, df_a, df_b, df_sim,
    std_varnames, comps, config):
    '''
    Flag matches for a set of candidates in a certain pass.
    This function calculates and evaluates the similarity scores to
    accept matches. Return the similarity scores and whether the candidates
    are accepted as a match.

    Inputs:
        - candidates: (pandas Dataframe) dataframe of indexes to compare
        - passnum: (int) current pass
        - df_a, df_b: (pandas Dataframe) original datasets to compare
        - df_sim: (pandas Dataframe) dataset to store results
        - std_varnames: (list) standardized variable names in input data
        - comps: (dict) mapping of comparison name/label (key) to comparer
                objects (value), returned by prepare_comparers function

    Returns updated df_sim
    '''
    # Find matching candidates for this pass based on blocking variables
    if (passnum == 0) & (len(config['comp_names_by_pass'][0]) == 0):
        # All candidates are exact matches, no comparison needed
        matches = candidates
        matches['passnum'] = passnum
        df_sim = pd.concat([df_sim, matches], axis=0)
    else:
        # Calculate similarities for comparison variables
        cands = pd.MultiIndex.from_frame(candidates[["idx_a", "idx_b"]])
        comp_names = config['comp_names_by_pass'][passnum]
        # Remove any comparisons listed in config but not in input data
        valid_comp_names = get_valid_comp_names(std_varnames)
        valid_comp_names_for_pass = [c for c in comp_names
            if c in valid_comp_names]

        df_sim = calc_similarities(
            df_a=df_a,
            df_b=df_b,
            passnum=passnum,
            candidates=cands,
            comp_names=valid_comp_names_for_pass,
            comps=comps
            )
        df_sim = pd.merge(
            candidates,
            df_sim,
            how='left',
            left_on=['idx_a', 'idx_b'],
            right_on=['idx_a', 'idx_b']
            )
    df_sim = accept.accept_matches(df_sim, passnum, config)
    return df_sim


def read_in_pairs_sharing_gid(name_a, name_b, gid, cursor, schema):
    '''
    For the ground truth ids, find the corresponding block on postgres
    and pull the matching candidates to add to the similarities dataframe

    Inputs:
        - name_a, name_b: (string) name of datasets
        - gid: (string) name of ground truth id
        - config: (dict)

    Returns pandas dataframe
    '''
    cursor.execute(f'SET ROLE {schema}admin')
    cmd = f'''
        SELECT indv_id_a, indv_id_b, idx_a, idx_b, 'dup_{gid}', TRUE, TRUE, TRUE, TRUE
        FROM {schema}.candidates_{name_a}_{name_b}_matching_{gid}
        '''
    cursor.execute(cmd)
    candidates = pd.DataFrame(
        cursor.fetchall(),
        columns=["indv_id_a", "indv_id_b", "idx_a", "idx_b", "passnum",
                 "match_strict", "match_moderate", "match_relaxed", "match_review"]
        )
    candidates_table = f'{schema}.candidates_{name_a}_{name_b}_matching_{gid}'
    cmd = '''DROP TABLE IF EXISTS {}'''.format(candidates_table)
    cursor.execute(cmd)
    return candidates


def prepare_comparers(vars_a, vars_b, config):
    '''Create recordlinkage comparer objects for all comparison variables
    listed in config['comp_names_by_pass']

    Returns: A dictionary.
        key: (str) shared comparison variable name
        value: record linkage comparer object
    '''
    sim_param = config['sim_param']
    # get all comparison variables
    all_comp_names = set(itertools.chain(*config['comp_names_by_pass']))
    valid_comp_names = get_valid_comp_names(std_varnames=list(vars_a))
    comps = {}
    for cn in all_comp_names:
        # skip if comparison variable is not included in input data
        if cn not in valid_comp_names:
            continue
        # jaro-winkler comparisons
        if sim_param[cn]['comparer'] == 'jarowinkler':
            comps[cn] = String(
                vars_a[cn], vars_b[cn],
                method='jarowinkler',
                missing_value=sim_param[cn]['missing_value'],
                label=cn
                )
        # inverted fname/lname jaro-winkler comparisons
        elif sim_param[cn]['comparer'] == 'inv_jarowinkler':
            va = cn[:5]
            vb = cn[5:]
            comps[cn] = String(
                vars_a[va], vars_b[vb],
                method='jarowinkler',
                missing_value=sim_param[cn]['missing_value'],
                label=cn)
        # custom minitial comparisons (no missing value, they are captured in mname_jw)
        elif sim_param[cn]['comparer'] == 'minitial':
            comps[cn] = CompareMinitial(
                (vars_a['minitial'], vars_a['mname']),
                (vars_b['minitial'], vars_b['mname']),
                minit_match_mname_unclear=sim_param[cn]['minit_match_mname_unclear'],
                missing_value=sim_param[cn]['missing_value'],
                label=cn
                )
        # custom birthdate component comparisons
        elif sim_param[cn]['comparer'] == 'bmonthbday':
            comps[cn] = CompareBmonthBday(
                (vars_a['bmonth'], vars_a['bday']),
                (vars_b['bmonth'], vars_b['bday']),
                swap_month_day=sim_param[cn]['swap_month_day'],
                either_month_day=sim_param[cn]['either_month_day'],
                missing_value=sim_param[cn]['missing_value'],
                label=cn)
        elif sim_param[cn]['comparer'] == 'byear':
            comps[cn] = CompareByear(
                vars_a[cn], vars_b[cn],
                within_year_dif=sim_param[cn]['within_1y'],
                missing_value=sim_param[cn]['missing_value'],
                year_dif=sim_param[cn].get("year_dif", 1),
                label=cn)
        elif sim_param[cn]['comparer'] == 'exact':
            comps[cn] = Exact(
                vars_a[cn], vars_b[cn],
                missing_value=sim_param[cn]['missing_value'],
                label=cn)
        elif sim_param[cn]['comparer'] == 'levenshtein':
            comps[cn] = String(
                vars_a[cn], vars_b[cn],
                method='levenshtein',
                missing_value=sim_param[cn]['missing_value'],
                label=cn)
        elif sim_param[cn]['comparer'] == 'numeric':
            comps[cn] = Numeric(
                vars_a[cn], vars_b[cn],
                method='linear',
                missing_value=sim_param[cn]['missing_value'],
                label=cn)
        elif sim_param[cn]['comparer'] == 'date':
            comps[cn] = Date(
                vars_a[cn], vars_b[cn],
                swap_month_day=sim_param[cn]['swap_month_day'],
                missing_value=sim_param[cn]['missing_value'],
                label=cn)
        else:
            raise Exception(f'Comparer for {cn} not yet defined. \
                Revise match_functions.prepare_comparers() accordingly.')
    return comps


def get_valid_comp_names(std_varnames):
    '''Returns a list of valid comparison names based on the variables
    this dataset includes. This handles the case when a dataset does
    not have a comparison variable listed in config["comp_names_by_pass"]

    Input:
        - std_varnames: (list) standardized variable names in input data
    Returns: (list) valid comparison names
    '''
    valid_comp_names = [v for v in std_varnames if v not in
        ('indv_id', 'xf', 'xl')]
    if set(['fname', 'lname']).issubset(set(std_varnames)):
        valid_comp_names.extend(('fnamelname', 'lnamefname'))
    if set(['bmonth', 'bday']).issubset(set(std_varnames)):
        valid_comp_names.append('bmonthbday')
    return valid_comp_names


def calc_similarities(df_a, df_b, passnum, candidates, comp_names, comps):
    '''Calculate similarity scores for a given pass as defined by comp_names

    Input:
        - df_a, df_b: (pandas DataFrame) input data
        - passnum: (int) pass number for this pass
        - candidates: (pandas MultiIndex) pairs of indices of match candidates,
            returned by find_candidates function
        - comp_names: (list of str) list of comparison names/labels for this
            pass, sourced from config['comp_names_by_pass']
        - comps: (dict) mapping of comparison name/label (key) to comparer
            objects (value), returned by prepare_comparers function

    Returns: (pandas DataFrame) updated results dataframe with similarity scores
    '''
    pass_comps = [comps[c] for c in comp_names]
    comparer = recordlinkage.Compare(pass_comps)
    df_sim = comparer.compute(candidates, df_a, df_b)
    df_sim = df_sim.rename_axis(['idx_a', 'idx_b']).reset_index()
    df_sim['passnum'] = passnum
    return df_sim


### Utility functions ---------------------------------------------------------

def load_data(ds_key, config):
    '''Loads preprocessed data file using data_param set in config.json

    Only reads in columns specified in vars under data_param.

    Input:
        ds_key: (str) key of config['data_param'], 'df_a' or 'df_b'
        config: (dict)

    Returns (tuple) pandas dataframe of data and name of table in database
    '''
    ds_config = config['data_param'][ds_key]
    dtypes = ds_config['dtype']
    keep_cols = ds_config['vars'].values()
    tablename = ds_config["name"] # name of table in database

    if ds_config['filetype'] == 'fwf':
        ds = pd.read_fwf(ds_config['filepath'],
            **ds_config['fwf_args'], dtype=dtypes
                )
    elif ds_config['filetype'] == 'csv':
        ds = pd.read_csv(ds_config['filepath'], dtype=dtypes, usecols=keep_cols)
    elif ds_config['filetype'] == 'db':
        conn, schema, tablename = connect_to_db(ds_config['db_args'])
        cursor = conn.cursor()
        select_cols_str = ','.join(keep_cols)
        cmd = f'''
                SELECT {select_cols_str} from {schema}.{tablename}
                ORDER BY idx::float
                '''
        ds = pd.read_sql(cmd, conn)
        ds = ds.astype(dtypes)
        conn.close()
    else:
        raise Exception('match_functions not yet built for this filetype')

    ds = format_preprocessed_dataset(ds, ds_key, config)
    ds = ds[keep_cols]

    return ds, tablename


def format_preprocessed_dataset(ds, ds_key, config):
    '''
    Prepare the preprocessed dataset to the right format for calculating
    similarity metrics

    Input:
        ds: (pandas DataFrame) preprocessed dataset
        ds_key: (str) key of config['data_param'], 'df_a' or 'df_b'
        config: (dict)

    Returns a pandas dataframe
    '''
    ds_config = config['data_param'][ds_key]
    ds_vars = ds_config['vars']

    # Convert birthday string to year/month/day columns
    # Used for older datasets not preprocessed with current script
    if "dob_str" in ds.columns and "byear" in ds_vars:
        ds['byear'] = ds.dob_str.str[:4].astype(float)
        ds['bmonth'] = ds.dob_str.str[4:6].astype(float)
        ds['bday'] = ds.dob_str.str[6:8].astype(float)

    # Convert bdate components to numeric
    ds['byear'] = pd.to_numeric(ds[ds_config["vars"]["byear"]],
        downcast='float', errors='coerce')
    ds['bmonth'] = pd.to_numeric(ds[ds_config["vars"]["bmonth"]],
        downcast='float', errors='coerce')
    ds['bday'] = pd.to_numeric(ds[ds_config["vars"]["bday"]],
        downcast='float', errors='coerce')

    if "minitial" not in ds.columns and "minitial" in ds_vars:
        ds["minitial"] = np.where(ds[ds_vars["mname"]].str.len() > 0,
                                  ds[ds_vars["mname"]].str[:1],
                                  '')

    # Find variables used for string comparison
    comp_config = config['sim_param']
    string_comparison_vars = []
    for comp_name, param_dict in comp_config.items():
        comp_algo = param_dict['comparer']
        if comp_algo in ('jarowinkler', 'levenshtein', 'minitial'):
            string_comparison_vars.append(comp_name)

    # Convert empty strings to NULLs for any string comparison variables
    for v in string_comparison_vars:
        if v in ds_vars:
            ds[ds_vars[v]].replace('', np.nan, inplace=True)

    return ds


def connect_to_db(database_config):
    '''
    Connect to database based on config.json

    Input:
        database_config: a dictionary with the following potential keys:
            - host
            - dbname
            - schema
            - tablename
    Returns: A tuple of:
        - A psycopg2 connection object
        - schema name (str)
        - table name (str) if applicable. Otherwise, returns empty string.
    '''
    host = database_config['host']
    dbname = database_config['dbname']
    schema = database_config['schema']
    tablename = database_config.get('tablename', '')
    conn = psycopg2.connect(host=host, dbname=dbname)
    return (conn, schema, tablename)


def get_latest_file_in_dir(directory, table_a, stricttype=None,
                           table_b=None):
    '''
    Get the most recent file matching the config inputs in the corresponding
    directory

    Inputs:
        directory (str): the directory to check
        table_a (str): the first table matched, if dedup, only table used
        stricttype (str): the strictness to use (input empty string if not needed)
        table_b (str): the other table matched, default None in case of dedup
    Returns (str) of the most recent file that matches the description
    '''
    files = os.listdir(directory)
    paths = []
    for basename in files:
        if table_a in basename and ((not table_b and "dedup" in basename)  or
                                    (table_b and table_b in basename)) and \
        (not stricttype or stricttype in basename):
            paths.append(os.path.join(directory, basename))
    file = max(paths, key=os.path.getctime)
    return file


def calculate_pass_match_counts(dfs, counts):
    '''
    Calculate the total match counts for each pass, over strictness levels,
    by converting to a long dataframe over idxes, pass, and strictness
    level and calculating sum of accepted pairs over passnum/strictness.

    Inputs:
        dfs (dataframe): match results
        counts (dataframe): Previously calculated counts of matches

    Returns (df) updated counts dataframe
    '''
    df_count =  pd.wide_to_long(dfs, 'match',
                        i=['idx_a', 'idx_b', 'passnum'],
                        j='strictness',
                        sep='_',
                        suffix=r'\w+'
                        ).reset_index().groupby(['passnum', 'strictness']
                        )['match'].sum().reset_index()
    counts = pd.concat([counts, df_count], axis=0, ignore_index=True)
    return counts


def calculate_weights(dfs, tot_pass_cnt):
    '''
    Calculate the weight of each pair to use for sorting.

    Inputs:
        dfs (dataframe): match results
        tot_pass_cnt (int): total number of passes
    '''
    eval_string = f"10 ** ({tot_pass_cnt} - passnum) + " + \
                  " + ".join(dfs.drop(columns=["indv_id_a",
                                               "indv_id_b",
                                               "idx_a", "idx_b",
                                               "passnum"]))
    # Replace negative scores (ie missing values) with .5 to avoid
    # negatively impacting weights
    dfs["weight"] = dfs.replace(-1, .5).fillna(0).eval(eval_string)


def print_runtime(last_time):
    '''Prints the difference between now and the last time check'''
    print('[time: %s seconds]' % (time.time() - last_time))


def print_match_count(counts, passnum=None):
    '''
    Print match count for each pass or the entire match
    Inputs:
        counts (df): dataframe storing counts of accepted matches
                     by pass/strictness level for each batch.
        passnum (int, default None): Pass to print counts for
                     if None, print all.
    Returns None, prints to terminal
    '''
    if passnum is None:
        p_mask = counts.passnum.notnull()
        sums = counts[p_mask].fillna(0).groupby(["strictness"]).agg({'match': sum}).reset_index()
        print('=== All Passes ===')
    else:
        p_mask = counts.passnum == passnum
        sums = counts[p_mask].fillna(0).groupby(["passnum",
                                                 "strictness"]).sum().reset_index()
        if str(passnum).startswith('dup'):
            print('=== Ground truth ID {} ==='.format(passnum[4:]))
        else:
            print('=== Pass {} ==='.format(passnum))
    if "match" not in sums.columns:
        sums["match"] = 0
    strict = sums.loc[sums['strictness'] == 'strict']["match"]
    strict = '0' if strict.empty else strict.to_string(index=False)
    moderate = sums.loc[sums['strictness'] == 'moderate']["match"]
    moderate = '0' if moderate.empty else moderate.to_string(index=False)
    relaxed = sums.loc[sums['strictness'] == 'relaxed']["match"]
    relaxed = '0' if relaxed.empty else relaxed.to_string(index=False)
    review = sums.loc[sums['strictness'] == 'review']["match"]
    review = '0' if review.empty else review.to_string(index=False)

    print('Strict: {} matches accepted'.format(strict))
    print('Moderate: {} matches accepted'.format(moderate))
    print('Relaxed: {} matches accepted'.format(relaxed))
    print(f"Review: {review} flagged pairs ({relaxed} relaxed matches + " +
          f"{int(review) - int(relaxed)} unmatched pairs for review)")


def save_output(name_a, name_b, config):
    '''
    Read in temp files and combine into one final csv file, sorted by weights.
    Temp files are then deleted.
    Output directory is defined in config.json

    Input:
        - name_a, name_b: (str) short hand names for the input data,
            sourced from config['data_param']
        - config: (dict)

    Returns: None
    '''
    path = "temp_match_*.csv"

    output_id = f'{name_a}_{name_b}_{date.today()}'
    output_filepath = config['output_dir'] + \
        f'match_results_with_pairwise_scores_{output_id}.csv'

    chunks = []
    for filename in glob.glob(path):
        f_input = open(filename)
        csv_input = csv.reader(f_input, delimiter=',', skipinitialspace=True)
        header = next(csv_input)
        chunks.append(csv_input)
    # Save the results to the output directory.
    with open(output_filepath, 'w') as f_out:
        csv_output = csv.writer(f_out, delimiter=',')
        csv_output.writerow(header)
        csv_output.writerows(heapq.merge(*chunks,
                                         key=lambda k: float(k[9]),
                                         reverse=True))
    for file in glob.glob(path):
        print(file)
        os.remove(file)
    print(f'Match results and similarity scores of all candidate pairs saved in {output_filepath}')
