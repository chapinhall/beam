'''
This script contains functions to create the blocks for
the matching.
'''
import json
import time
import psycopg2

start = time.time()

CONFIG_FILE = "config.json"


def get_pass_join_cond(passblocks_a, passblocks_b):
    # create the "ON" part of a join command
    join_cond = []
    for i in range(len(passblocks_a)):
        cond = f'''
            (a.{passblocks_a[i]} = b.{passblocks_b[i]} \
            AND a.{passblocks_a[i]} != ''
            AND a.{passblocks_a[i]} IS NOT NULL)
            '''
        join_cond.append(cond)
    join_cond_str = ' AND '.join(join_cond)

    return join_cond_str


def exclude_past_join_cond(join_cond_str, past_join_cond_str):
    '''exclude pairs that are captured by past blocking strategies
    from the new join'''
    if past_join_cond_str:
        join_cond_fullstr = join_cond_str + f' AND NOT ({past_join_cond_str})'
    else:
        join_cond_fullstr = join_cond_str

    return join_cond_fullstr


def update_past_join_cond(join_cond_str, past_join_cond_str):
    '''update past_join_cond_str to include the current pass' join conditions
    '''
    if past_join_cond_str:
        past_join_cond_str = past_join_cond_str + f' OR ({join_cond_str})'
    else:
        past_join_cond_str = f'({join_cond_str})'

    return past_join_cond_str


def execute_blocking_join(table_a, table_b, candidates_table, indv_id_a,
                          indv_id_b, join_cond_fullstr, cursor):
    '''
    Drop candidates table for pass, if exists, and execute command
    to create the candidates table using the join condition.
    '''
    cmd = '''DROP TABLE IF EXISTS {}'''.format(candidates_table)
    cursor.execute(cmd)

    cmd = '''
    CREATE TABLE IF NOT EXISTS {candidates_table} AS
    SELECT
        a.{indv_id_a} as indv_id_a,
        b.{indv_id_b} as indv_id_b,
        a.idx::integer AS idx_a,
        b.idx::integer AS idx_b
    FROM (SELECT * FROM {table_a} ORDER BY idx::float) a
    INNER JOIN (SELECT * FROM {table_b} ORDER BY idx::float) b
    ON {join_cond_fullstr}
    ;'''.format(
        candidates_table=candidates_table,
        indv_id_a=indv_id_a,
        indv_id_b=indv_id_b,
        table_a=table_a,
        table_b=table_b,
        join_cond_fullstr=join_cond_fullstr
        )
    cursor.execute(cmd)


def find_pass_candidates(passblocks_a, passblocks_b, indv_id_a, indv_id_b,
                         past_join_cond_str, table_a, table_b,
                         candidates_table, dedup, cursor):
    '''defines join conditions, creates a candidate table with rows
    (idx_a, idx_b) in Postgres, and updates the string that stores
    past join conditions, which are excluded in the next blocking pass
    '''

    join_cond_str = get_pass_join_cond(passblocks_a, passblocks_b)
    join_cond_fullstr = exclude_past_join_cond(join_cond_str,
                                               past_join_cond_str)
    if dedup:
        # exclude joining to self
        join_cond_fullstr += 'AND a.idx < b.idx'
        join_cond_fullstr += f' AND a.{indv_id_a} != b.{indv_id_b}'
    execute_blocking_join(table_a, table_b, candidates_table,
                          indv_id_a, indv_id_b, join_cond_fullstr, cursor)

    # update past join conditions to exclude in the next pass
    past_join_cond_str = update_past_join_cond(join_cond_str,
                                               past_join_cond_str)

    return past_join_cond_str


def run_ground_truth_ids_passes(ground_truth_ids, vars_a, vars_b, schema,
                              name_a, name_b, past_join_cond_str,
                              cursor, table_a, table_b):
    '''
    Blocks on each ground truth ID provided and stores all the matching
    pairs in a candidate table.

    Inputs:
        ground_truth_ids (list)
        vars_a (dict): source variable names for df a
        vars_b (dict): source variable names for df b
        schema (str): name of schema where candidates tables are stored
        name_a (str): name of df a (for storing/print purposes)
        name_b (str): name of df b (for storing/print purposes)
        past_join_cond_str (str): string of past join combinations to avoid
            matching pairs repeatedly
        cursor (Cursor object): cursor connected to database
        table_a (str): database table name of df_a
        table_b (str): database table name of db_b

    Returns (str) updated past_join_cond_str
    '''
    print('Finding pairs sharing ground truth IDs...')
    tot_gid_cnt = len(ground_truth_ids)

    indv_id_a = vars_a['indv_id']
    indv_id_b = vars_b['indv_id']

    if name_b == "dedup":
        match_name = f"{name_a}_dedup"
        table_b = table_a
        dedup = True
    else:
        match_name = f"{name_a}_{name_b}"
        dedup = False

    for i in range(tot_gid_cnt):
        gid = ground_truth_ids[i]
        print(f'- {gid}')
        gid_start = time.time()

        # get actual ground truth ID variable names for each dataset
        gid_a = vars_a[gid]
        gid_b = vars_b[gid]

        candidates_table = f'{schema}.candidates_{match_name}_matching_{gid}'

        past_join_cond_str = find_pass_candidates([gid_a], [gid_b],
                                                  indv_id_a, indv_id_b,
                                                  past_join_cond_str,
                                                  table_a, table_b,
                                                  candidates_table, dedup,
                                                  cursor)

        gid_end = time.time()
        print('***Table: {}'.format(candidates_table))
        print('***Rows inserted: {:,}'.format(cursor.rowcount))
        print('***Time: ', gid_end - gid_start)
    return past_join_cond_str


def run_blocking_pass(blocks_by_pass, passnum, vars_a, vars_b,
                      schema, name_a, name_b, past_join_cond_str,
                      cursor, table_a, table_b):
    '''
    Blocks on pass provided and stores all the matching
    pairs in a candidate table.

    Inputs:
        blocks_by_pass (list): list of each pass, containing variables to block
            on for each pass
        passnum (int): The current pass
        vars_a (dict): source variable names for df a
        vars_b (dict): source variable names for df b
        schema (str): name of schema where candidates tables are stored
        name_a (str): name of df a
        name_b (str): name of df b
        past_join_cond_str (str): string of past join combinations to avoid
            matching pairs repeatedly
        cursor (Cursor object): cursor connected to database
        table_a (str): database table name of df_a
        table_b (str): database table name of db_b

    Returns (str) updated past_join_cond_str
    '''
    blocking_vars = blocks_by_pass[passnum]

    indv_id_a = vars_a['indv_id']
    indv_id_b = vars_b['indv_id']

    if name_b == "dedup":
        match_name = f"{name_a}_dedup"
        table_b = table_a
        dedup = True
    else:
        match_name = f"{name_a}_{name_b}"
        dedup = False

    if blocking_vars:
        print(f"Pass {passnum} - Blocking on: {', '.join(blocking_vars)}")
        pass_start = time.time()

        # Flag if this pass is blocked on variables inverted
        # (e.g. xf/xl, xl/xf)
        inverted_blocks = '_inv' in str(blocking_vars)
        if inverted_blocks:
            blocking_vars = [s.replace('_inv', '') for s in blocking_vars]

        # get actual blocking variable names for each dataset
        passblocks_a = [vars_a[v] for v in blocking_vars if v in vars_a]
        passblocks_b = [vars_b[v] for v in blocking_vars if v in vars_b]

        if (len(passblocks_a) != len(blocking_vars) or
            len(passblocks_b) != len(blocking_vars)):
            missing_var = {v for v in blocking_vars if v not in vars_a or v not in vars_b}
            print(f"\tPass {passnum} is being skipped since {missing_var} is not included.")
            return past_join_cond_str
        if inverted_blocks:
            passblocks_b.reverse()

        candidates_table = f'{schema}.candidates_{match_name}_p{passnum}'
        past_join_cond_str = find_pass_candidates(passblocks_a, passblocks_b,
                                                  indv_id_a, indv_id_b,
                                                  past_join_cond_str,
                                                  table_a, table_b,
                                                  candidates_table, dedup,
                                                  cursor)
        pass_end = time.time()
        rows = cursor.rowcount
        print('***Table: {}'.format(candidates_table))
        print('***Rows inserted: {:,}'.format(rows))
        print('***Time: ', pass_end - pass_start)
        return past_join_cond_str

    else:
        print(f"Pass {passnum} - Skipped according to config_match")
        return ''
