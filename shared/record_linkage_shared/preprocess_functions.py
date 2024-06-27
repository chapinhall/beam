import re
import os
import unicodedata
import pandas as pd
import numpy as np

# Load list of bad names to use in functions
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

BADNAME_FILE = os.path.join(__location__, 'badnames.txt')
with open(BADNAME_FILE, "r") as f:
    badnames = f.readlines()[0].split(',')

DELIMS = r"[^\w]"

SUFFIXES = {'IIII': 'IV',
            'IV': 'IV',
            'III': 'III',
            ' II': 'II',
            ' I': 'I',
            ',I': 'I',
            ' 1': 'I',
            ',1': 'I',
            '2ND': 'II',
            '3RD': 'III',
            '4TH': 'IV',
            'ESQ': 'ESQ',
            'JR': 'JR',
            'JR.': 'JR',
            'SR': 'SR',
            '2D': 'II',
            '3D': 'III',
            '2': 'II',
            '3': 'III',
            '4': 'IV'
            }

PREFIXES = ['ABDEL','ABDUL','AB','ABU',
            'AL','CA', 'C','DA','DE',
            'DEL','DES','DI','DU','D',
            'EL','ET','LOS','JEAN','TE',
            'VAN','DELA','DAL','DER',
            'DELLA','L','LE','LA','MAC',
            'MC','TEN','TER','VER','VON',
            'O', 'SAN', 'SANTA']

FNAME_PREFIXES = ['DA', 'DE','JA','KE','LA',
                  'LE','LU','SA','SAN','TA',
                  'TE','TRE','TY', 'KA']

SAINTS = ['SAINT', 'SAINTE', 'ST', 'STE']

TITLES = ["MR", "MRS", "MS", "MISS", "DR"]


def remove_accents(text):
    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3
        pass

    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")

    return str(text)


def get_aliases(data, lname=False):
    '''
    Creates aliases for the specified name values in the dataset

    Input:
        data (pandas DataFrame)
        lname (bool): whether to create aliases for lname.
            If False, creates aliases for fname

    Returns updated dataframe
    '''
    if lname:
        name_vars = ['lname', 'altlname']
    else:
        name_vars = ['fname', 'altfname']

    data["orig_alt"] = data[name_vars[1]]

    data = pd.melt(data,
                   id_vars=data.columns.difference(name_vars),
                   value_vars=name_vars,
                   value_name='name_update')
    data = data.drop(data[(data.variable == name_vars[1]) &
                          (data.name_update.isnull())].index)

    data.loc[data.variable == name_vars[1], 'orig_alt'] = None
    data = data.rename(columns={'name_update': name_vars[0],
                                'orig_alt': name_vars[1]})
    data.drop('variable', axis=1, inplace=True)
    return data


def get_mname_aliases(data):
    '''
    Creates aliases for the middle names, so there is one record for
    the full middle name, and a record for each separate name in the mname
    field.

    Input:
        data (pandas DataFrame)

    Returns updated dataframe
    '''
    # Split the middle name by space
    data = pd.concat([data,
                      data["mname"].str.split(" ", expand=True).add_prefix("mname_")],
                      axis=1)
    keep_cols = [col for col in data.columns if "mname" not in col]
    mname_cols = [col for col in data.columns if "mname" in col]
    data = pd.melt(data,
                   id_vars=keep_cols,
                   value_vars=mname_cols,
                   value_name='mname_keep')
    # Remove all empty results that aren't the original mname
    data = data.drop(data[((data.mname_keep.isnull()) | (data.mname_keep == ""))
                             & (data.variable != "mname")].index)
    # Rename columns and drop duplicates of middle names
    data = data.rename(columns={'mname_keep': 'mname'}
                       ).drop('variable', axis=1
                       ).drop_duplicates()
    return data


def fix_fname(fname, mname, addtlbadnames):
    '''
    Cleans the first and middle name, creating additional altfname
    if needed

    Input:
        fname (str)
        mname (str)
        addtlbadnames (list of str) any additional names that
            should be removed from the data

    Return (str, str, str, str)
    '''
    badnames.extend(addtlbadnames)
    # Clean inputs
    if mname and mname != np.nan:
        mname = remove_accents(str(mname)).upper().strip().replace("1","I"
                              ).replace("0", "O"
                              ).replace("'", ""
                              ).replace("^","")
        if "'" in mname:
            print(mname)
    else:
        mname = ''
    suffix = ''
    altfname = ''

    if fname:
        fname = remove_accents(str(fname)).upper().strip()
        fname = fname.replace("1",
                              "I").replace("0",
                              "O").replace("'",
                              "").replace("^",
                              "")
        if fname in badnames:
            fname = ''
        # Check if fname or mname ends with a suffix
        for suf in SUFFIXES:
            if fname.endswith(suf, -4):
                fname = fname[:-len(suf)].strip()
                if not suffix:
                    suffix = SUFFIXES[suf]
            if mname.endswith(suf, -4):
                mname = mname[:-len(suf)].strip()
                if not suffix:
                    suffix = SUFFIXES[suf]

        # Check for common splits in fname to identify altfname
        fnames_orig = re.split(r"/|\[|&|\\|\(", fname)
        fnames_clean = []
        for i, fn in enumerate(fnames_orig):
            fn = fn.strip(")]}->")
            if fn in SUFFIXES:
                if not suffix:
                    suffix = SUFFIXES[fn]
            elif not (fn in badnames or fn.isnumeric() or fn in TITLES):
                fnames_clean.append(fn)
        if not fnames_clean:
            fname = ''
        else:
            fname = fnames_clean[0]
        if len(fnames_clean) > 1:
            altfname = ''.join(fnames_clean[1:]).strip(")]}->")

        # Clean mname of delimiters and suffixes
        mname_all_split = filter(None, re.split(DELIMS, mname))
        mn_clean  = []

        for mn in mname_all_split:
            if mn in SUFFIXES:
                if not suffix:
                    suffix = SUFFIXES[mn]
            elif not (mn in badnames or mn.isnumeric()):
                mn_clean.append(mn)

        mname = ' '.join(mn_clean).strip()
        # Clean fname of delimiters and suffixes
        fname_all_split = filter(None, re.split(DELIMS, fname))
        fn_clean = []
        pref = ''

        for fn in fname_all_split:
            if fn in SUFFIXES:
                if not suffix:
                    suffix = SUFFIXES[fn]
            elif not (fn in badnames or fn.isnumeric()):
                # If prefix seen, attach to following value
                if pref and fn not in FNAME_PREFIXES:
                    fn = pref + fn
                    pref = ''
                if fn in FNAME_PREFIXES or len(fn) == 1:
                    pref += fn
                    continue
                fn_clean.append(fn)
        if pref:
            fn_clean.append(pref)

        # Assign fname to first clean value
        if fn_clean:
            fname = re.sub(r"\d", '', fn_clean[0])
        else:
            fname = ''
        # For remaining name parts, include in mname. If mname is an
        # initial of the last name part or equal to it, don't include
        if len(fn_clean) > 1:
            potential_mname = ' '.join(fn_clean[1:])
            if mname != fn_clean[-1][0] and mname != fn_clean[-1]:
                mname = re.sub(r"\d", "", potential_mname + ' ' + mname)
            else:
                mname = re.sub(r"\d", "", potential_mname)
            mname = mname.strip()
        # Remove repeat names
        if altfname == mname or altfname == fname:
            altfname = ''
        if mname == fname:
            mname = ''

        # Update suffix with JR if necessary (not sure if this code should still be included?)
        if fname == "JR":
            suffix = "JR"
            fname = ''

    return [None if not x else x for x in (fname, mname, altfname, suffix)]


def fix_lname(lname, altlname, addtlbadnames):
    '''
    Cleans the last and alternative last name
    Input:
        lname (str)
        altlname (str)
        addtlbadnames (list of str) any additional names that
            should be removed from the data

    Return (str, str, str)

    Things to check on still:
        if altlname is provided, do we replace with any others seen
        or only use provided altlname?
        if no lname but altlname, use altlname?
        cleaning provided altlname
    '''
    badnames.extend(addtlbadnames)

    # Standardize inputs
    lname = remove_accents(lname).upper().strip()
    altlname = remove_accents(altlname).upper().strip()
    altln = ''
    suffix = ''

    if lname:
        # Check for brackets and slashes for altlnames
        lnames_orig = re.split(r"\(|\[|/|\\", lname)
        lnames_clean = []
        for i, ln in enumerate(lnames_orig):
            ln = ln.replace('0', 'O').replace('1', 'I')
            if ln in badnames or ln.isnumeric():
                if i == 0:
                    return (None, None, None)
            else:
                ln = ln.strip(")]}->")
                for suf in SUFFIXES:
                    if ln.endswith(suf, -4):
                        ln = ln[:-len(suf)].strip()
                        suffix = SUFFIXES[suf]
                        break
                if ln not in badnames:
                    lnames_clean.append((ln, suffix))

        # Assign lname to the first cleaned value
        lname, suffix = lnames_clean[0]

        # Create altlname with remaining values
        if len(lnames_clean) > 1:
            altln = ''
            for ln, suf in lnames_clean[1:]:
                if ln.strip(")]}-> ") not in badnames:
                    altln += ln.strip(")]}-> ") +  ' '
                    # Update suffix with first one seen
                    if not suffix:
                        suffix = suf
            altln = altln.strip().replace('-', '').strip(")]}-> ")

        # Split on dash, checking for suffixes and prefixes
        # Replace with space otherwise
        if '-' in lname.strip("-"):
            lnames = lname.split('-')
            ln_clean = []
            pref = ''
            for ln in lnames:
                if ln in SUFFIXES:
                    if not suffix:
                        suffix = SUFFIXES[ln]
                elif not (ln in badnames or ln.isnumeric()):
                    if pref and ln not in PREFIXES:
                        ln = pref + ln
                        pref = ''
                    if ln in PREFIXES:
                        pref += ln
                        continue
                    ln_clean.append(ln)
            if pref:
                ln_clean.append(pref)
            lname = ' '.join(ln_clean)

        # Split on all delimiters (removing) and check for suffixes,
        # prefixes and saints
        lname_all_split = filter(None, re.split(DELIMS, lname))
        ln_clean  = []
        pref = ''
        saint = False
        for ln in lname_all_split:
            if ln in SUFFIXES:
                if not suffix:
                    suffix = SUFFIXES[ln]
            elif not (ln in badnames or ln.isnumeric()):
                if pref and ln not in PREFIXES:
                    ln = pref + ln
                    pref = ''
                if saint:
                    ln = "ST" + ln.strip()
                saint = ln in SAINTS
                if ln in PREFIXES:
                    pref += ln
                    continue
                ln_clean.append(ln)
        if pref:
            ln_clean.append(pref)
        # Assign lname to first value seen
        if ln_clean:
            lname = re.sub(r"\d", "", ln_clean[0])

            # If no altlname, assign with remaining values joined
            if len(ln_clean) > 1 and not altln:
                altln = ''.join(ln_clean[1:])
        else:
            lname = ''

    if (not altlname) or (altlname == lname):
        altlname = altln
    altlname = re.sub(r"\d| ", "", altlname)

    # Use altlname instead if inclusive of lname
    if len(lname) < len(altlname) and lname in altlname:
        lname = altlname
        altlname = ''
    # Remove altlname if lname inclusive of altlname
    elif lname == altlname or altlname in lname:
        altlname = ''

    # Replace lname with altlname if lname is empty
    if lname == '' and altlname != '':
        lname = altlname
        altlname = ''
    return [None if not x else x for x in (lname, suffix, altlname)]
