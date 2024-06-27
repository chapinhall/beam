'''
The default logic for the acceptance parameters.
'''

def accept_p0_strict(df_match, masks, thresholds):
    '''
    Strict for pass 0 accepts all matches
    '''
    return True


def accept_p0_moderate(df_match, masks, thresholds):
    '''
    Moderate for pass 0 accepts all matches
    '''
    return True


def accept_p0_relaxed(df_match, masks, thresholds):
    '''
    Relaxed for pass 0 accepts all matches
    '''
    return True


def accept_p0_review(df_match, masks, thresholds):
    '''
    Relaxed for pass 0 accepts all matches
    '''
    return True


## Pass 1: Block on common_id

# - Strict:
#   - High-similarity fname & high-similarity lname & DOB partial match,
#     i.e. (bmm matches & bdd matches & byear w/in 1) |
#          (byear matches & bmm matches) |
#          (byear matches & bdd matches)
#     OR High similarity fnamelname & high similarity lnamefname & DOB partial match
# - Moderate
#    - Strict, OR
#    - High similarity fname & high similarity lname, OR
#    - High similarity fnamelname & high-similarity lnamefname, OR
#    - (High similarity fname or high similiarity lname) AND DOB is exact or partial match
# - Relaxed
#    - Moderate, OR
#    - Any other high-similarity fname, OR
#    - Any other DOB is partial match
#    -  i.e. (bmm matches & bdd matches & byear w/in 1) |
#            (byear matches & bmm matches) |
#            (byear matches & bdd matches)

def accept_p1_strict(df_match, masks, thresholds):
    '''
  - High-similarity fname & high-similarity lname & DOB partial match,
    i.e. (bmm matches & bdd matches & byear w/in 1) |
         (byear matches & bmm matches) |
         (byear matches & bdd matches)
    OR High similarity fnamelname & high similarity lnamefname & DOB partial match
    '''
    dob_partial_mask = masks['dob_partial_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask =  \
            (# close fname/lname/dob (inversion checked as well)
             (df_match.fname >= name_high_score) &
             (df_match.lname >= name_high_score) &
             (dob_partial_mask)) | (
             (df_match.fnamelname >= name_high_score) &
             (df_match.lnamefname >= name_high_score) &
             (dob_partial_mask)
             )
    return accept_mask


def accept_p1_moderate(df_match, masks, thresholds):
    '''
    - Strict, OR
   - High similarity fname & high similarity lname, OR
   - High similarity fnamelname & high-similarity lnamefname, OR
   - (High similarity fname or high similiarity lname) AND DOB is exact or partial match

   '''
    dob_partial_mask = masks['dob_partial_mask']
    dob_exact_mask = masks['dob_exact_mask']
    name_high_score = thresholds['name_high_score']
    byear_within1_score = thresholds['byear_within1_score']
    accept_mask = \
            ( # at least two highly similar fields (fname, lname, byear, bdate), inversion checked
             (df_match.fname >= name_high_score) & (df_match.lname >= name_high_score)) | ( \
             (df_match.fnamelname >= name_high_score) & (df_match.lnamefname >= name_high_score)) | ( \
             (df_match.fname >= name_high_score) & (df_match.byear >= byear_within1_score)) | ( \
             (
                (df_match.fname >= name_high_score) | (df_match.lname >= name_high_score)
              ) & \
                (dob_exact_mask | dob_partial_mask)
              )
    return accept_mask


def accept_p1_relaxed(df_match, masks, thresholds):
    '''
    - Moderate, OR
    - Any other high-similarity fname, OR
    - Any other DOB is partial match
    -  i.e. (bmm matches & bdd matches & byear w/in 1) |
           (byear matches & bmm matches) |
           (byear matches & bdd matches)
    '''
    dob_partial_mask = masks['dob_partial_mask']
    dob_exact_mask = masks['dob_exact_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
             ( # close birthday or first name
             (df_match.fname >= name_high_score) | \
            (dob_exact_mask | dob_partial_mask))
    return accept_mask


def accept_p1_review(df_match, masks, thresholds):
    '''
    Relaxed for pass 0 accepts all matches
    '''
    return True


# Pass 2: Block on xf, xl

#  - Strict
#   - If common ID edit distance is close:
#        - At least 2/3 of fname, lname and DOB are exact matches, other
#          is high similarity
#   - If common ID is null:
#        - At least 2/3 of fname, lname and DOB are exact matches, other
#          is high similarity AND
#             - high similarity altlname or minitial/mname good match i.e.
#                   - minitial matches and one is single letter OR
#                   - very high-similarity mname
#        - OR 3/3 of fname, lname and DOB are exact and middle
#          name is missing, and same zip OR county
#   - If common ID is not close:
#        - Fname, lname and DOB are exact &
#           - mname/minitial is a good match OR
#           - altlname is exact and not null
# - Moderate
#   - High-similarity fname & high-similarity lname & high-similarity DOB &
#     high similarity in one of the additional evidence fields:
#       - Common ID edit distance
#       - Another name component (mname/altlname)
#       - Same zipcode or county
# - Relaxed
#   - Moderate/strict, OR
#   - High-similarity fname & high-similarity lname &
#     (high-similarity DOB | similar common ID edit distance)

def accept_p2_strict(df_match, masks, thresholds):
    '''
  - If common ID edit distance is close:
       - At least 2/3 of fname, lname and DOB are exact matches, other
         is high similarity
  - If common ID is null:
       - At least 2/3 of fname, lname and DOB are exact matches, other
         is high similarity AND
            - high similarity altlname or minitial/mname good match i.e.
                  - minitial matches and one is single letter OR
                  - very high-similarity mname
       - OR 3/3 of fname, lname and DOB are exact and middle
         name is missing, and same zip OR county
  - If common ID is not close:
       - Fname, lname and DOB are exact &
          - mname/minitial is a good match OR
          - altlname is exact and not null
    '''
    id_high_mask = masks['id_high_mask']
    dob_partial_mask = masks['dob_partial_mask']
    dob_exact_mask = masks['dob_exact_mask']
    common_id_null = masks['common_id_null']
    minit_match_mname_veryhighsim_mask = masks['minit_match_mname_veryhighsim_mask']
    loc_exact_mask = masks['loc_exact_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
        (  # common id are a close match
            (id_high_mask) & \
            ( # two match exactly, third is close match
                (
                    (df_match.fname == 1) & \
                    (df_match.lname == 1) & \
                    (dob_partial_mask)
                ) | \
                (
                    (df_match.fname >= name_high_score) & \
                    (df_match.lname == 1) & \
                    (dob_exact_mask)
                 ) | \
                (
                    (df_match.fname == 1) & \
                    (df_match.lname >= name_high_score) & \
                    (dob_exact_mask)
                 )
            )
        ) | \
        ( # common id is null for at least one
            (common_id_null) & \
            (
                (
                    ( # two match exactly and third is close match and
                      # either middle name or alt name close match
                        (
                            (df_match.fname == 1) & \
                            (df_match.lname == 1) & \
                            (dob_partial_mask)
                        ) | \
                        (
                            (df_match.fname >= name_high_score) & \
                            (df_match.lname == 1) & \
                            (dob_exact_mask)
                         ) | \
                        (
                            (df_match.fname == 1) & \
                            (df_match.lname >= name_high_score) & \
                            (dob_exact_mask)
                         )
                    ) & \
                    (
                        (minit_match_mname_veryhighsim_mask) | \
                        (df_match.altlname >= name_high_score)
                    )
                ) | \
                ( # all three are exact match and middle name is null
                    (df_match.fname == 1) & \
                    (df_match.lname == 1) & \
                    (dob_exact_mask) & \
                    (df_match.mname == -1) & \
                    (loc_exact_mask)
                )
            )
        ) | \
        ( # common id is not a good match (exact match + one other field)
            (df_match.fname == 1) & \
            (df_match.lname == 1) & \
            (dob_exact_mask) & \
            (
                (df_match.altlname == 1) | \
                (minit_match_mname_veryhighsim_mask)
            )
        )
    return accept_mask


def accept_p2_moderate(df_match, masks, thresholds):
    '''
    - Moderate
      - High-similarity fname & high-similarity lname & high-similarity DOB &
        high similarity in one of the additional evidence fields:
          - Common ID edit distance
          - Another name component (mname/altlname)
          - Same zipcode or county
    '''
    dob_partial_mask = masks['dob_partial_mask']
    id_high_mask = masks['id_high_mask']
    loc_exact_mask = masks['loc_exact_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
            (  # close match for name and dob and one other field
                (df_match.fname >= name_high_score) & \
                (df_match.lname >= name_high_score) & \
                dob_partial_mask & (
                    # one of the additional fields: id/mname/altlname
                    id_high_mask | (df_match.mname >= name_high_score) | \
                    (df_match.altlname >= name_high_score) | \
                    (loc_exact_mask)
                    )
            )
    return accept_mask


def accept_p2_relaxed(df_match, masks, thresholds):
    '''
    - Relaxed
      - Moderate/strict, OR
      - High-similarity fname & high-similarity lname &
        (high-similarity DOB | similar common ID edit distance)
    '''
    dob_partial_mask = masks['dob_partial_mask']
    id_high_mask = masks['id_high_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
            (
            # accept all matches with close first/last names and either
            # close birthdates or close common ids
                (df_match.fname >= name_high_score) & \
                (df_match.lname >= name_high_score) & \
                (dob_partial_mask | id_high_mask)
            )
    return accept_mask


def accept_p2_review(df_match, masks, thresholds):
    '''
    - Relaxed
      - Moderate/strict, OR
      - High-similarity fname & high-similarity lname &
        (high-similarity DOB | similar common ID edit distance)
    '''
    dob_partial_mask = masks['dob_partial_mask']
    id_review_mask = masks['id_review_mask']
    name_review_score = thresholds['name_review_score']
    accept_mask = \
            (
            # accept all matches with close first/last names and either
            # close birthdates or close common ids
                (df_match.fname >= name_review_score) & \
                (df_match.lname >= name_review_score) & \
                (dob_partial_mask | id_review_mask)
            )
    return accept_mask


# Pass 3: Block on xf/xl, xl/xf (inverted name soundex)

# Similar logic as pass 2, but use similarity fnamelname
# and lnamefname instead
# NOTE: Change made for common_id null rule in strict and moderate:
#   - Strict: Don't accept matches with exact name/exact DOB/null middle name
#   - Moderate: Accept matches with exact name/DOB if middle name is null

def accept_p3_strict(df_match, masks, thresholds):
    '''
    Check accept_p2_strict logic, using fnamelname/lnamefname instead
    of fname/lname
    Doesn't accept matches with exact name/exact DOB/null middle name
    '''
    id_high_mask = masks['id_high_mask']
    dob_partial_mask = masks['dob_partial_mask']
    dob_exact_mask = masks['dob_exact_mask']
    common_id_null = masks['common_id_null']
    minit_match_mname_veryhighsim_mask = masks['minit_match_mname_veryhighsim_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
                ( # common id are a close match
                    (id_high_mask) & \
                    ( # two match exactly, third is close match
                        (
                            (df_match.fnamelname == 1) & \
                            (df_match.lnamefname == 1) & \
                            (dob_partial_mask)
                        ) | \
                        (
                            (df_match.fnamelname >= name_high_score) & \
                            (df_match.lnamefname == 1) & \
                            (dob_exact_mask)
                         ) | \
                        (
                            (df_match.fnamelname == 1) & \
                            (df_match.lnamefname >= name_high_score) & \
                            (dob_exact_mask)
                         )
                    )
                ) | \
                ( # common id is null for at least one
                    (common_id_null) & \
                    (
                        ( # two match exactly and third is close match and
                          # either middle name or alt name close match
                            (
                                (df_match.fnamelname == 1) & \
                                (df_match.lnamefname == 1) & \
                                (dob_partial_mask)
                            ) | \
                            (
                                (df_match.fnamelname >= name_high_score) & \
                                (df_match.lnamefname == 1) & \
                                (dob_exact_mask)
                             ) | \
                            (
                                (df_match.fnamelname == 1) & \
                                (df_match.lnamefname >= name_high_score) & \
                                (dob_exact_mask)
                             )
                        ) & \
                        (
                            (minit_match_mname_veryhighsim_mask) | \
                            (df_match.altlname >= name_high_score)
                        )
                    )
                ) | \
                ( # common id is not a good match (exact match + one other field)
                    (df_match.fnamelname == 1) & \
                    (df_match.lnamefname == 1) & \
                    (dob_exact_mask) & \
                    (
                        (df_match.altlname == 1) | \
                        (minit_match_mname_veryhighsim_mask)
                    )
                )
    return accept_mask


def accept_p3_moderate(df_match, masks, thresholds):
    '''
    Check accept_p2_moderate logic, using fnamelname/lnamefname instead
    of fname/lname
    Accept matches with exact name/DOB if middle name is null
    '''
    dob_partial_mask = masks['dob_partial_mask']
    dob_exact_mask = masks['dob_exact_mask']
    common_id_null = masks['common_id_null']
    id_high_mask = masks['id_high_mask']
    loc_exact_mask = masks['loc_exact_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
            ((
                (df_match.fnamelname >= name_high_score) & \
                (df_match.lnamefname >= name_high_score) & \
                dob_partial_mask & (
                    # one of the additional fields: id/mname/altlname
                    id_high_mask | (df_match.mname >= name_high_score) | \
                    (df_match.altlname >= name_high_score) | \
                    (loc_exact_mask)
                )
            ) | \
            ( # if no common id, all three are exact match and middle name is null
                (common_id_null) & \
                (df_match.fnamelname == 1) & \
                (df_match.lnamefname == 1) & \
                (dob_exact_mask) & \
                (df_match.mname == -1) & \
                (loc_exact_mask)
            ))
    return accept_mask


def accept_p3_relaxed(df_match, masks, thresholds):
    '''
    Check accept_p2_relaxed logic, using fnamelname/lnamefname instead
    of fname/lname
    '''
    dob_exact_mask = masks['dob_exact_mask']
    id_high_mask = masks['id_high_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask =\
            (# accept all matches with close inverted names and either
            # close birthdates or close common ids
            (df_match.fnamelname >= name_high_score) & \
            (df_match.lnamefname >= name_high_score) & \
            (dob_exact_mask | id_high_mask)
            )
    return accept_mask


def accept_p3_review(df_match, masks, thresholds):
    '''
    - Relaxed
      - Moderate/strict, OR
      - High-similarity fname & high-similarity lname &
        (high-similarity DOB | similar common ID edit distance)
    '''
    dob_partial_mask = masks['dob_partial_mask']
    id_review_mask = masks['id_review_mask']
    name_review_score = thresholds['name_review_score']
    accept_mask = \
            (
            # accept all matches with close first/last names and either
            # close birthdates or close common ids
                (df_match.fnamelname >= name_review_score - .05) & \
                (df_match.lnamefname >= name_review_score - .05) & \
                (dob_partial_mask | id_review_mask)
            )
    return accept_mask

# Pass 4: Block on DOB

# - Strict:
#     - If common ID edit distance is close:
#         - High-similarity fname & high-similarity lname
#     - If common ID is null:
#           exact fname and high similarity lname AND
#           high similarity altlname or minitial/mname good match, i.e.
#              - minitial matches and one is single letter OR
#              - very high-similarity mname
#     - If common ID is not close:
#           Exact fname and high-similarity lname AND
#           altlname is exact match and not null or minitial/mname good match
# - Moderate: Strict OR
#     - High-similarity fname & high similarity lname &
#       high-similarity mname or altlname OR same zip or county
# - Relaxed: Moderate OR
#     - High-similarity in 2 out of 3:
#           fname, lname, common_id

def accept_p4_strict(df_match, masks, thresholds):
    '''
    - Strict:
    - If common ID edit distance is close:
        - High-similarity fname & high-similarity lname
    - If common ID is null:
          exact fname and high similarity lname AND
          high similarity altlname or minitial/mname good match, i.e.
             - minitial matches and one is single letter OR
             - very high-similarity mname
    - If common ID is not close:
          Exact fname and high-similarity lname AND
          altlname is exact match and not null or minitial/mname good match
    '''
    id_high_mask = masks['id_high_mask']
    common_id_null = masks['common_id_null']
    minit_match_mname_veryhighsim_mask = masks['minit_match_mname_veryhighsim_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
            (
                ( # close common id, fname and lname
                    (id_high_mask) & \
                    (df_match.fname >= name_high_score) & \
                    (df_match.lname >= name_high_score)
                ) | \
                ( # null common_id, exact fname, close lname and
                  # one additional field is close
                    (common_id_null) & \
                    (df_match.fname == 1) & \
                    (df_match.lname >= name_high_score) & \
                    (
                        (minit_match_mname_veryhighsim_mask) | \
                        (df_match.altlname >= name_high_score)
                    )
                 ) | \
                ( # bad common id, exact fname, close lname and
                  # one additional field
                    (df_match.fname == 1) & \
                    (df_match.lname >= name_high_score) & \
                    (
                        (minit_match_mname_veryhighsim_mask) | \
                        (df_match.altlname == 1)
                    )
                )
            )
    return accept_mask


def accept_p4_moderate(df_match, masks, thresholds):
    '''
    - Moderate: Strict OR
    - High-similarity fname & high similarity lname &
      high-similarity mname or altlname OR same zip or county
    '''
    loc_exact_mask = masks['loc_exact_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
            ( # fname, lname and one additional field is close
                (df_match.fname >= name_high_score) & \
                (df_match.lname >= name_high_score) & \
                (
                    (df_match.mname >= name_high_score) | \
                    (df_match.altlname >= name_high_score) | \
                    (loc_exact_mask)
                )
            )
    return accept_mask


def accept_p4_relaxed(df_match, masks, thresholds):
    '''
    - Relaxed: Moderate OR
    - High-similarity in 2 out of 3:
      fname, lname, common_id
    '''
    id_high_mask = masks['id_high_mask']
    name_high_score = thresholds['name_high_score']
    accept_mask = \
             ( # two fields of lname, fname and id are close matches
                ((df_match.lname >= name_high_score) & (df_match.fname >= name_high_score)) | (
                (df_match.lname >= name_high_score) & (id_high_mask)) | (
                (id_high_mask) & (df_match.fname >= name_high_score))
             )
    return accept_mask


def accept_p4_review(df_match, masks, thresholds):
    '''
    - Relaxed: Moderate OR
    - High-similarity in 2 out of 3:
      fname, lname, common_id
    '''
    id_review_mask = masks['id_review_mask']
    name_review_score = thresholds['name_review_score']
    accept_mask = (
            (df_match.lname >= name_review_score -.1) & id_review_mask) | (
            (df_match.fname >= name_review_score -.1) &
            (df_match.lname >= name_review_score -.1))
    return accept_mask
