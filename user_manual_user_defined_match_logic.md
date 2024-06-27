# User Manual: User-Defined Match Logic

Users can customize the match logic by changing the following aspects of the match:
- Blocking strategy
- Comparison variables and similarity measures
- Acceptance criteria

Currently, we don't have functionality for users to customize postprocessing (weighting).

This document shows how to change each of those aspects of the match logic.

## User-Defined Blocking Strategy

Blocking strategy is the structure of the blocking passes used to narrow down pairs that could be match candidates at each pass, based on whether they match on a set blocking variables. Generally, we relax the blocking variables as pass increases, because the candidates in an earlier pass are already evaluated and won't be considered in later passes.

To change the blocking strategy, update the following parameters in `config.py`:

- `blocks_by_pass`: The indices of the lists nested within `blocks_by_pass` correspond to the pass number, and each nested list contains the standardized blocking variable names for that pass. 
    - You can add or remove blocking variables and/or passes by editing the nested lists.
    - If you only want to skip a pass in the default blocking strategy, but use the rest of the blocking strategy as is, leave the corresponding nested list for the skipped pass empty. For example, if the two datasets don't have a `common_id` to match on, you can leave `blocks_by_pass[1]` as `[]`. This allows the rest of the match to run on the default match logic without having to edit acceptance critieria.


**NOTE: If you only skip passes in the default blocking strategy but use the rest of the blocking strategy as is, no further action is needed. If you change the number of blocking passes in other ways, you also need to change the *comparison variables and similarity measures* and *acceptance criteria*, since those are affected by blocking strategies.**


## User-Defined Comparison Variables and Similarity Measures

Comparison variables are the pair of personal identifiers we use to calculate to see how similar the records in each candidate pair are to each other in a given blocking pass. The algorithm we use to calculate the similarity score is referred to as a similarity measure. You can change what comparsions to run at each pass by editing:

To change the comparisons run at each pass, update the following parameters in `config.py`:

- `comp_names_by_pass`: The indices of the lists nested within `comp_names_by_pass` correspond to the pass number, which lines up with nested lists in `blocks_by_pass` defined in the section above. Each nested list contains the names of comparisons to run at each pass, which are keys to the `sim_param` variable below. 
-  `sim_param`: Maps each comparison name defined in `comp_names_by_pass` to its corresponding similarity measure as well as the parameters for that measure (e.g. score for missing value, score for swapping birth month and day, etc.).
    - See [recordlinkage library's documentation](https://recordlinkage.readthedocs.io/en/latest/ref-compare.html#module-recordlinkage.compare) on the available similarity measures and their parameters. 
    - It's possible to define your own similarity measures (we use some in our default logic), but it will be more involved. If your match needs custom similarity measures, please contact the Beam Team for further information.
    
**NOTES:**

1. **The config variables above should always be edited at the same time since they affect each other.**

1. **If comparisons are changed, acceptance criteria must be adjusted accordingly, since that's where we evaluate the results of the comparisons.**

1. **Comparisons needs to be adjusted if the number of blocking passes changed.**

## User-Defined Acceptance Criteria

Acceptance criteria are the set of rules we use to evaluate the similarity scores between comparison variables for each blocking pass and strictness level, to decide whether a pair is a match or not. To change that, follow the steps below:

1. Create a custom script named `accept_functions.py` in a separate directory, by making a copy of `./shared/record_linkage_shared/accept_functions.py` and editing the functions to fit your project needs. There should be one function for each level of strictness (strict, moderate, relaxed and review) for each pass.

1. In `config.py`, update `alt_acceptance_dir` with the directory where your custom `accept_functions.py` is saved.

**NOTE**: **Acceptance criteria should be adjusted if there are changes to the number of blocking passes or the comparisons made at each pass.**

