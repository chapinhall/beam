# User Manual: Preprocessing

Follow these steps to use Beam's preprocessing functions:

1. **Create preprocessing scripts**:
	- For each dataset to be preprocessed, create a script based on ```preprocess_file_template.py```. If multiple files need to be processed, a preprocessing script must be created for each file.
	- Make sure to follow the naming convention for preprocessing scripts described at the top of ```preprocess_file_template.py```.
	- Store the preprocessing script(s) in the **repository for the research project** associated with the link.
1. **Update config file**:
	- In `config.py`, for each dataset, update the parameters for preprocessing raw data, including:
		- `project_repo`: Path to the research project's repo, where preprocessing scripts are saved. Enter the path from user's home directory (e.g. `gitlab/analysis123/`)
		- Any previously preprocessed data that should be added to this batch of
	preprocessed data, to create a cumulative dataset for linkage:
			- `combined_prev_tbl`: previously preprocessed data stored as a Postgres table in the same schema as the current schema in `db_args`
			- `combined_prev_csv`: previously preprocessed data in csv format
	- Run `python config.py` to create an updated `./config.json`.
1. Run preprocessing:
	- From the top level of the record linkage repository, run ```python preprocessing/run_preprocess.py```