'''
Run a match from start to finish using already preprocessed data, including:
    - Matching (blocking, calculating similarity scores, accepting matched pairs)
    - Postprocessing
    - Calculating match rates
Note that this script does NOT include preprocessing, which needs to be
completed separately before running a match.

The process outputs pairwise scores of all candidates with similarity scores
above the review threshold, as well as final ID crosswalks under strict,
moderate, relaxed, and review. To join an ID crosswalk to an original dataset,
see postprocessing/join_to_orig_template.py for instructions.

Usage:
    run_match.py [-c CONFIG_JSON_FILEPATH]

    Arguments:
        -c, --config_json_filepath: filepath to copy of config_json in project directory
'''
import logging
import subprocess
import sys
import shutil
import shlex
import argparse
import datetime


def run_subprocess_and_print_output(command_line):
    command_line_args = shlex.split(command_line)
    result = subprocess.run(
        command_line_args,
        capture_output=True,
        text=True,
        check=True
        )
    print(result.stdout)
    print(result.stderr)

# Initialize the Python logging module
logging.basicConfig(format='%(asctime)s [record_linkage]: %(message)s',
                    datefmt='%H:%M', level=logging.DEBUG, stream=sys.stdout)

# Get start time
startTime = datetime.datetime.now()

# Creating parser object used to receive config.json's filepath
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config_json_filepath", required=False,
help="Filepath to a copy of the config.json created by config.py")
args = parser.parse_args()

logging.info('Running match')

# Copy a version of the specified config.json to the working directory
if args.config_json_filepath:
    logging.info(f'Copying config.json from {args.config_json_filepath}')
    shutil.copy(args.config_json_filepath, "./config.json")
else:
    logging.info('Using existing config.json in the working directory')

# Match workflow
logging.info('Matching')
run_subprocess_and_print_output("python ./matching/match.py")

logging.info('Postprocessing')
run_subprocess_and_print_output("python ./postprocessing/postprocess.py")

logging.info('Calculating match rates')
run_subprocess_and_print_output("python ./match_rates/get_match_rates.py")

logging.info(f'Match complete - run time: {datetime.datetime.now() - startTime}')