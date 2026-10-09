"""
Extraction and transformation of Neo4j entity data for the
globus-downloads-to-JSON pipeline's File Downloads.  Output is to JSON
files created during an earlier phase of this pipeline.

N.B. This ETL of File Downloads logged file transfers is assumed
     to be a nightly process, even if delivery of data means there are
     many nights with little or no change (e.g. waiting for a new
     "usage details" spreadsheet from Globus most of the month.)

N.B. The ETL processes all logged file download events that it can,
     without being restrained by the delivery dates of other data, such
     as the Globus Usage Details spreadsheet.  Placeholder values
     are inserted during earlier phases of this pipeline, such as the
     ones this process tries to update.  These placeholder remain
     unchanged until the new enough information is received to
     update them or mark them UNRESOLVED.

Sentinel files should exist along with each JSON file with file transfer
content.  Comments in the analytics-platform-loading loading process
describe the loader's interpretation of sentinel files.  This
globus-downloads-to-JSON process's manipulation of sentinel files is
described in the comments of the gridftp_log_extract.py file.

The content JSON files this process creates may contain information not
loaded into the DuckDB tables, notably the `provenance` field.
"""
from __future__ import annotations

import os
import argparse
import sys
import configparser
import logging
import copy
import re
import json
import ast
from zoneinfo import ZoneInfo
from datetime import datetime
from pathlib import Path

# Import project resources
from log_extract_xfer_utils import LogExtractXferUtils
from entity_info_provider import EntityInfoProvider

tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)

print('The augment-with-entity-info phase: resolving PENDING entity_type/dataset_type/application_id fields (currently: Neo4j, eventually: entity-api)')

# This script's own Slack header emoji: derived from its parent directory name
# ('phase2' -> ':two:'), so relocating a script to a different phase directory
# automatically updates its emoji with no code change. Falls back to ':diamonds:'
# if the parent directory doesn't match the 'phaseN' pattern.
_PHASE_NUMBER_WORDS = {1: 'one', 2: 'two', 3: 'three', 4: 'four', 5: 'five', 6: 'six', 7: 'seven', 8: 'eight', 9: 'nine'}
_phase_dir_match = re.match(r'^phase(\d+)$', Path(__file__).resolve().parent.name)
if _phase_dir_match and int(_phase_dir_match.group(1)) in _PHASE_NUMBER_WORDS:
    SLACK_PHASE_EMOJI = f":{_PHASE_NUMBER_WORDS[int(_phase_dir_match.group(1))]}:"
else:
    SLACK_PHASE_EMOJI = ':diamonds:'

# arg_project_dir is provided as an argument by the .sh wrapper calling this program from
# its BASH_SOURCE-derived PROJECT_DIR (e.g. globus-downloads-to-JSON).
arg_parser = argparse.ArgumentParser(description='The augment-with-entity-info phase: resolve PENDING entity_type/dataset_type/application_id fields via entity info lookups.')
arg_parser.add_argument('--project-dir', required=True, dest='project_dir',
                         help="This process's own directory (one level above src/), e.g."
                              " .../log-processing/globus-downloads-to-JSON. Normally supplied"
                              " by the .sh wrapper's own PROJECT_DIR.")
args = arg_parser.parse_args()
arg_project_dir = args.project_dir
arg_portfolio_dir = os.path.dirname(arg_project_dir) # one level above arg_project_dir

#
# Read configuration from the project INI file and set global constants
#
Config = configparser.ConfigParser()

# NOTE: this script lives one directory deeper than before (src/phase4/, not src/
# directly), so its own vm001-default candidate needs the extra phase4/ segment, and
# the PyCharm-dev candidate needs an extra '../' level.
process_ini_candidates = [
    Path('augment_with_entity_info.ini'),                                              # Docker WORKDIR
    Path(f'{arg_project_dir}/src/phase2/augment_with_entity_info.ini'),                # vm001 default
    Path('../../../globus-downloads-to-JSON/src/phase2/augment_with_entity_info.ini'), # PyCharm dev
]
config_file_name = None
for candidate in process_ini_candidates:
    if candidate.is_file():
        config_file_name = str(candidate.resolve())
        break
if not config_file_name:
    print(f"\a\nUnable to find augment_with_entity_info.ini in any expected location.\n")
    sys.exit(3)
Config.read(config_file_name)
try:
    # The PROJECT_NAME pulled from the INI file should match the script variable PROJECT_DIR
    # in the bash script executing this program.
    # PROJECT_NAME must match the PROJECT_NAME used by globus_access_log_extract.py and
    # gridftp_log_extract.py -- that's the directory under JSON_FILE_NIGHTLY_DIR where
    # this process reads sentinel files for completion markers.
    PROJECT_NAME = Config.get('ProcessSpecificSettings', 'PROJECT_NAME')
    SLACK_NOTIFICATION_CHANNEL = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATION_CHANNEL')
    SLACK_BAD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_BAD_NEWS_EMOJI')
    SLACK_GOOD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_GOOD_NEWS_EMOJI')
    SLACK_NEUTRAL_INFO_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_NEUTRAL_INFO_EMOJI')
    SLACK_NOTIFICATIONS = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATIONS')
    slack_user_id_mentions_on_error_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_ERROR'))
    slack_user_id_mentions_on_success_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_SUCCESS'))
except Exception as e:
    print(f"\a\nUnable to read configuration from '{config_file_name}'.\n")
    sys.exit(3)
print('Process-specific configuration loaded')

#
# Set up a logger in the configured directory for the current execution.
#
exec_info_dir_candidates = [
    Path('exec_info'),                          # Docker WORKDIR
    Path(f'{arg_project_dir}/exec_info'),       # vm001 default 
    Path(f'../../../{PROJECT_NAME}/exec_info'), # PyCharm dev   
]
exec_info_dir = None
for candidate in exec_info_dir_candidates:
    if candidate.is_dir():
        exec_info_dir = str(candidate.resolve())
        break
if not exec_info_dir:
    print(f'Unable to find exec_info directory in any expected location.')
    sys.exit(3)
log_file_name = f"{exec_info_dir}" \
                f"/augment_with_entity_info-" \
                f"{datetime.now().strftime('%Y-%m-%d_%H%M%s')}" \
                f".log"
logging.basicConfig(filename=log_file_name
                    ,level=logging.INFO
                    ,format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                    ,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

print('Logger instantiated')

portfolio_utils = None
try:
    config_file_location = None
    candidates = [
        Path('logProcessingProject.ini'),                          # Docker WORKDIR
        Path(f'{arg_portfolio_dir}/src/logProcessingProject.ini'), # vm001 default 
        Path('../../../src/logProcessingProject.ini'),             # PyCharm dev   
    ]
    for candidate in candidates:
        if candidate.is_file():
            config_file_location = candidate.resolve()
            break
    portfolio_utils = LogExtractXferUtils(config_file_name=config_file_location
                                          , disable_slack_notifications=(SLACK_NOTIFICATIONS == 'DISABLED'))
    portfolio_config = portfolio_utils.get_config()
    print('Shared log processing configuration loaded.')
    JSON_FILE_NIGHTLY_DIR = portfolio_config['JSON_FILE_NIGHTLY_DIR']
    node_dir_list = ast.literal_eval(portfolio_config['NODE_LOG_DIR_LIST'])
    logger.info("LogExtractXferUtils instantiated.")
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded')

entity_info_provider = EntityInfoProvider(portfolio_utils=portfolio_utils)

#
# Set more global constants specific to parsing files and generating JSON.
#
# Set up a paths with PCRE regular expressions for sentinel files indicating the
# previous phase has been done, and that this phase and subsequent phases have not.
STAGE1_DONE_PATTERNS = [
    re.compile(r'^globus_access_log-\d{8}\.json\.DONE\.1$')
    ,re.compile(r'^gridftp\.log-\d{8}\.json\.DONE\.1$')
]

# The key this process uses for its own entry in each JSON object's provenance dict.
UPDATER_PROVENANCE_KEY = Path(__file__).stem

# Hardcoded labels for checking the entities returned by
# entity_info_provider.py's DATASET_INFO_QUERY
# (MATCH (d:Dataset|Upload|Publication)).
KNOWN_ENTITY_TYPES = frozenset({'Dataset', 'Upload', 'Publication'})

# Order matters: entity_type MUST resolve before dataset_type within the
# same pass (see resolve_entity_info_fields below), since dataset_type's
# own resolution now gates on entity_type's already-resolved value.
#
# A record whose entity_type isn't (or doesn't resolve to) 'Dataset' gets
# dataset_type='NA'.
#
# application_id applies universally regardless of entity_type, and
# its position in this ordering doesn't matter.
AUGMENTABLE_FIELD_ORDER = (
    ('entity_type', None),
    ('dataset_type', 'Dataset'),
    ('application_id', None),
)

# Verify any expectations about the configuration are valid. Print
# messages for each expectation not met and halt if there are any.
def verify_configuration_expectations():
    global node_dir_list

    exit_rather_than_return=False
    if not os.path.exists(JSON_FILE_NIGHTLY_DIR):
        print(f"Halting program due to not finding JSON_FILE_NIGHTLY_DIR at "
              f"'{JSON_FILE_NIGHTLY_DIR}'")
        exit_rather_than_return = True
    if not os.path.exists(exec_info_dir):
        print(f"Halting program due to not finding exec_info_dir at "
              f"'{exec_info_dir}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    for node_dir in node_dir_list:
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}{node_dir}"
        if not os.path.exists(node_json_dir_fullpath):
            print(f"Halting program due to not finding an expected node JSON directory at "
                  f"'{node_json_dir_fullpath}'")
            exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":red_circle: {portfolio_utils.get_slack_host_context()} :red_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :red_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Halted trying to verify configuration expectations.\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_red_square::skull_and_crossbones: ' * 5}\n"
                    f":red_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)


# Given one transfer record, resolve field_name (one of
# AUGMENTABLE_FIELD_ORDER) if it's still 'PENDING', using
# dataset_info_crosswalk (keyed by entity_uuid, each value a dict with
# one entry per augmentable field -- see
# EntityInfoProvider.get_dataset_info_crosswalk()).
#
# requires_entity_type, if given, gates this field to records whose
# entity_type has ALREADY resolved (earlier in the same pass -- see
# AUGMENTABLE_FIELD_ORDER) to exactly that value. A record whose
# entity_type is anything else -- a different real entity_type, or itself
# still UNRESOLVED/UNTRACKED -- gets this field set to 'NA': the field
# genuinely doesn't apply, which is a different, non-failure outcome from
# a real resolution failure. This is NOT the same check as known_values:
# known_values validates a value already fetched from the crosswalk;
# requires_entity_type decides whether to even attempt that fetch.
#
# known_values, if given, is dataset_type's own extra check (against a
# controlled vocabulary) -- pass None to skip it entirely (application_id
# and entity_type work this way, though entity_type gets its own fixed
# set applied by the caller, see KNOWN_ENTITY_TYPES).
#
# Outcomes:
# - 'NA', if requires_entity_type is given and doesn't match this record's
#   already-resolved entity_type.
# - a real value, if the crosswalk has a matching uuid whose value for
#   this field is truthy (and, if known_values is given, in that set).
# - 'UNRESOLVED' + logged ERROR, if entity_uuid isn't in the crosswalk at
#   all -- a real Dataset/Upload/Publication uuid extracted from the logs
#   should exist in Neo4j; not finding it there at all is surprising
#   enough to warrant investigation, not just a quiet UNRESOLVED.
# - 'UNRESOLVED' + logged WARNING, if entity_uuid IS in the crosswalk but
#   this field's value is null/empty, or (when known_values is given) not
#   in that set.
#
# Returns True if a real value was resolved this call, False otherwise
# (not 'PENDING' to begin with, the NA gate, or any UNRESOLVED outcome
# above). Does NOT stamp provenance -- resolve_entity_info_fields does
# that once per record, after every augmentable field has been attempted.
def resolve_entity_info_field(transfer_record:dict, field_name:str, dataset_info_crosswalk:dict,
                              known_values:set|None, requires_entity_type:str|None=None) -> bool:
    if transfer_record.get(field_name) != 'PENDING':
        return False

    if requires_entity_type is not None and transfer_record.get('entity_type') != requires_entity_type:
        transfer_record[field_name] = 'NA'
        return False

    entity_uuid = transfer_record.get('entity_uuid')
    if entity_uuid not in dataset_info_crosswalk:
        logger.error(f"entity_uuid={entity_uuid!r} not found in the dataset info"
                     f" crosswalk at all; marking {field_name}='UNRESOLVED'.")
        transfer_record[field_name] = 'UNRESOLVED'
        return False

    real_value = dataset_info_crosswalk[entity_uuid].get(field_name)
    if not real_value:
        logger.warning(f"entity_uuid={entity_uuid!r} found in the dataset info"
                       f" crosswalk, but its {field_name} is null/empty; marking"
                       f" {field_name}='UNRESOLVED'.")
        transfer_record[field_name] = 'UNRESOLVED'
        return False

    if known_values is not None and real_value not in known_values:
        logger.warning(f"entity_uuid={entity_uuid!r} has {field_name}="
                       f"{real_value!r}, which is not in the {len(known_values)}"
                       f" known values; marking {field_name}='UNRESOLVED'.")
        transfer_record[field_name] = 'UNRESOLVED'
        return False

    transfer_record[field_name] = real_value
    return True


# Wraps resolve_entity_info_field() across every field in
# AUGMENTABLE_FIELD_ORDER, IN ORDER (entity_type before dataset_type is
# load-bearing, not incidental -- see AUGMENTABLE_FIELD_ORDER's own
# comment), for one transfer record, then stamps provenance once -- the
# same overall shape resolve_dataset_type_field() originally had,
# generalized to however many fields this phase is responsible for
# augmenting. Records that were already 'UNTRACKED' for a given field
# (entity_uuid was None at extraction time -- nothing to look up) are
# left untouched for that field, the same permanent, structural outcome
# UNTRACKED already is elsewhere in this portfolio.
def resolve_entity_info_fields(transfer_record:dict, dataset_info_crosswalk:dict, known_dataset_types:set,
                               run_provenance:dict) -> tuple[dict,bool]:
    resolved_any = False
    for field_name, requires_entity_type in AUGMENTABLE_FIELD_ORDER:
        if field_name == 'entity_type':
            known_values = KNOWN_ENTITY_TYPES
        elif field_name == 'dataset_type':
            known_values = known_dataset_types
        else:
            known_values = None
        if resolve_entity_info_field(transfer_record, field_name, dataset_info_crosswalk,
                                      known_values, requires_entity_type):
            resolved_any = True
    if 'provenance' in transfer_record:
        transfer_record['provenance'][UPDATER_PROVENANCE_KEY] = copy.deepcopy(run_provenance)
    return transfer_record, resolved_any

# Find files ready for this process by matching STAGE1_DONE_PATTERNS for sentinel files.
def get_processable_markers():
    global node_dir_list

    marker_files = []
    for node_dir in node_dir_list:
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}{node_dir}"
        for f in Path(node_json_dir_fullpath).iterdir():
            if not f.is_file():
                continue
            if not any(pattern.fullmatch(f.name) for pattern in STAGE1_DONE_PATTERNS):
                continue
            marker_files.append(str(f))
    return marker_files


# Find files ready for this process by matching STAGE1_DONE_PATTERNS for sentinel files.
def process_marker_file(marker_filename:str, dataset_info_crosswalk:dict, known_dataset_types:set):
    if not marker_filename.endswith('.DONE.1'):
        logger.error(f"Marker '{marker_filename}' doesn't match the expected stage-1-done"
                     f" pattern; skipping.")
        return None

    data_filename = re.sub(r'\.DONE\.1$', '', marker_filename)
    if not os.path.isfile(data_filename):
        logger.error(f"Marker '{marker_filename}' exists but data file '{data_filename}' does not; skipping.")
        return None

    with open(data_filename, 'r') as f:
        transfer_records = json.load(f)

    run_provenance = {
        'process_script' : os.path.basename(__file__)
        , 'process_utc_dt': datetime.now(tz_utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        , 'local_file': data_filename
    }

    resolved_count = 0
    for idx, transfer_record in enumerate(transfer_records):
        transfer_records[idx], resolved = resolve_entity_info_fields(transfer_record=transfer_record
                                                                      , dataset_info_crosswalk=dataset_info_crosswalk
                                                                      , known_dataset_types=known_dataset_types
                                                                      , run_provenance=run_provenance)
        if resolved:
            resolved_count += 1

    with open(data_filename, "w") as jf:
        jf.write(json.dumps(transfer_records))

    new_marker = f"{data_filename}.DONE.1.2"
    os.rename(marker_filename, new_marker)
    logger.info(f"Wrote {len(transfer_records)} records ({resolved_count} newly resolved) to"
                f" '{data_filename}'; marker is now '{new_marker}'.")
    return data_filename

if __name__ == '__main__':
    msg =   f":red_circle: {portfolio_utils.get_slack_host_context()} :red_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :red_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to resolve PENDING entity_type/dataset_type/application_id fields in" \
            f" JSON files at {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}\n" \
            f" using entity info lookups.\n" \
            f" Process logging to {log_file_name}\n" \
            f":red_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()

    # Loaded exactly once for the whole run, pulling the entire Neo4j crosswalk
    # data into memory. Every subsequent lookup reuses this same instance.
    try:
        dataset_info_crosswalk = entity_info_provider.get_dataset_info_crosswalk()
        logger.info(f"Loaded dataset info crosswalk with {len(dataset_info_crosswalk)} entries.")
        known_dataset_types = entity_info_provider.get_known_dataset_types()
        logger.info(f"Loaded {len(known_dataset_types)} known dataset_type value(s).")
    except Exception as e:
        bad_news = (f":red_circle: {portfolio_utils.get_slack_host_context()} :red_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :red_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Unable to load the dataset info crosswalk or known values: {e}\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_red_square::skull_and_crossbones: ' * 5}\n"
                    f":red_circle:")
        logger.exception('Unable to load the dataset info crosswalk or known values.')
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)

    marker_files = get_processable_markers()
    logger.info(f"Found {len(marker_files)} files ready to process (stage-1-done).")

    advanced_to_done_count = 0
    failed_count = 0
    for marker_filename in marker_files:
        try:
            result = process_marker_file(marker_filename=marker_filename
                                         ,dataset_info_crosswalk=dataset_info_crosswalk
                                         ,known_dataset_types=known_dataset_types)
            if not result:
                failed_count += 1
                continue
            advanced_to_done_count += 1
        except Exception as e:
            logger.exception(f"Error processing '{marker_filename}'.")
            failed_count += 1

    if failed_count > 0:
        bad_news = (f":red_circle: {portfolio_utils.get_slack_host_context()} :red_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :red_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" {failed_count} of {failed_count + advanced_to_done_count} files could not be processed. See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_red_square::skull_and_crossbones: ' * 5}\n"
                    f":red_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)

    process_utc_finish = datetime.now(tz_utc)
    good_news = (f":red_circle: {portfolio_utils.get_slack_host_context()} :red_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :red_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" {advanced_to_done_count} files advanced to DONE, {failed_count} failed.\n"
                 f" Process logged to {log_file_name}\n"
                 f"{':heart: ' * 5}\n"
                 f":red_circle:")
    logger.info(good_news)
    try:
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=good_news
                                           , mentions_dict=slack_user_id_mentions_on_success_dict)
    except Exception as e:
        logger.exception('Unable to post Slack success notification.')

    sys.exit(0 if failed_count == 0 else 2)
