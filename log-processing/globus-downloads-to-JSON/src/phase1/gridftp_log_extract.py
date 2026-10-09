"""
Extraction and transformation of Globus Grid FTP file transfers for the
globus-downloads-to-JSON pipeline's File Downloads.  Output is to JSON
files sourced by a load process.

N.B. This ETL of File Downloads logged file transfers is assumed
     to be a nightly process, even if delivery of data means there are
     many nights with little or no change (e.g. waiting for a new
     "usage details" spreadsheet from Globus most of the month.)

N.B. The ETL processes all logged file download events that it can,
     without being restrained by the delivery dates of other data, such
     as the Globus Usage Details spreadsheet.  Therefore placeholder values
     are inserted where later phases of this pipeline may add information
     like the user's identity or geolocation information derived from the
     IP address logged for a file transfer.

Sentinel files should exist along with each JSON file with file transfer
content.  Comments in the analytics-platform-loading loading process
describe the loader's interpretation of sentinel files.  The processes in
this extraction and transformation pipeline use sentinel as follows.

1. The early phases of this pipeline are only run once.  Running them
   again would provide no additional information.  Therefore, after
   these phases are run, sentinel files will indicate DONE.
   - Phase 1 covers extraction from logged events. Logs are never
     revised, so after running, sentinel files will indicate *.DONE.1.
   - Phase 2 covers transformation using entity information from Neo4j.
     During this phase, immutable information for an entity is added to
     the content JSON.  After running, sentinel files indicate *.DONE.1.2.
   - Phase 3 covers transformation using the IP address pulled from the
     logs during Phase 1 to identify geolocation information for the transfer.
     During this phase, immutable information for an entity is added to
     the content JSON.  After running, sentinel files indicate *.DONE.1.2.3.
2. Later phases of this pipeline may run repeatedly without providing
   new results. During that time, they retain sentinel files in the
   LOADED state. Once they have the result which they will always provide, the
   sentinel file switches to the DONE state.
   - Phase 4 cover transformation using the Globus Usage Details data delivered
     nightly, but only containing new data after the previous month ends. Most
     nights the content JSON files will be associated with sentinel files named
     *.LOADED.1.2.3.4.  But when new data arrives, the PENDING fields in the
     content JSON will be replaced with the best information available, which
     will either be user data or UNRESOLVED.  After this, no new information
     for this time period is expected, and the sentinel file becomes *.DONE.1.2.3.4.
3. On each run, if there are any duplicate sentinel files, each one is logged, then
   the process exits.
2. The content associated with LOADED sentinel files is used to create
   the hot_file_download table.  This should reflect logged file transfers
   for which more information may eventually be received.  The table is
   simply dropped and created from LOADED-associated content each time.
   This is a quick operation because only the current and maybe previous
   month have partial information.  All other file transfers have as much
   information as they will ever have.
3. The content associated with DONE sentinel files is used to create
   the file_download table. A ledger kept in file_download_ledger tracking
   the sentinel files seen during previous loads.
   3.1 If the current sentinel file matches the sentinel file of the last
       load, the content is skipped.
   3.2 If the sentinel file suffix reflects a well-formed pattern indicating
       additional globus-downloads-to-JSON pipeline "phases" have been run
       on the content, the content is reloaded.
   3.3 Other changes to the sentinel file log an error, and the content is
       skipped.

The content JSON files this process creates may contain information not
loaded into the DuckDB tables, notably the `provenance` field.
"""
import os
import argparse
import subprocess
import sys
import configparser
import logging
import copy
import re
import glob
import time
from zoneinfo import ZoneInfo
from datetime import datetime
import gzip
import json
import ast
from collections import defaultdict
from pathlib import Path

# Import project resources
from log_extract_xfer_utils import LogExtractXferUtils
from log_extract_xfer_utils import LogFileStatusType
from log_extract_xfer_utils import parse_datetime_flexible

tz_pgh = ZoneInfo(key='America/New_York')
tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)
epoch_utc = datetime.strptime('1970-01-01T00:00:00.000Z','%Y-%m-%dT%H:%M:%S.%fZ').astimezone(tz_utc)

print('Processing file transfer entries in Globus logs')

# This script's own Slack header emoji: derived from its parent directory name
# ('phase1' -> ':one:'), so relocating a script to a different phase directory
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
arg_parser = argparse.ArgumentParser(description='Extract Globus GridFTP file-transfer entries to JSON.')
arg_parser.add_argument('--project-dir', required=True, dest='project_dir',
                         help="This process's own directory (one level above src/), e.g."
                              " .../log-processing/globus-downloads-to-JSON. Normally supplied"
                              " by the .sh wrapper's own PROJECT_DIR.")
args = arg_parser.parse_args()
arg_project_dir = args.project_dir
arg_portfolio_dir = os.path.dirname(arg_project_dir)

#
# Read configuration from the project INI file and set global constants
#
Config = configparser.ConfigParser()

# NOTE: this script lives one directory deeper than before (src/phase1/, not src/
# directly), so its own vm001-default candidate needs the extra phase1/ segment, and
# the PyCharm-dev candidate needs an extra '../' level.
process_ini_candidates = [
    Path('gridftp_log_extract.ini'),                                              # Docker WORKDIR
    Path(f'{arg_project_dir}/src/phase1/gridftp_log_extract.ini'),                # vm001 default
    Path('../../../globus-downloads-to-JSON/src/phase1/gridftp_log_extract.ini'), # PyCharm dev
]
config_file_name = None
for candidate in process_ini_candidates:
    if candidate.is_file():
        config_file_name = str(candidate.resolve())
        break
if not config_file_name:
    print(f"\a\nUnable to find gridftp_log_extract.ini in any expected location.\n")
    sys.exit(3)
Config.read(config_file_name)
try:
    # The PROJECT_NAME pulled from the INI file should match the script variable PROJECT_DIR
    # in the bash script executing this program.
    PROJECT_NAME=Config.get('ProcessSpecificSettings', 'PROJECT_NAME')
    LOG_FILE_NIGHTLY_DIR = Config.get('ProcessSpecificSettings', 'LOG_FILE_NIGHTLY_DIR')
    PUBLIC_DIR_PREFIX = Config.get('ProcessSpecificSettings', 'PUBLIC_DIR_PREFIX')
    CONSORTIUM_DIR_PREFIX = Config.get('ProcessSpecificSettings', 'CONSORTIUM_DIR_PREFIX')
    PROTECTED_DIR_PREFIX = Config.get('ProcessSpecificSettings', 'PROTECTED_DIR_PREFIX')
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
# exec_info is at PROJECT_DIR/exec_info, where PROJECT_DIR is one level above src/.
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
                f"/gridftp_log_extract-" \
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
    ABS_PATH_BASE_TO_REMOVE = portfolio_config['ABS_PATH_BASE_TO_REMOVE']
    node_dir_list = ast.literal_eval(portfolio_config['NODE_LOG_DIR_LIST'])
    logger.info("LogExtractXferUtils instantiated.")
    print('LogExtractXferUtils instantiated.')
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded')

#
# Set more global constants specific to parsing files and generating JSON.
#
# List of regular expressions match lines within one logged session, which indicate that
# session is of interest to the File Downloads project for file transfer events.
SESSION_RETAIN_LINE_RE_LIST = [ \
                                {"session_interesting_indicator": True
                                 , "payload_re": "Finished transferring .*"} \
                                ,{"session_interesting_indicator": False
                                  , "payload_re": "Starting to transfer .*"} \
                                ,{"session_interesting_indicator": False
                                  , "payload_re": "Sharee \'[^\']*\' is restricted to \'/hive/hubmap/data/public\'.*"} \
                                ,{"session_interesting_indicator": False
                                  , "payload_re": "Transfer stats: .*"} \
                                ,{"session_interesting_indicator": True
                                  , "payload_re": "Failure attempting to transfer .*"} \
                                ,{"session_interesting_indicator": True
                                  , "payload_re": "Transfer failure:.*"} \
                                ,{"session_interesting_indicator": False
                                  , "payload_re": "[SERVER]: [0-9]* Transfer .*"}
                            ]

# More PCRE regular expressions for matching and retention of
# data in creating the JSON output for events of interest.
RE_NEW_SESSION_LINE = r'^\[[0-9]+\] .* :: .*'
RE_FOULED_UP_DOUBLE_PID_LINE = r'.*\[[0-9]+\].*\[[0-9]+\].*'
RE_FOULED_UP_MID_PID_LINE = r'..*\[[0-9]+\].*'
GRIDLOG_DATE_FORMAT = '%a %b %d %H:%M:%S %Y'
XFER_STATS_TO_RETAIN = ['START_UTC','FILE','NBYTES','DEST','TASKID']

now_utc = datetime.now(tz_utc)

verbose=True

# Verify any expectations about the configuration are valid. Print
# messages for each expectation not met and halt if there are any.
def verify_configuration_expectations():
    global node_dir_list
    
    exit_rather_than_return=False
    if not os.path.exists(LOG_FILE_NIGHTLY_DIR):
        print(f"Halting program due to not finding LOG_FILE_NIGHTLY_DIR at "
              f"'{LOG_FILE_NIGHTLY_DIR}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    if not os.path.exists(JSON_FILE_NIGHTLY_DIR):
        print(f"Halting program due to not finding JSON_FILE_NIGHTLY_DIR at "
              f"'{JSON_FILE_NIGHTLY_DIR}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    if not os.path.exists(exec_info_dir):
        print(f"Halting program due to not finding exec_info_dir at "
              f"'{exec_info_dir}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    for node_dir in node_dir_list:
        node_log_dir_fullpath = f"{LOG_FILE_NIGHTLY_DIR}{os.sep}{node_dir}{os.sep}gridftp-log"
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}{node_dir}"
        if not os.path.exists(node_log_dir_fullpath):
            print(f"Halting program due to not finding an expected node log directory at "
                  f"'{node_log_dir_fullpath}'")
            exit_rather_than_return = True
        if not os.path.exists(node_json_dir_fullpath):
            print(f"Halting program due to not finding an expected node JSON directory at "
                  f"'{node_json_dir_fullpath}'")
            exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":large_purple_circle: {portfolio_utils.get_slack_host_context()} :large_purple_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_purple_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Halted trying to verify configuration expectations.\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_purple_square::skull_and_crossbones: ' * 5}\n"
                    f":large_purple_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)
        
# Read all the lines from the specified GZip file, and
# return them in an ordered list.
def get_log_lines_from_gzip_file(file_name):
    log_file_lines=[]
    if os.path.isfile(file_name):
        with gzip.open(file_name, 'rb') as f:
            for line in f:
                log_file_lines.append(line.decode())
    else:
        logger.error(f"File '{file_name}' not found.")
    return log_file_lines

# Given a list of strings for each line in a Grid FTP log, parse each into a dict, tack a
# dict with provenance info on each, and accumulate all the per-line dicts to a list to return.
def create_dict_by_session_from_log_lines(log_file_lines):

    fouled_up_line_counter = 0
    current_session_ID = None
    session_log_lines_dict = {}
    for idx, logFileLine in enumerate(log_file_lines):
        try:
            if re.match(RE_FOULED_UP_DOUBLE_PID_LINE, logFileLine):
                logger.debug(f"Skipped processing line {idx+1} because there appears to be more"
                             f" than one session ID on the line.")
                fouled_up_line_counter = fouled_up_line_counter + 1
            elif re.match(RE_FOULED_UP_MID_PID_LINE, logFileLine):
                logger.debug(f"Skipped processing line {idx+1} because an apparent session ID"
                             f" occurs after the start of the line.")
                fouled_up_line_counter = fouled_up_line_counter + 1
            elif re.match(RE_NEW_SESSION_LINE, logFileLine):
                logFileLineKey, logFileLinePayload = re.split(r'::', logFileLine, 1)
                logFileLineKeyPID, logFileLineKeyDate = re.split(r'\s', logFileLineKey, 1)
                logFileLinePID = int(logFileLineKeyPID[1:-1])
                logFileLineTime=time.strptime(logFileLineKeyDate.strip(), GRIDLOG_DATE_FORMAT)
                current_session_ID = logFileLinePID
                current_line_dict = { 'line_num': idx+1, 'logged_time': logFileLineTime, 'payload': logFileLinePayload.strip() }
            else:
                current_line_dict = { 'line_num': idx+1, 'logged_time': logFileLineTime, 'payload': logFileLine.strip() }
            if current_session_ID in session_log_lines_dict:
                session_log_lines_dict[current_session_ID]['lines'].append(current_line_dict)
            else:
                session_log_lines_dict[current_session_ID] = {'lines': [current_line_dict]}
        except Exception as e:
            logger.exception(f"Skipped processing line {idx+1} due to exception")
            fouled_up_line_counter = fouled_up_line_counter + 1
    if fouled_up_line_counter > 0:
        logger.info(f"A total of {fouled_up_line_counter} lines of input were skipped due to"
                    f" formatting of the gridftp log.")
    return session_log_lines_dict

def pull_interesting_sessions(session_log_lines_dict):
    global SESSION_RETAIN_LINE_RE_LIST

    interesting_sessions_list = []
    for pid in session_log_lines_dict:
        session_dict = {'pid': pid, 'required_line_count': 0, 'interesting_lines': []}
        for line in session_log_lines_dict[pid]['lines']:
            for re_dict in SESSION_RETAIN_LINE_RE_LIST:
                if re.match(re_dict['payload_re'], line['payload']):
                    if re_dict['session_interesting_indicator']:
                        session_dict['required_line_count'] = session_dict['required_line_count']+1
                    session_dict['interesting_lines'].append(line)
        if session_dict['required_line_count'] > 0:
            interesting_sessions_list.append(session_dict)
    return interesting_sessions_list

def generate_stats_for_finished_transfers(interesting_sessions_list):

    session_transfer_stats_list=[]
    for session in interesting_sessions_list:
        apparent_entity_uuid=''
        for line in session['interesting_lines']:
            session_transfer_stats_dict = {}
            payload = line['payload']
            if re.match('Transfer stats: .* TYPE=STOR .*', payload):
                logger.debug(f"For session {session['pid']}, not interested in 'Transfer stats' with TYPE=STOR.")
                continue
            elif re.match('Transfer stats: .* TYPE=RETR .*', payload):
                session_transfer_stats_dict['stats_line_num'] = line['line_num']
                key=value=None
                for payload_token in re.split('([^= ]+=)',payload.replace('Transfer stats: ','').strip()):
                    if payload_token[-1:] == '=':
                        key=payload_token[0:-1]
                    else:
                        value=payload_token.strip()
                        if key == 'FILE':
                            filepath_tokens = re.split(os.sep, value)
                            try:
                                apparent_entity_uuid = filepath_tokens[5] if filepath_tokens[4] == 'public' else filepath_tokens[6]
                            except Exception as e:
                                logger.debug(f"While trying to parse entity UUID in"
                                             f" {str(filepath_tokens)} got e={str(e)}.")
                    if key and value:
                        try:
                            if key == 'START':
                                t = parse_datetime_flexible(value)
                                key = 'START_UTC'
                                value = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            if key == 'DATE':
                                t = parse_datetime_flexible(value)
                                key = 'DATE_UTC'
                                value = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            if key == 'FILE':
                                value = value.replace(ABS_PATH_BASE_TO_REMOVE
                                                      ,''
                                                      ,1)
                        except Exception as e:
                            logger.error(f"Session {session['pid']}"
                                         f" appears to be a file transfer session, but has"
                                         f" unexpected format on its 'Transfer stats:' line."
                                         f" Skipping {key} statistic.")
                            logger.error(f"Time conversion failure for"
                                         f" key={key},"
                                         f" value={value},"
                                         f" e={str(e)}")

                        if key in XFER_STATS_TO_RETAIN:
                            if key in ['NBYTES'] and value.isdigit():
                                session_transfer_stats_dict[key]=int(value)
                            else:
                                session_transfer_stats_dict[key]=value
                        key=value=None
            else:
                continue
            if apparent_entity_uuid and len(apparent_entity_uuid)==32:
                session_transfer_stats_dict['entity_uuid']=apparent_entity_uuid
            if 'FILE' in session_transfer_stats_dict:
                session_transfer_stats_dict['file_scope']=re.sub('/.*$'
                                                                 ,''
                                                                 ,session_transfer_stats_dict['FILE'])
            session_transfer_stats_dict['globus_session_id']=session['pid']
            session_transfer_stats_list.append(session_transfer_stats_dict)

    logger.info(f"Returning session_transfer_stats_list of length {len(session_transfer_stats_list)}.")
    return session_transfer_stats_list

def create_transfer_JSON_dict(session_transfer_stats_list:list, provenance_dict:dict, transfer_scope_prefix:str=PUBLIC_DIR_PREFIX):
    transfer_stats_list = []
    sessions_without_success_stats = []
    loopback_skip_counter = 0

    for session_transfer_stats_dict in session_transfer_stats_list:
        transfer_stats_dict = {
            'destination_ip': None
            , 'user_info': {'user': 'PENDING'}
            , 'entity_uuid': None
            , 'dataset_type': 'UNTRACKED'
            , 'application_id': 'UNTRACKED'
            , 'entity_type': 'UNTRACKED'
            , 'relative_file_path': None
            , 'bytes_transferred': None
            , 'download_date_time': None
            , 'protocol': None
            , 'globus_task_id': 'NOT_FOUND'
            , 'provenance': copy.deepcopy(provenance_dict)
        }
        if 'FILE' in session_transfer_stats_dict:
            if not session_transfer_stats_dict['FILE'].startswith(transfer_scope_prefix):
                logger.debug(f"Skipping non-{transfer_scope_prefix[:-1]} file transfers in {session_transfer_stats_dict}")
                continue

            if re.match(f"{PUBLIC_DIR_PREFIX}[0-9a-f]{{32}}{os.sep}"
                        , session_transfer_stats_dict['FILE']):
                relative_file_path = re.sub(f"^{PUBLIC_DIR_PREFIX}"
                                            , ''
                                            , session_transfer_stats_dict['FILE'])
            else:
                relative_file_path = session_transfer_stats_dict['FILE']
            transfer_stats_dict['relative_file_path'] = relative_file_path
            
            if 'DEST' in session_transfer_stats_dict:
                transfer_stats_dict['destination_ip'] = re.sub('[^0-9]$'
                                                             ,''
                                                             , re.sub('^[^0-9]'
                                                                      ,''
                                                                      ,session_transfer_stats_dict['DEST']))

            raw_task_id = session_transfer_stats_dict.get('TASKID', '')
            if transfer_stats_dict['destination_ip'] == '127.0.0.1' and raw_task_id and raw_task_id.lower() == 'none':
                logger.debug(f"Skipping loopback GridFTP entry (DEST=127.0.0.1, TASKID=none) at"
                             f" stats_line_num={session_transfer_stats_dict.get('stats_line_num')};"
                             f" already captured by the HTTP access log.")
                loopback_skip_counter += 1
                continue

            if 'entity_uuid' in session_transfer_stats_dict:
                transfer_stats_dict['entity_uuid'] = session_transfer_stats_dict['entity_uuid']
            if transfer_stats_dict['entity_uuid']:
                transfer_stats_dict['dataset_type'] = 'PENDING'
                transfer_stats_dict['application_id'] = 'PENDING'
                transfer_stats_dict['entity_type'] = 'PENDING'
            if 'NBYTES' in session_transfer_stats_dict:
                transfer_stats_dict['bytes_transferred'] = session_transfer_stats_dict['NBYTES']
            if 'START_UTC' in session_transfer_stats_dict:
                transfer_stats_dict['download_date_time'] = session_transfer_stats_dict['START_UTC']
            if 'TASKID' in session_transfer_stats_dict and session_transfer_stats_dict['TASKID']:
                g_tid = session_transfer_stats_dict['TASKID']
                if g_tid.lower() != 'none':
                    transfer_stats_dict['globus_task_id'] = g_tid
                else:
                    logger.debug(f"Session {session_transfer_stats_dict.get('globus_session_id')}"
                                 f" transferred without a Globus TASKID (logged as 'none');"
                                 f" globus_task_id left as '{transfer_stats_dict['globus_task_id']}'.")
            transfer_stats_dict['protocol'] = 'gridftp'
            src_file_base = transfer_stats_dict['provenance'][PROJECT_NAME]['destination_local_file'].replace(f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}"
                                                                                                           , ''
                                                                                                           , 1)
            src_file_base = src_file_base.replace('.json','').replace(os.sep,'_')
            transfer_stats_dict['provenance'][PROJECT_NAME]['source_log_line'] = session_transfer_stats_dict['stats_line_num']
            transfer_stats_dict['provenance'][PROJECT_NAME]['es_id'] = f"{src_file_base}" \
                                                                                     f"_{session_transfer_stats_dict['stats_line_num']}"
            transfer_stats_list.append(transfer_stats_dict)
        else:
            sessions_without_success_stats.append(session_transfer_stats_dict['globus_session_id'])
    if sessions_without_success_stats:
        logger.error(f"The following Sessions appear to be file transfer sessions, but"
                     f" did not successfully transfer or logged the FILE attribute among"
                     f" input lines skipped due to formatting."
                     f"\n{str(sessions_without_success_stats)}")
    if loopback_skip_counter > 0:
        logger.info(f"Skipped {loopback_skip_counter} internal Globus HTTP-to-GridFTP loopback"
                    f" entries (DEST=127.0.0.1, TASKID=none), already captured by the HTTP access log.")
    return transfer_stats_list

# Given a list of transfer statistics in a dict and the associated file name which
# should contain them, convert the dict to JSON and save in the file.
def save_transfer_stats_json(session_transfer_stats_json, json_filename):
    if os.path.isfile(json_filename):
        logger.error(f"File '{json_filename}' already exists.")
    else:
        with open(json_filename, "w") as jf:
            jf.write(session_transfer_stats_json)
        logger.info(f"Wrote {len(session_transfer_stats_json)} bytes of JSON to '{json_filename}'\n")
        Path(f"{json_filename}.DONE.1").touch()
        
# Create a dict keyed with the name of log files which exist, for which an associated
# JSON file does not exist on the local file system.
def get_unparsed_log_dict():
    global node_dir_list
    
    input_file_list = []
    output_file_list = []
    for node_dir in node_dir_list:
        node_log_dir_fullpath = f"{LOG_FILE_NIGHTLY_DIR}{os.sep}{node_dir}"
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}{os.sep}{node_dir}"

        node_input_wildcard_pattern=f"{node_log_dir_fullpath}{os.sep}gridftp-log{os.sep}gridftp.log-[0-9]*.gz"
        node_output_wildcard_pattern=f"{node_json_dir_fullpath}{os.sep}gridftp.log-[0-9]*.json"

        #Get the files matching the wildcard pattern
        node_input_file_list = glob.glob(node_input_wildcard_pattern)
        node_output_file_list = glob.glob(node_output_wildcard_pattern)

        input_file_list.extend(node_input_file_list)
        output_file_list.extend(node_output_file_list)

    logger.info(f"Found {len(input_file_list)} input files to correlate with {len(output_file_list)} output files.")

    parsing_src_dest_dict={}
    for input_filename in input_file_list:
        output_filename = re.sub(LOG_FILE_NIGHTLY_DIR
                                 ,f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}"
                                 ,re.sub('gz$'
                                         ,'json'
                                         , input_filename))
        output_filename = re.sub(f"{os.sep}gridftp-log{os.sep}"
                                 ,os.sep
                                 ,output_filename)
        # Identify the input log files for which there is not a corresponding output JSON file.
        if output_filename in output_file_list:
            if verbose:
                logger.info(f"Skip {input_filename} because {output_filename} already exists.")
            continue
        if verbose:
            logger.info(f"Add parsing_src_dest_dict[{input_filename}]={output_filename}.")
        parsing_src_dest_dict[input_filename]=output_filename
    return parsing_src_dest_dict

if __name__ == '__main__':
    msg =   f":large_purple_circle: {portfolio_utils.get_slack_host_context()} :large_purple_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_purple_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to process Grid FTP logs\n" \
            f" to create JSON files at {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}.\n" \
            f" Process logging to {log_file_name}\n" \
            f":large_purple_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()

    # Log the criteria which will be used to identify log lines which are
    # interesting because they indicate a file transfer.
    logger.info(f"Identifying sessions with apparent file transfers, based on"
                f" the following criteria for log lines:")
    for re_dict in SESSION_RETAIN_LINE_RE_LIST:
        if re_dict['session_interesting_indicator']:
            logger.info(f"regex matches '{re_dict['payload_re']}'")

    # Figure out which log files do not have an accompanying JSON file, and
    # need to be processed
    parsing_src_dest_dict=get_unparsed_log_dict()
    if not parsing_src_dest_dict:
        logger.info(f"No new JSON generated since one exists for each gridftp log files found.")
    else:
        logger.info(f"Found {len(parsing_src_dest_dict)} input files to check in pipeline progresss status.")
        
    # Process logs files and generate accompanying JSON files.
    processed_file_count = 0
    for input_filename in parsing_src_dest_dict.keys():
        logger.info(f"\nLooking for file transfer lines in {input_filename}")

        try:
            log_lines = get_log_lines_from_gzip_file(file_name=input_filename)
            logger.info(f"Read {len(log_lines)} log lines from {input_filename}.")
            session_log_lines_dict = create_dict_by_session_from_log_lines(log_file_lines=log_lines)
            logger.info(f"Found {len(session_log_lines_dict.keys())} sessions among {len(log_lines)} log lines.")
            interesting_sessions=pull_interesting_sessions(session_log_lines_dict=session_log_lines_dict)
            session_transfer_stats=generate_stats_for_finished_transfers(interesting_sessions_list=interesting_sessions)
            logger.info(f"Found {len(session_transfer_stats)} file transfer stats for {len(interesting_sessions)} file transfer sessions.")

            # Create a provenance dict for the processed log file, which can become a
            # part of each JSON Object of the JSON list that will become a file.
            # Once the file is saved, subsequent processes will modify this provenance
            # data with their own entries.
            src_dest_prov_dict = {
                PROJECT_NAME: {
                    'process_script' : os.path.basename(__file__)
                    , 'process_utc_dt': datetime.now(tz_utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    , 'source_log_file': f"{input_filename}"
                    , 'destination_local_file': f"{parsing_src_dest_dict[input_filename]}"
                }
            }
            
            transfer_stats = []
            for scope_dir_prefix in [PUBLIC_DIR_PREFIX, CONSORTIUM_DIR_PREFIX, PROTECTED_DIR_PREFIX]:
                scope_transfer_stats=create_transfer_JSON_dict(session_transfer_stats_list=session_transfer_stats
                                                               , provenance_dict=src_dest_prov_dict
                                                               , transfer_scope_prefix=scope_dir_prefix)
                logger.info(f"Found {len(scope_transfer_stats)} {scope_dir_prefix[:-1]} file transfer stats among {len(session_transfer_stats)} file transfer stats.")
                transfer_stats.extend(scope_transfer_stats)

            logger.info(f"Converting {len(transfer_stats)} extracted transfer statistics to JSON.")
            transfer_stats_json = json.dumps(transfer_stats)
            save_transfer_stats_json(session_transfer_stats_json=transfer_stats_json
                                     , json_filename=parsing_src_dest_dict[input_filename])
            logger.info(f"Saved {len(transfer_stats)} file transfer stats to '{parsing_src_dest_dict[input_filename]}'")

            processed_file_count += 1
        except Exception as e:
            logger.exception(f"Error halted processing file {input_filename}.")

    process_utc_finish = datetime.now(tz_utc)
    good_news = (f":large_purple_circle: {portfolio_utils.get_slack_host_context()} :large_purple_circle: {PROJECT_NAME} {SLACK_PHASE_EMOJI} {Path(__file__).name} :large_purple_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" Wrote {processed_file_count} JSON files to {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROJECT_NAME}.\n"
                 f" Process logged to {log_file_name}\n"
                 f"{':purple_heart: ' * 5}\n"
                 f":large_purple_circle:")
    logger.info(good_news)
    try:
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=good_news
                                           , mentions_dict=slack_user_id_mentions_on_success_dict)
    except Exception as e:
        logger.exception('Unable to post Slack success notification.')

    sys.exit(0)
