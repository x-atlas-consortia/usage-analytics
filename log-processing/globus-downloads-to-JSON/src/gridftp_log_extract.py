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
#
# arg_process_dir replaces the old hardcoded HIVE_DEPLOY_BASE. It's the .sh wrapper's own
# BASH_SOURCE-derived PROCESS_DIR (e.g. .../log-processing/globus-downloads-to-JSON), passed
# in explicitly rather than guessed at, so moving this whole tree to a new server or a new
# repository never requires touching this file's own content.
arg_parser = argparse.ArgumentParser(description='Extract Globus GridFTP file-transfer entries to JSON.')
arg_parser.add_argument('--process-dir', required=True, dest='process_dir',
                         help="This process's own directory (one level above src/), e.g."
                              " .../log-processing/globus-downloads-to-JSON. Normally supplied"
                              " by the .sh wrapper's own PROCESS_DIR.")
args = arg_parser.parse_args()
arg_process_dir = args.process_dir
arg_portfolio_dir = os.path.dirname(arg_process_dir)  # one level above arg_process_dir

#
# Read configuration from the project INI file and set global constants
#
Config = configparser.ConfigParser()

process_ini_candidates = [
    Path('gridftp_log_extract.ini'),                                    # Docker WORKDIR
    Path(f'{arg_process_dir}/src/gridftp_log_extract.ini'),             # vm001 default
    Path('../../globus-downloads-to-JSON/src/gridftp_log_extract.ini'), # PyCharm dev
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
    # The PROC_NAME pulled from the INI file should match the script variable PROCESS_DIR
    # in the bash script executing this program.
    PROC_NAME=Config.get('ProcessSpecificSettings', 'PROC_NAME')
    TRACKING_FILE=Config.get('ProcessSpecificSettings', 'TRACKING_FILE')
    LOG_FILE_NIGHTLY_DIR = Config.get('ProcessSpecificSettings', 'LOG_FILE_NIGHTLY_DIR')
    NODE_LOG_DIR_LIST = Config.get('ProcessSpecificSettings', 'NODE_LOG_DIR_LIST')
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
# exec_info is at PROCESS_DIR/exec_info, where PROCESS_DIR is one level above src/.
#
exec_info_dir_candidates = [
    Path('exec_info'),                                                             # Docker WORKDIR
    Path(f'{arg_process_dir}/exec_info'),                 # vm001 default
    Path(f'../../{PROC_NAME}/exec_info'),                 # PyCharm dev
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
                    ,level=logging.DEBUG+1 # INFO
                    ,format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
                    ,datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

print('Logger instantiated')

portfolio_utils = None
try:
    config_file_location = None
    candidates = [
        Path('logProcessingProject.ini'),                                    # Docker WORKDIR
        Path(f'{arg_portfolio_dir}/src/logProcessingProject.ini'),          # vm001 default
        Path('../../src/logProcessingProject.ini'),                          # PyCharm dev
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
    logger.info("LogExtractXferUtils instantiated.")
    print('LogExtractXferUtils instantiated.')
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded')

EMPTY_TRACKING_DICT_VALUE = {
    "status": None,
    "input_info": {
        "discovery_dt": None,
        "input_process_dt": None,
        "lines_read": None,
        "session_file_transfers": None,
        "dataset_file_transfers": None
    },
    "process_info": {
        "inclusion_span": {
            "on_or_after": None,
            "on_or_before": None
        }
    },
    "output_product": {
        "filename": None,
        "size_in_bytes": None,
    }
}

#
# Set more global constants specific to parsing files and generating JSON.
#

# Create a usable Python list global from the str in the INI file
node_dir_list = ast.literal_eval(NODE_LOG_DIR_LIST)

# List of regular expressions used to match the "payload" section of a log line i.e.
# everything after the session ID, datetime, server, and port.  Log line payloads which
# match these regular expressions are retained for possible usage in creating the
# JSON output for events of interest.
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

# Regular expression indicating a log line begins with a session ID, and
# is therefore a "new" log line.
# ************ When log lines do not begin with such a ************
# ************ pattern, attempt to associate line with ************
# ************ the last know "session ID line" until   ************
# ************ the next "session ID line" is found.    ************
RE_NEW_SESSION_LINE = r'^\[[0-9]+\] .* :: .*'
# The threading of Globus logging is questionable, and frequently a log line
# being written for one event will be interrupted by a line logged for another
# event.  Eventually both complete writing, but this script does not attempt
# to resolve the mess.
# The following two patterns identify lines to discard, if they either
# appear to have two session IDs on the line, or have one session ID which
# does not occur at the start of the line.
RE_FOULED_UP_DOUBLE_PID_LINE = r'.*\[[0-9]+\].*\[[0-9]+\].*'
RE_FOULED_UP_MID_PID_LINE = r'..*\[[0-9]+\].*'
# This format matches the format received in Globus logs as of Summer 2023, and
# is used to separate the "payload" of a logged line from the "preamble."
GRIDLOG_DATE_FORMAT = '%a %b %d %H:%M:%S %Y' # default format for time.strptime()
# A list of "file transfer statistics" which are to be retained and published in
# the JSON output. Each stat is either included on a "Transfer stats:" log line or
# from transforming an element of that log line.
XFER_STATS_TO_RETAIN = ['START_UTC','FILE','NBYTES','DEST','TASKID']

# Create a lookup dictionary for transforming key names contained in the dictionaries
# in session_transfer_stats_list to JSON field names compatible with the JSON output by
# the generate_usage_report.py Airflow DAG of ingest-pipeline.
# https://github.com/hubmapconsortium/ingest-pipeline/blob/devel/src/ingest-pipeline/airflow/dags/generate_usage_report.py
#
# Sample output from Derek's script
# user_name - huangqis@andrew.cmu.edu
# request_time - 2022-05-23 22:15:58.058825
# source_endpoint_id - af603d86-eab9-4eec-bb1d-9d26556741bb
# source_endpoint_name - HuBMAP Public
# destination_endpoint_name - DESKTOP
# destination_endpoint_id - 7f686ef2-bdf1-11ec-8f85-e31722b18688
# source_endpoint_host_id - 38ce5af2-46db-4695-8288-7ba40ff570eb
# destination_endpoint_host_id - 7f686ef2-bdf1-11ec-8f85-e31722b18688
# taskid - ebbf72ce-dae5-11ec-990a-3b4cfda38030
# bytes_transferred - 17752746798
# data_type - Public: Unknown
# hubmap_id - Public: Unknown
# entity_type - Public: Unknown
#

now_utc = datetime.now(tz_utc)

# Setting occasionally used for debugging. Could be passed in if usage expanded.
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
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}{node_dir}"
        if not os.path.exists(node_log_dir_fullpath):
            print(f"Halting program due to not finding an expected node log directory at "
                  f"'{node_log_dir_fullpath}'")
            exit_rather_than_return = True
        if not os.path.exists(node_json_dir_fullpath):
            print(f"Halting program due to not finding an expected node JSON directory at "
                  f"'{node_json_dir_fullpath}'")
            exit_rather_than_return = True
    if not os.path.isfile(TRACKING_FILE):
        print(f"Halting program due to not finding an expected tracking JSON file at "
              f"'{TRACKING_FILE}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":large_purple_circle: {portfolio_utils.get_slack_host_context()} :large_purple_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_purple_circle:\n"
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

# Given all the lines in a log file as an ordered list, go through them one-by-one. Determine
# which session each one can be attributed to, either because a session ID starts the line, or
# by attributing lines without a session ID to be a continuation of the last session ID line
# which was identified.
# Return a dictionary keyed by session ID, with a dict value about the line(s).
# Skip over and log lines which seem fouled up.
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
                # presume a PID line was read, and its data for recorded time is still appropos for the non-PID line
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

# Identify sessions containing lines matching the "required" regular expressions indicating
# the session involved file transfer.
# Return a list containing only session dicts for sessions involved with file transfer.
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

# For session log lines containing transfer statistics, pull the line apart to create a
# dict with each statistic of interest, along with added information to include in the JSON.
def generate_stats_for_finished_transfers(interesting_sessions_list):

    session_transfer_stats_list=[]
    for session in interesting_sessions_list:
        apparent_Dataset_UUID=''
        for line in session['interesting_lines']:
            session_transfer_stats_dict = {}
            payload = line['payload']
            if re.match('Transfer stats: .* TYPE=STOR .*', payload):
                logger.debug(f"For session {session['pid']}, not interested in 'Transfer stats' with TYPE=STOR.")
                # Keep processing session lines, in case any TYPE=RETR Transfer stats payloads actually
                # make this session interesting.
                continue
            elif re.match('Transfer stats: .* TYPE=RETR .*', payload):
                session_transfer_stats_dict['stats_line_num'] = line['line_num']
                #print(f"\tJSON\t{payload}")
                # Transfer stats, particularly FILE, may contain spaces.  So split by looking for regular
                # expression which ends in an equals sign and picks up the characters before the
                # equals sign which are not spaces or equal signs.  Retain these statistic labels by
                # surrounding the regex with parentheses, then piece together a dictionary of stats.
                #
                # typical input:
                # [24775] Thu Jul 27 12:59:42 2023 :: Transfer stats: DATE=20230727165942.313344
                # HOST=app001.hive.psc.edu PROG=globus-gridftp-server NL.EVNT=FTP_INFO
                # START=20230727165926.169723 USER=shirey
                # FILE=/hive/hubmap/data/public/c95d9373d698faf60a66ffdc27499fe1/drv_CX_20-008_lymphnode_n10_reg001/processed_2020-12-2320-008LNn10r001/segm/segm-1/fcs/compensated/LN7910_20_008_11022020_reg001_compensated.csv
                # BUFFER=131072 BLOCK=1048576 NBYTES=217159441 VOLUME=/ STREAMS=1 STRIPES=1
                # DEST=[127.0.0.1] TYPE=RETR CODE=226 TASKID=none

                key=value=None
                for payload_token in re.split('([^= ]+=)',payload.replace('Transfer stats: ','').strip()):
                    if payload_token[-1:] == '=':
                        key=payload_token[0:-1]
                    else:
                        value=payload_token.strip()
                        if key == 'FILE':
                            filepath_tokens = re.split(os.sep, value)
                            try:
                                apparent_Dataset_UUID = filepath_tokens[5] if filepath_tokens[4] == 'public' else filepath_tokens[6]
                            except Exception as e:
                                logger.debug(f"While trying to parse Dataset UUID in"
                                             f" {str(filepath_tokens)} got e={str(e)}.")
                    if key and value:
                        try:
                            # For any time values supplement with a UTC time
                            if key == 'START':
                                t = parse_datetime_flexible(value)
                                key = 'START_UTC'
                                value = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            if key == 'DATE':
                                t = parse_datetime_flexible(value)
                                key = 'DATE_UTC'
                                value = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                            if key == 'FILE':
                                # Strip off the beginning of the absolute path to
                                # form a relative path under ABS_PATH_BASE_TO_REMOVE.
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

                        # Only keep the key/value pairs for statistics of interest
                        if key in XFER_STATS_TO_RETAIN:
                            # Store the key/value pair in a dictionary which will be
                            # converted to JSON.
                            if key in ['NBYTES'] and value.isdigit():
                                session_transfer_stats_dict[key]=int(value)
                            else:
                                session_transfer_stats_dict[key]=value
                        key=value=None
            else:
                continue
            # Tack on other values not in the logged message
            if apparent_Dataset_UUID and len(apparent_Dataset_UUID)==32:
                # For directories which are coincidentally 32 characters long but are not
                # Dataset UUIDs, this hands them back as if they are Dataset UUIDs, because
                # we are not going to import the Dataset formatting logic of hubmap-commons
                # for this edge case.
                session_transfer_stats_dict['dataset_uuid']=apparent_Dataset_UUID
            if 'FILE' in session_transfer_stats_dict:
                session_transfer_stats_dict['file_scope']=re.sub('/.*$'
                                                                 ,''
                                                                 ,session_transfer_stats_dict['FILE'])
            session_transfer_stats_dict['globus_session_id']=session['pid']
            # N.B. TASKID is captured above via XFER_STATS_TO_RETAIN. Resolving it against
            # the transfer-details CSV to get owner_identity_name happens later, in
            # globus_xfer_details_updater.py -- this script no longer waits on that CSV.
            session_transfer_stats_list.append(session_transfer_stats_dict)

    logger.info(f"Returning session_transfer_stats_list of length {len(session_transfer_stats_list)}.")
    return session_transfer_stats_list

# Transform session dicts containing transfer statistics to
# dicts keyed by names to use in the JSON for file transfers.
def create_transfer_JSON_dict(session_transfer_stats_list:list, provenance_dict:dict, transfer_scope_prefix:str=PUBLIC_DIR_PREFIX):
    transfer_stats_list = []
    sessions_without_success_stats = []
    loopback_skip_counter = 0

    for session_transfer_stats_dict in session_transfer_stats_list:
        transfer_stats_dict = {
            'destination_ip': None
            # 'destination_host': optional field, added if a truthy value is found
            , 'user_info': {'user': 'PENDING'}
            , 'dataset_uuid': None
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
                continue # Only interested in file transfers whose relative paths start with transfer_scope_prefix

            # Because HTTP transfers from /hive/hubmap/data/public are recorded in the Globus access log relative to
            # the dataset directory name rather than the scope, strip the prefix 'public/' when it occurs before a
            # dataset_uuid
            if re.match(f"{PUBLIC_DIR_PREFIX}[0-9a-f]{{32}}{os.sep}"
                        , session_transfer_stats_dict['FILE']):
                relative_file_path = re.sub(f"^{PUBLIC_DIR_PREFIX}"
                                            , ''
                                            , session_transfer_stats_dict['FILE'])
            else:
                relative_file_path = session_transfer_stats_dict['FILE']
            transfer_stats_dict['relative_file_path'] = relative_file_path
            
            if 'DEST' in session_transfer_stats_dict:
                # If the DEST has non-numeric characters around it (e.g. [nnn.nn.n.nnn]) trim them off. Assume only one IP enclosed.
                transfer_stats_dict['destination_ip'] = re.sub('[^0-9]$'
                                                             ,''
                                                             , re.sub('^[^0-9]'
                                                                      ,''
                                                                      ,session_transfer_stats_dict['DEST']))

            # Globus serves HTTPS downloads internally via a GridFTP loopback fetch, logged with
            # DEST=[127.0.0.1] and TASKID=none. That GridFTP line duplicates the same download's own
            # entry in the HTTP access log, so exclude it here to avoid double-counting the transfer.
            # (Checking the raw TASKID rather than the derived globus_task_id, since NOT_FOUND can
            # also arise from TASKID simply being absent, which isn't this specific loopback case.)
            raw_task_id = session_transfer_stats_dict.get('TASKID', '')
            if transfer_stats_dict['destination_ip'] == '127.0.0.1' and raw_task_id and raw_task_id.lower() == 'none':
                logger.debug(f"Skipping loopback GridFTP entry (DEST=127.0.0.1, TASKID=none) at"
                             f" stats_line_num={session_transfer_stats_dict.get('stats_line_num')};"
                             f" already captured by the HTTP access log.")
                loopback_skip_counter += 1
                continue

            if 'dataset_uuid' in session_transfer_stats_dict:
                transfer_stats_dict['dataset_uuid'] = session_transfer_stats_dict['dataset_uuid']
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
            # Work up a unique key for this document which can be used as the ElasticSearch document _id.
            src_file_base = transfer_stats_dict['provenance'][PROC_NAME]['destination_local_file'].replace(f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}"
                                                                                                           , ''
                                                                                                           , 1)
            src_file_base = src_file_base.replace('.json','').replace(os.sep,'_')
            transfer_stats_dict['provenance'][PROC_NAME]['source_log_line'] = session_transfer_stats_dict['stats_line_num']
            transfer_stats_dict['provenance'][PROC_NAME]['es_id'] = f"{src_file_base}" \
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
        # Mark stage 1 complete for this file. N.B. simple/hard-coded for now, per plan --
        # atomic replace, read-only permissions, etc. come with next week's checklist pass.
        Path(f"{json_filename}.DONE.1").touch()
        
# Create a dict keyed with the name of log files which exist, for which an associated
# JSON file does not exist on the local file system.
def get_unparsed_log_dict():
    global node_dir_list
    
    input_file_list = []
    output_file_list = []
    for node_dir in node_dir_list:
        node_log_dir_fullpath = f"{LOG_FILE_NIGHTLY_DIR}{os.sep}{node_dir}"
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}{node_dir}"

        # Set up a paths with "shell-style wildcards" (not Python regular expressions!)

        node_input_wildcard_pattern=f"{node_log_dir_fullpath}{os.sep}gridftp-log{os.sep}gridftp.log-[0-9]*.gz"
        node_output_wildcard_pattern=f"{node_json_dir_fullpath}{os.sep}gridftp.log-[0-9]*.json"

        #Get the files matching the wildcard pattern
        node_input_file_list = glob.glob(node_input_wildcard_pattern)
        node_output_file_list = glob.glob(node_output_wildcard_pattern)

        input_file_list.extend(node_input_file_list)
        output_file_list.extend(node_output_file_list)

    logger.info(f"Found {len(input_file_list)} input files to correlate with {len(output_file_list)} output files.")

    # Identify the input log files for which there is not a
    # corresponding output JSON file.
    parsing_src_dest_dict={}
    for input_filename in input_file_list:
        # For a log filename found in on input_file_list, generate the name of the JSON file
        # corresponding to it.  Then check if the file already exists in output_file_list, which
        # reflects what is on the file system.
        output_filename = re.sub(LOG_FILE_NIGHTLY_DIR
                                 ,f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}"
                                 ,re.sub('gz$'
                                         ,'json'
                                         , input_filename))
        # Unlike inputs which are rsync'ed using the directory structure of
        # another machine, the outputs exist directly inside a directory named
        # for the origin node.  So strip out the "gridftp-log" directly above the filename.
        output_filename = re.sub(f"{os.sep}gridftp-log{os.sep}"
                                 ,os.sep
                                 ,output_filename)
        if output_filename in output_file_list:
            if verbose:
                logger.info(f"Skip {input_filename} because {output_filename} already exists.")
            continue
        if verbose:
            logger.info(f"Add parsing_src_dest_dict[{input_filename}]={output_filename}.")
        parsing_src_dest_dict[input_filename]=output_filename
    return parsing_src_dest_dict

if __name__ == '__main__':
    msg =   f":large_purple_circle: {portfolio_utils.get_slack_host_context()} :large_purple_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_purple_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to process Grid FTP logs\n" \
            f" to create JSON files at {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}.\n" \
            f" Process logging to {log_file_name}\n" \
            f":large_purple_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()

    # Get the information about the files already processed during previous runs.
    try:
        tracking_dict = portfolio_utils.get_tracking_from_file(filename=TRACKING_FILE)
    except json.JSONDecodeError as jde:
        # JSON file exists but contains invalid JSON
        print(f"Invalid JSON in {TRACKING_FILE}: {jde}")
        sys.exit(2)
    except OSError as ose:
        # Problems opening/reading the file itself
        print(f"Error reading {TRACKING_FILE}: {ose}")
        sys.exit(2)
    logger.info(f"Loaded tracking_dict with {len(tracking_dict)} entries from {TRACKING_FILE}.")

    # Log the criteria which will be used to identify log lines which make a
    # session interesting because they indicate a file transfer involved.
    # N.B. There are more regular expression for "lines to retain" for each
    #      file transfer session than there are lines which make the session
    #      "interesting" i.e. the session includes a successful transfer. This
    #      is for dealing with unsuccessful transfers, non-public transfers,
    #      etc. when needed.
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
        logger.info(f"Found {len(parsing_src_dest_dict)} input files to check in tracking status.")
        
    # Process logs files and generate accompanying JSON files.
    processed_file_count = 0
    for input_filename in parsing_src_dest_dict.keys():
        new_tracking_dict_value = None
        if input_filename in tracking_dict:
            if tracking_dict[input_filename]['status'] in [LogFileStatusType.PROCESSED_TO_JSON.value, LogFileStatusType.PROCESSED_TO_S3.value]:
                if verbose:
                    logger.info(f"{input_filename} processed. Not processing again")
                continue
            elif tracking_dict[input_filename]['status'] == LogFileStatusType.UNPROCESSED.value:
                if verbose:
                    logger.info(f"{input_filename} unprocessed. Will evaluate to see if it should be processed in current window.")
            else:
                logger.error(f"{input_filename} has unrecognized status {tracking_dict[input_filename]['status']}."
                             , file=sys.stderr)
                sys.exit(2)
        else:
            if verbose:
                logger.info(f"{input_filename} newly discovered, will add to tracking.")
            new_tracking_dict_value=copy.deepcopy(EMPTY_TRACKING_DICT_VALUE)
            new_tracking_dict_value['status'] = LogFileStatusType.UNPROCESSED.value
            new_tracking_dict_value['input_info']['discovery_dt'] = str(now_utc)

            tracking_dict[input_filename] = new_tracking_dict_value

        logger.info(f"\nLooking for file transfer lines in {input_filename}")

        try:
            log_lines = get_log_lines_from_gzip_file(file_name=input_filename)
            logger.info(f"Read {len(log_lines)} log lines from {input_filename}.")
            tracking_dict[input_filename]['input_info']['lines_read'] = len(log_lines)
            session_log_lines_dict = create_dict_by_session_from_log_lines(log_file_lines=log_lines)
            logger.info(f"Found {len(session_log_lines_dict.keys())} sessions among {len(log_lines)} log lines.")
            interesting_sessions=pull_interesting_sessions(session_log_lines_dict=session_log_lines_dict)
            session_transfer_stats=generate_stats_for_finished_transfers(interesting_sessions_list=interesting_sessions)
            logger.info(f"Found {len(session_transfer_stats)} file transfer stats for {len(interesting_sessions)} file transfer sessions.")
            tracking_dict[input_filename]['input_info']['session_file_transfers'] = len(session_transfer_stats)

            # Create a provenance dict for the processed log file, which can become a
            # part of each JSON Object of the JSON list that will become a file. This
            # JSON becomes the input to a subsequent processes to incorporate transfer
            # details and load the analytic data-store.
            src_dest_prov_dict = {
                PROC_NAME: {
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
                # Merge the list for transfers of a specific scope into the list accumulating all the transfer stats.
                transfer_stats.extend(scope_transfer_stats)
            tracking_dict[input_filename]['input_info']['dataset_file_transfers'] = len(transfer_stats)

            logger.info(f"Converting {len(transfer_stats)} extracted transfer statistics to JSON.")
            transfer_stats_json = json.dumps(transfer_stats)
            save_transfer_stats_json(session_transfer_stats_json=transfer_stats_json
                                     , json_filename=parsing_src_dest_dict[input_filename])
            tracking_dict[input_filename]['output_product']['filename'] = parsing_src_dest_dict[input_filename]
            tracking_dict[input_filename]['output_product']['size_in_bytes'] = len(transfer_stats_json)
            logger.info(f"Saved {len(transfer_stats)} file transfer stats to '{parsing_src_dest_dict[input_filename]}'")

            # Indicate processing of the input log file to a local JSON file is complete.
            # N.B. A subsequent step will revise this JSON using transfer details periodically
            #      received from Globus, and rename the file after doing so.
            tracking_dict[input_filename]['status'] = LogFileStatusType.PROCESSED_TO_JSON.value
            tracking_dict[input_filename]['input_info']['input_process_dt'] = str(now_utc)
            processed_file_count += 1
        except Exception as e:
            logger.exception(f"Error halted processing file {input_filename}.")

    logger.info(f"Writing out tracking_dict with {len(tracking_dict)} entries.")
    portfolio_utils.overwrite_tracking_to_file(filename=TRACKING_FILE
                                               , pydict=tracking_dict)

    process_utc_finish = datetime.now(tz_utc)
    good_news = (f":large_purple_circle: {portfolio_utils.get_slack_host_context()} :large_purple_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_purple_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" Wrote {processed_file_count} JSON files to {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}.\n"
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
