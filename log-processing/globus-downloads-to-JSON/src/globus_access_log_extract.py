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
from pathlib import Path
import gzip
import json
import ast
from collections import defaultdict

from apachelogs import LogParser

# Import project resources
from log_extract_xfer_utils import LogExtractXferUtils

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
arg_parser = argparse.ArgumentParser(description='Extract Globus HTTP access log file-transfer entries to JSON.')
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
    Path('globus_access_log_extract.ini'),                                    # Docker WORKDIR
    Path(f'{arg_process_dir}/src/globus_access_log_extract.ini'),             # vm001 default
    Path('../../globus-downloads-to-JSON/src/globus_access_log_extract.ini'), # PyCharm dev
]
config_file_name = None
for candidate in process_ini_candidates:
    if candidate.is_file():
        config_file_name = str(candidate.resolve())
        break
if not config_file_name:
    print(f"\a\nUnable to find globus_access_log_extract.ini in any expected location.\n")
    sys.exit(3)
Config.read(config_file_name)
try:
    # The PROC_NAME pulled from the INI file should match the script variable PROCESS_DIR
    # in the bash script executing this program.
    PROC_NAME = Config.get('ProcessSpecificSettings', 'PROC_NAME')
    LOG_FILE_NIGHTLY_DIR = Config.get('ProcessSpecificSettings', 'LOG_FILE_NIGHTLY_DIR')
    NODE_LOG_DIR_LIST = Config.get('ProcessSpecificSettings', 'NODE_LOG_DIR_LIST')
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
                f"/globus_access_log_extract-" \
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
        Path(f'{arg_portfolio_dir}/src/logProcessingProject.ini'),           # vm001 default
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
# Set up a paths with PCRE regular expressions
node_log_pcre_pattern=re.compile(r'^globus_access_log-\d{8}$')
node_json_pcre_pattern=re.compile(r'^globus_access_log-\d{8}\.json$')

# Create a usable Python list global from the str in the INI file
node_dir_list = ast.literal_eval(NODE_LOG_DIR_LIST)

# Create a parser for Apache access log lines
aal_parser = LogParser('%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"')

# # List of regular expressions used to match the "payload" section of an "access log" line i.e.
# g-d00e7b.09193a.5898.dn.glob.us:af603d86-eab9-4eec-bb1d-9d26556741bb 62.192.175.142 - [19/Apr/2025:00:29:24 -*      48 0400] "GET /c95d9373d698faf60a66ffdc27499fe1/drv_CX_20-008_lymphnode_n10_reg001/processed_2020-12-2320-008LNn10r001/segm/segm-1/f*      48 cs/compensated/LN7910_20_008_11022020_reg001_compensated.csv?download=1 HTTP/1.1" 200 803 "-" "Mozilla/5.0 (Macintosh; Intel Mac *      48 OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
# The default log format for Apache access logs, known as the Common Log Format (CLF), is: %h %l %u %t "r" %>s %b. This format includes the remote host (IP address), user identity, username, request time, request line, status code, and response size. 
# Here's a breakdown of the fields: 
# %h: Remote host (client IP address).
# %l: User identity (identd lookup, usually '-').
# %u: Username (authenticated user).
# %t: Timestamp of the request.
# "%r": Request line (e.g., GET /index.html HTTP/1.1).
# %>s: Status code of the response (e.g., 200, 404).
# %b: Size of the response (in bytes).

# Log line payloads which
# match these regular expressions are retained for possible usage in creating the
# JSON output for events of interest.
RETAIN_LINE_RE_LIST = ['GET /protected/.*', 'GET /[0-9a-fA-F]{32}/.*', 'GET /consortium/.*']

now_utc = datetime.now(tz_utc)

# Verify any expectations about the configuration are valid. Print
# messages for each expectation not met and halt if there are any.
def verify_configuration_expectations():
    global node_dir_list
    
    exit_rather_than_return=False
    if not os.path.exists(LOG_FILE_NIGHTLY_DIR):
        print(f"Halting program due to not finding LOG_FILE_NIGHTLY_DIR at "
              f"'{LOG_FILE_NIGHTLY_DIR}'")
        exit_rather_than_return = True
    if not os.path.exists(JSON_FILE_NIGHTLY_DIR):
        print(f"Halting program due to not finding JSON_FILE_NIGHTLY_DIR at "
              f"'{JSON_FILE_NIGHTLY_DIR}'")
        exit_rather_than_return = True
    if not os.path.exists(exec_info_dir):
        print(f"Halting program due to not finding exec_info_dir at "
              f"'{exec_info_dir}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    for node_dir in node_dir_list:
        node_log_dir_fullpath = f"{LOG_FILE_NIGHTLY_DIR}{os.sep}{node_dir}{os.sep}httpd"
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}{node_dir}"
        if not os.path.exists(node_log_dir_fullpath):
            print(f"Halting program due to not finding an expected node log directory at "
                  f"'{node_log_dir_fullpath}'")
            exit_rather_than_return = True
        if not os.path.exists(node_json_dir_fullpath):
            print(f"Halting program due to not finding an expected node JSON directory at "
                  f"'{node_json_dir_fullpath}'")
            exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":large_orange_circle: {portfolio_utils.get_slack_host_context()} :large_orange_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_orange_circle:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Halted trying to verify configuration expectations.\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':large_orange_square::skull_and_crossbones: ' * 5}\n"
                    f":large_orange_circle:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)

# Read all the lines from the specified file, and
# return them in an ordered list.
def get_log_lines_from_file(file_name):
    log_file_lines=[]
    if os.path.isfile(file_name):
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                for line in f:
                    log_file_lines.append(line)
                    log_entry=aal_parser.parse(line)
        except Exception as e:
            logger.error(f"Error reading '{file_name}': {e}.")
    else:
        logger.error(f"File '{file_name}' not found.")
    return log_file_lines

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

# Given a list of strings for each line in an access log, parse each into a dict, tack a
# dict with provenance info on each, and accumulate all the per-line dicts to a list to return.
def create_dict_by_session_from_log_lines(log_file_lines:list[str], provenance_dict:dict, node_dirname:str)->list[dict]:
    global RETAIN_LINE_RE_LIST

    log_lines_list = []
    for idx, logFileLine in enumerate(log_file_lines):
        try:
            log_entry=aal_parser.parse(logFileLine)
            if log_entry.final_status < 200 or log_entry.final_status > 299:
                # We're only creating ElasticSearch documents for successful transfers
                continue
            is_data_transfer_line = False
            for regex in RETAIN_LINE_RE_LIST:
                if re.match(regex, log_entry.request_line):
                    is_data_transfer_line = True
            if not is_data_transfer_line:
                # We're only creating ElasticSearch documents for transfers from our
                # recognized data locations.
                continue
            logger.debug(f"KBKBKB log_entry={str(log_entry)}")
            logger.debug([attr for attr in dir(log_entry) if not attr.startswith('_')])
            #iso8601_request_time = log_entry.request_time_fields['timestamp'].isoformat().replace('+00:00', 'Z')
            #request_dt = datetime(log_entry.request_time_fields['timestamp'], tzinfo=tz_pgh)
            # HTTP access logs never carry a real user identity when %u comes back empty, '-', or
            # None (the parser's representation of the CLF '-' placeholder varies, so all three are
            # treated the same here). UNTRACKED marks this as structurally unavailable for this
            # protocol, distinct from gridftp's 'TBD' (pending resolution) and 'NOT_FOUND' (task id).
            remote_user = log_entry.remote_user
            clf_user = remote_user if remote_user not in (None, '', '-') else 'UNTRACKED'
            logged_line_dict = {
                'destination_ip': log_entry.remote_logname
                , 'destination_host': log_entry.remote_host #optional field, but expected from globus_access_log* format
                , 'user_info': {'user': clf_user}
                , 'dataset_uuid': None
                , 'relative_file_path': None
                , 'bytes_transferred': log_entry.bytes_sent
                , 'download_date_time': log_entry.request_time.astimezone(tz_utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                , 'protocol': 'http'
                # 'provenance': optional field, but always added below
            }
            # Assuming the request line is of the form GET <filename> <protocol>, use the second token
            # from the space-separated string as the full filename.  Then reduce to the relative path.
            try:
                for regex in RETAIN_LINE_RE_LIST:
                    if re.match(regex, log_entry.request_line):
                        logger.debug(f"KBKBKB log_entry.request_line={log_entry.request_line}")
                        request_tokens = log_entry.request_line.split()
                        logger.debug(f"KBKBKB request_tokens={str(request_tokens)}")
                        request_target = request_tokens[1]
                        requested_filename = re.sub(r'\?.*', '', request_target)
                        logged_line_dict['relative_file_path'] = requested_filename
            except Exception as e:
                logger.error(f"Failed to split log_entry.request_line='{log_entry.request_line}' to"
                             f" retrieve filename from second space-separated token due to e={str(e)}")

            # If the relative_file_path was set, use it to determine the dataset_uuid
            if logged_line_dict['relative_file_path']:
                # Expect paths probably start with slash, but filter the empty token rather than strip() the string
                file_path_tokens = [path_elt for path_elt in logged_line_dict['relative_file_path'].split(os.sep) if path_elt]
                if file_path_tokens[0] in ['consortium', 'protected'] and len(file_path_tokens[2]) == 32:
                    logged_line_dict['dataset_uuid'] = file_path_tokens[2]
                elif len(file_path_tokens[0]) == 32:
                    logged_line_dict['dataset_uuid'] = file_path_tokens[0]
                else:
                    logger.error(f"Expected to determine dataset_uuid using"
                                 f" logged_line_dict['relative_file_path']={logged_line_dict['relative_file_path']}"
                                 f" but encountered unexpected format.")

            iso8601_utc_request_time = log_entry.request_time_fields['timestamp'].isoformat().replace('+00:00', 'Z')
            logged_line_dict['provenance'] = copy.deepcopy(provenance_dict)
            # Work up a unique key for this document which can be used as the ElasticSearch document _id.
            src_file_base = logged_line_dict['provenance'][PROC_NAME]['destination_local_file'].replace(f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}"
                                                                                                                         ,''
                                                                                                                         ,1)
            src_file_base = src_file_base.replace('.json','').replace(os.sep,'_')
            transfer_line_number = idx+1
            logged_line_dict['provenance'][PROC_NAME]['source_log_line'] = transfer_line_number
            logged_line_dict['provenance'][PROC_NAME]['es_id'] = f"{src_file_base}" \
                                                                                  f"_{transfer_line_number}"

            log_lines_list.append(logged_line_dict)
        except Exception as e:
            logger.exception(f"Skipped processing line {idx+1} due to exception")
    return log_lines_list

# Given a list of transfer statistics in a dict and the associated file name which
# should contain them, convert the dict to JSON and save in the file.
def save_transfer_stats_as_json(transfer_stats_list:list[dict], json_filename:str):
    logger.info(f"Converting {len(transfer_stats_list)}"
                f" extracted transfer statistics to JSON.")

    if os.path.isfile(json_filename):
        logger.error(f"File '{json_filename}' already exists and will not be overwritten.")
    else:
        session_transfer_stats_json = json.dumps(transfer_stats_list)
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
    global node_log_pcre_pattern
    global node_json_pcre_pattern

    node_log_file_dict = {}
    node_json_file_dict = {}
    log_file_count = 0
    json_file_count = 0
    for node_dir in node_dir_list:
        if node_dir not in node_log_file_dict:
            node_log_file_dict[node_dir] = []
        if node_dir not in node_json_file_dict:
            node_json_file_dict[node_dir] = []
        node_log_dir_fullpath = f"{LOG_FILE_NIGHTLY_DIR}{os.sep}{node_dir}{os.sep}httpd"
        node_json_dir_fullpath = f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}{node_dir}"
        logger.debug(f"Correlate input from logs found at {node_log_dir_fullpath}"
                     f" with JSON found at {node_json_dir_fullpath}.")
        
        #Get the files matching the wildcard pattern
        node_log_file_list = [f"{node_log_dir_fullpath}{os.sep}{f.name}" \
                              for f in Path(node_log_dir_fullpath).iterdir() \
                              if f.is_file() and node_log_pcre_pattern.fullmatch(f.name)]
        node_json_file_list = [f"{node_json_dir_fullpath}{os.sep}{f.name}" \
                               for f in Path(node_json_dir_fullpath).iterdir() \
                               if f.is_file() and node_json_pcre_pattern.fullmatch(f.name)]

        log_file_count += len(node_log_file_list)
        node_log_file_dict[node_dir].extend(node_log_file_list)
        json_file_count += len(node_json_file_list)
        node_json_file_dict[node_dir].extend(node_json_file_list)

    logger.info(f"Found {log_file_count} log files to correlate with {json_file_count} json files.")

    # Identify the input log files for which there is not a
    # corresponding output JSON file.
    parsing_src_dest_dict={}
    for node_dir in node_log_file_dict:
        for input_filename in node_log_file_dict[node_dir]:
            # Put all the *.json files generated in the same directory, rather than having an
            # 'httpd' directory under each node directory like the input has.  Distinction between
            # Globus transfers and HTTP transfers is by name of output file.
            output_filename = re.sub(f"{LOG_FILE_NIGHTLY_DIR}{os.sep}{node_dir}{os.sep}httpd"
                                     ,f"{JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}{os.sep}{node_dir}"
                                     ,input_filename)
            output_filename = f"{output_filename}.json"
            if output_filename in node_json_file_dict[node_dir]:
                logger.info(f"Skip '{input_filename}' because output already exists as '{output_filename}'.")
                continue
            parsing_src_dest_dict[input_filename]=output_filename
    return parsing_src_dest_dict

if __name__ == '__main__':
    msg =   f":large_orange_circle: {portfolio_utils.get_slack_host_context()} :large_orange_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_orange_circle:\n" \
            f"{SLACK_NEUTRAL_INFO_EMOJI} Launched to process Globus access logs\n" \
            f" to create JSON files at {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}.\n" \
            f" {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}.\n" \
            f" Process logging to {log_file_name}\n" \
            f":large_orange_circle:"
    logger.info(msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                     , msg=msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()

    # Log the criteria which will be used to identify log lines which make a
    # session interesting because they indicate a file transfer involved.
    # N.B. There are more regular expression for "lines to retain" for each
    #      file transfer session than there are lines which make the session
    #      "interesting" i.e. the session includes a successful transfer. This
    #      is for dealing with unsuccessful transfers, non-public transfers,
    #      etc. when needed.
    logger.info(f"Identifying sessions with apparent file transfers, based on"
                f" the following criteria for log lines:")
    for regex in RETAIN_LINE_RE_LIST:
        logger.info(f"regex matches '{regex}'")

    # Figure out which log files do not have an accompanying JSON file, and
    # need to be processed
    parsing_src_dest_dict=get_unparsed_log_dict()
    if not parsing_src_dest_dict:
        logger.info(f"No new JSON generated since one exists for each Globus access log files found.")
    else:
        logger.info(f"Found {len(parsing_src_dest_dict)} input files for which to create output JSON.")
        
    # Process logs files and generate accompanying JSON files.
    processed_file_count = 0
    for input_filename in parsing_src_dest_dict.keys():
        logger.info(f"\nLooking for file transfer lines in {input_filename}")

        # Create a provenance dict for the processed log file, which can become a
        # part of each JSON Object of the JSON list that will become a file.
        # Once the file is saved, subsequent processes will modify this provenance
        # data with their own entries.
        node_dirname = os.path.basename(os.path.dirname(os.path.dirname(input_filename)))
        if node_dirname not in node_dir_list:
            logger.error(f"File path element '{node_dirname}' is not recognized as a node in the list {str(node_dir_list)}.")
            node_prefix = ''
        else:
            node_prefix = f"{node_dirname}{os.sep}"
        src_dest_prov_dict = {
            PROC_NAME: {
                'process_script' : os.path.basename(__file__)
                , 'process_utc_dt': datetime.now(tz_utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                , 'source_log_file' : f"{input_filename}"
                , 'destination_local_file' : f"{parsing_src_dest_dict[input_filename]}"
            }
        }

        try:
            # KBKBKB @TODO verify generator/yield lazy reading approach
            #log_lines = get_log_lines_from_gzip_file(file_name=input_filename)
            log_file_lines=[]
            is_gz = input_filename.endswith('.gz') or portfolio_utils.is_gzip_file(input_filename)
            open_func = gzip.open if is_gz else open
            mode = 'rt' if is_gz else 'r'

            try:
                with open_func(input_filename, mode, encoding='utf-8') as f:
                    for line in f:
                        log_file_lines.append(line)
            except Exception as e:
                logger.error(f"Error reading file '{input_filename}': {e}")
            logger.info(f"Read {len(log_file_lines)} log lines from {input_filename}.")

            successful_http_transfers = create_dict_by_session_from_log_lines(log_file_lines = log_file_lines
                                                                              , provenance_dict = src_dest_prov_dict
                                                                              , node_dirname = node_dirname)
            # KBKBKB @TODO pull interesting_lines=pull_interesting_lines(log_line_dict_list=line_dicts_list)
            # KBKBKB @TODO pull logger.info(f"Found {len(interesting_lines)} data file transfer events among {len(line_dicts_list)} Globus access log lines.")

            # KBKBKB @TODO pull successful_http_transfers=identify_successful_http_transfers(file_xfer_lines=interesting_lines)
                
            save_transfer_stats_as_json(transfer_stats_list=successful_http_transfers
                                        , json_filename=parsing_src_dest_dict[input_filename])
            logger.info(f"Saved {len(successful_http_transfers)} file transfer stats to '{parsing_src_dest_dict[input_filename]}'")
            processed_file_count += 1
        except Exception as e:
            logger.exception(f"Error halted processing file {input_filename}.")
            
    process_utc_finish = datetime.now(tz_utc)

    good_news = (f":large_orange_circle: {portfolio_utils.get_slack_host_context()} :large_orange_circle: {PROC_NAME} :diamonds: {Path(__file__).name} :large_orange_circle:\n"
                 f"{SLACK_GOOD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                 f" finished at {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')} after"
                 f" {int((process_utc_finish - process_utc_start).total_seconds() // 60)} minutes.\n"
                 f" Wrote {processed_file_count} JSON files to {JSON_FILE_NIGHTLY_DIR}{os.sep}{PROC_NAME}.\n"
                 f" Process logged to {log_file_name}\n"
                 f"{':orange_heart: ' * 5}\n"
                 f":large_orange_circle:")
    logger.info(good_news)
    try:
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=good_news
                                           , mentions_dict=slack_user_id_mentions_on_success_dict)
    except Exception as e:
        logger.exception('Unable to post Slack success notification.')

    sys.exit(0)
