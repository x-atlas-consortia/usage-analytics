import os
import argparse
import sys
import configparser
import logging
import json
import ast
from zoneinfo import ZoneInfo
from datetime import datetime
from pathlib import Path

# Import project resources
from log_extract_xfer_utils import LogExtractXferUtils

# Monitor files which are data sources for the log-processing portfolio.
# File locations to monitor are pulled from the [LocalServerSettings] section of the
# portfolio's logProcessingProject.ini file, although this could be extended to pull
# file locations from the configuration of other projects within log-processing.
# Locactions are NOT specified in the INI file of this project.
#

tz_utc = ZoneInfo("UTC")
process_utc_start = datetime.now(tz_utc)

print('Monitoring delivery of portfolio data source files')

# Handle arguments
arg_parser = argparse.ArgumentParser(description='Monitor delivery of one or more portfolio data source files.')
arg_parser.add_argument('--process-dir', required=True, dest='process_dir',
                         help="This process's own directory (one level above src/), e.g."
                              " .../log-processing/portfolio-monitoring. Normally supplied"
                              " by the .sh wrapper's own PROCESS_DIR.")
args = arg_parser.parse_args()
# Expect an argument for the path for this project passed in from the invoking shell script
arg_process_dir = args.process_dir
arg_portfolio_dir = os.path.dirname(arg_process_dir)  # one level above arg_process_dir

#
# Read configuration from the project INI file and set global constants
#
Config = configparser.ConfigParser()

process_ini_candidates = [
    Path('file_data_source_monitor.ini'),                                    # Docker WORKDIR
    Path(f'{arg_process_dir}/src/file_data_source_monitor.ini'),             # vm001/dtn03 default
    Path('../../portfolio-monitoring/src/file_data_source_monitor.ini'),     # PyCharm dev
]
config_file_name = None
for candidate in process_ini_candidates:
    if candidate.is_file():
        config_file_name = str(candidate.resolve())
        break
if not config_file_name:
    print(f"\a\nUnable to find file_data_source_monitor.ini in any expected location.\n")
    sys.exit(3)
Config.read(config_file_name)

# This process monitors files characterized by an element of this tuple.
# APPEND_ONLY files are considered to have a problem if their size decreases from the previous run.
# VARIABLE files may decrease in size without it being a problem.
FILE_SIZE_CHARACTERISTICS = ('VARIABLE', 'APPEND_ONLY')

try:
    PROC_NAME = Config.get('ProcessSpecificSettings', 'PROC_NAME')
    SLACK_NOTIFICATION_CHANNEL = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATION_CHANNEL')
    SLACK_BAD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_BAD_NEWS_EMOJI')
    SLACK_NEUTRAL_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_NEUTRAL_NEWS_EMOJI')
    SLACK_GOOD_NEWS_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_GOOD_NEWS_EMOJI')
    SLACK_START_INFO_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_START_INFO_EMOJI')
    SLACK_BAD_ITEM_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_BAD_ITEM_EMOJI')
    SLACK_NEUTRAL_ITEM_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_NEUTRAL_ITEM_EMOJI')
    SLACK_GOOD_ITEM_EMOJI = Config.get('ProcessSpecificSettings', 'SLACK_GOOD_ITEM_EMOJI')
    SLACK_NOTIFICATIONS = Config.get('ProcessSpecificSettings', 'SLACK_NOTIFICATIONS')
    slack_user_id_mentions_on_error_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_ERROR'))
    slack_user_id_mentions_on_success_dict = ast.literal_eval(Config.get('ProcessSpecificSettings', 'SLACK_USER_ID_MENTIONS_ON_SUCCESS'))

    # Which shared-config keys to monitor, and this monitor's own per-file policy for each.
    # The keys themselves are resolved against portfolio_config further down, once
    # LogExtractXferUtils has loaded the shared logProcessingProject.ini.
    FILE_KEYS = [k.strip() for k in Config.get('MonitoredFiles', 'FILE_KEYS').split(',') if k.strip()]
    if not FILE_KEYS:
        raise ValueError("[MonitoredFiles] FILE_KEYS is empty -- nothing to monitor.")

    file_settings = {}  # file_key -> {'file_size_characteristic': str, 'stale_threshold_hours': float}
    for file_key in FILE_KEYS:
        if not Config.has_section(file_key):
            raise ValueError(f"FILE_KEYS lists '{file_key}' but there is no [{file_key}] section"
                              f" declaring its FILE_SIZE_CHARACTERISTICS and STALE_THRESHOLD_HOURS.")
        file_size_characteristic = Config.get(file_key, 'FILE_SIZE_CHARACTERISTIC')
        if file_size_characteristic not in FILE_SIZE_CHARACTERISTICS:
            raise ValueError(f"[{file_key}] FILE_SIZE_CHARACTERISTIC='{file_size_characteristic}' is not one of {FILE_SIZE_CHARACTERISTICS}.")
        # How many hours may pass since the file's own mtime before a delivery is considered
        # missed. Per-file and explicitly configurable, since "roughly once a day" and "roughly
        # once a month" (or any other cadence) need very different thresholds.
        file_settings[file_key] = {
            'file_size_characteristic': file_size_characteristic,
            'stale_threshold_hours': float(Config.get(file_key, 'STALE_THRESHOLD_HOURS')),
        }
except Exception as e:
    print(f"\nUnable to read configuration from '{config_file_name}': {e}\n")
    sys.exit(3)
print('Process-specific configuration loaded')

#
# Set up a logger in the configured directory for the current execution.
#
exec_info_dir_candidates = [
    Path('exec_info'),                                    # Docker WORKDIR
    Path(f'{arg_process_dir}/exec_info'),                 # vm001/dtn03 default
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
                f"/file_data_source_monitor-" \
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
        Path(f'{arg_portfolio_dir}/src/logProcessingProject.ini'),          # vm001/dtn03 default
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
    logger.info("LogExtractXferUtils instantiated.")
except Exception as e:
    print(f"Error configuring for startup due to e={str(e)}")
    logger.critical(f"Error configuring for startup due to e={str(e)}")
    sys.exit(3)

# Resolve each monitored file's actual path using the shared portfolio config from
# logProcessingProject.ini, as loaded by LogExtractXferUtils.
try:
    for file_key in FILE_KEYS:
        if file_key not in portfolio_config:
            raise KeyError(
                f"FILE_KEYS entry '{file_key}' is not exposed by LogExtractXferUtils.get_config()."
                f" Either it's misspelled, or its path currently lives only in some other"
                f" project's own .ini and hasn't been added to the shared logProcessingProject.ini"
                f" (and LogExtractXferUtils) yet."
            )
        file_settings[file_key]['path'] = portfolio_config[file_key]
except Exception as e:
    print(f"Error resolving monitored file paths due to e={str(e)}")
    logger.critical(f"Error resolving monitored file paths due to e={str(e)}")
    sys.exit(3)
print('Portfolio configuration loaded; resolved paths for: ' + ', '.join(FILE_KEYS))

# State for this script is in a JSON file in the same directory as this script. The state
# file is keyed by FILE_KEY, tracking only what's needed to compare "today" against
# "yesterday" for each monitored file.
# Written once per run, after every file has been checked.
STATE_FILE = str(Path(arg_process_dir) / 'src' / 'file_data_source_monitor_state.json')

def load_state() -> dict:
    if not os.path.isfile(STATE_FILE):
        return {}
    with open(STATE_FILE, 'r') as f:
        return json.load(f)


def save_state(all_state: dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(all_state, f, indent=2)


        # Setting occasionally used for debugging. Could be passed in if usage expanded.
verbose=True

# Verify any expectations about the configuration are valid. Print
# messages for each expectation not met and halt if there are any.
def verify_configuration_expectations():
    global node_dir_list
    
    exit_rather_than_return=False
    if not os.path.exists(exec_info_dir):
        print(f"Halting program due to not finding exec_info_dir at "
              f"'{exec_info_dir}' relative to '{os.getcwd()}'.")
        exit_rather_than_return = True
    if exit_rather_than_return:
        bad_news = (f":package: {portfolio_utils.get_slack_host_context()} :package: {PROC_NAME} :diamonds: {Path(__file__).name} :package:\n"
                    f"{SLACK_BAD_NEWS_EMOJI} The process started at {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    f" exited after {int((datetime.now(tz_utc) - process_utc_start).total_seconds())} seconds.\n"
                    f" Halted trying to verify configuration expectations.\n"
                    f" See the logs.\n"
                    f" Process logged to {log_file_name}\n"
                    f"{':package::skull_and_crossbones: ' * 5}\n"
                    f":package:")
        logger.error(bad_news)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           , msg=bad_news
                                           , mentions_dict=slack_user_id_mentions_on_error_dict)
        sys.exit(2)
        


# Deliberately simple counting of newlines by iterating the file as bytes, no parsing.
# Cheap enough even at ~800MB (a few seconds), and no current need for anything more elaborate.
def count_lines(path: str) -> int:
    count = 0
    with open(path, 'rb') as f:
        for _ in f:
            count += 1
    return count


# Evaluate a single monitored file.  Read-only inspection (exists/access/stat) of the
# monitored file at the path given.
#
# file_size_characteristic governs how a size change is classified:
#   APPEND_ONLY -- the file should only ever grow between deliveries. Shrinking always lands in
#                  halt_reasons (a problem, same severity as staleness). Growing is a delivery.
#   VARIABLE    -- the file is periodically replaced wholesale (e.g. a downloaded reference
#                  database). Both growing and shrinking are normal and never land in
#                  halt_reasons -- callers report either as a (neutral) delivery instead.
# Staleness (no new delivery within stale_threshold_hours) is always a halt_reason, regardless
# of file_size_characteristic.
#
# Returns a dict describing what was found. Callers decide what to do about it.
def evaluate_file_status(path: str, stale_threshold_hours: float, file_size_characteristic: str,
                          prior_state: dict, now_utc: datetime) -> dict:
    result = {
        'exists': False, 'readable': False, 'size': None, 'mtime_utc': None,
        'eval_findings': [], 'is_first_run': False,
        'grew': False, 'shrank': False, 'unchanged': False,
        'need_line_count': False,
    }

    if not os.path.exists(path):
        result['eval_findings'].append(f"File does not exist: {path}")
        return result
    result['exists'] = True

    if not os.access(path, os.R_OK):
        result['eval_findings'].append(f"Lost read permission on file: {path}")
        return result
    result['readable'] = True

    st = os.stat(path)
    size = st.st_size
    # We are typically monitoring files dropped onto the /hive filesystem, but
    # they may be put there by servers which will use UTC time (e.g. dtn03) or
    # which will use local time (e.g. vm001.)
    # If we need to switch to per-monitored-file tz entries, it will be time to
    # pull the per-monitored-file settings from the INI file and move them to
    # JSON objects in another configuration file.
    mtime_utc = datetime.fromtimestamp(st.st_mtime, tz=tz_utc)
    result['size'] = size
    result['mtime_utc'] = mtime_utc

    hours_since_modified = (now_utc - mtime_utc).total_seconds() / 3600
    if hours_since_modified > stale_threshold_hours:
        result['eval_findings'].append(
            f"No new delivery in the past {hours_since_modified:.1f} hours (threshold"
            f" {stale_threshold_hours}h) -- last modified {mtime_utc.isoformat()}"
        )

    if prior_state is None:
        result['is_first_run'] = True
        result['need_line_count'] = True
        return result

    if size < prior_state['size']:
        result['shrank'] = True
        # Still need a fresh line count even when file_size_characteristic suppresses the alert --
        # otherwise tomorrow's comparison would be working from a stale baseline.
        result['need_line_count'] = True
        if file_size_characteristic == 'APPEND_ONLY':
            result['eval_findings'].append(
                f"File size decreased from {prior_state['size']:,} to {size:,} bytes"
            )
        # VARIABLE: shrink is expected/benign for a periodically-replaced file -- no
        # halt_reason. Still surfaced as a (neutral) delivery by the caller below.
    elif size > prior_state['size']:
        result['grew'] = True
        result['need_line_count'] = True
    else:
        result['unchanged'] = True
        result['need_line_count'] = False

    return result


if __name__ == '__main__':
    header = (f":package: {portfolio_utils.get_slack_host_context()} :package: {PROC_NAME}"
              f" :diamonds: {Path(__file__).name} :package:\n")

    launch_msg = (header +
                  f"{SLACK_START_INFO_EMOJI} Launched to check delivery of {len(FILE_KEYS)} monitored"
                  f" file(s): {', '.join(FILE_KEYS)}\n"
                  f" Process logging to {log_file_name}\n"
                  f":package:")
    logger.info(launch_msg)
    portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL, msg=launch_msg)

    # Exit if anything loaded from the INI files doesn't match what is found
    # in the file system, or any other expectations are not met.
    verify_configuration_expectations()
    
    now_utc = datetime.now(tz_utc)
    all_prior_state = load_state()
    all_new_state = dict(all_prior_state)

    # Determine the state and accumulate problem descriptions for every monitored file.
    master_eval_findings = []
    per_file_findings = {}  # file_key -> {status, file_size_characteristic, line_count, prior_state}

    for file_key in FILE_KEYS:
        settings = file_settings[file_key]
        path = settings['path']
        file_size_characteristic = settings['file_size_characteristic']
        stale_threshold_hours = settings['stale_threshold_hours']
        prior_state = all_prior_state.get(file_key)

        status = evaluate_file_status(path=path
                                      ,stale_threshold_hours=stale_threshold_hours
                                      ,file_size_characteristic=file_size_characteristic
                                      ,prior_state=prior_state
                                      ,now_utc=now_utc)

        # Existence/permission failures mean nothing was obtained at all -- no size/mtime to
        # persist, state for this file is left exactly as it was.
        #
        # KBKBKB is this redundant with below?
        #
        # if not status['exists'] or not status['readable']:
        #     for reason in status['eval_findings']:
        #         master_eval_findings.append(f"[{file_key}] {reason}")

        # File exists and is readable -- always safe to persist its current size/mtime/line
        # count, whether or not a staleness or (APPEND_ONLY) shrink problem is about to be
        # reported below. Otherwise tomorrow's comparison would be working from a stale
        # baseline, exactly as noted in evaluate_file_status()'s own comments.
        line_count = count_lines(path) if status['need_line_count'] else prior_state['line_count']
        all_new_state[file_key] = {
            'last_check_utc': now_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'size': status['size'],
            'mtime_utc': status['mtime_utc'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'line_count': line_count,
        }

        if status['eval_findings']:
            for reason in status['eval_findings']:
                master_eval_findings.append(f"[{file_key}] {reason}")

        per_file_findings[file_key] = {
            'status': status, 'file_size_characteristic': file_size_characteristic, 'line_count': line_count,
            'prior_state': prior_state,
        }

    logger.info(f"KBKBKB-per_file_findings={str(per_file_findings)}")
    save_state(all_new_state)

    # Nothing went wrong anywhere -- every file in FILE_KEYS is in per_file_findings.
    # Post to Slack with good news if any file looks like it just received a new
    # delivery, or with neutral news if every monitored file simply matches the last run.
    eval_affirmations = []
    any_delivery = False
    for file_key in FILE_KEYS:
        if file_key in per_file_findings:
            finding = per_file_findings[file_key]
            status = finding['status']
            file_size_characteristic = finding['file_size_characteristic']
            line_count = finding['line_count']
            prior_state = finding['prior_state']

            if status['is_first_run']:
                eval_affirmations.append(f"{SLACK_NEUTRAL_ITEM_EMOJI} {file_key}: monitoring initialized -- no prior"
                                         f" baseline existed yet. Current: {status['size']:,} bytes, {line_count:,}"
                                         f" lines, last modified {status['mtime_utc'].isoformat()}.")
            elif status['grew'] or status['shrank']:
                any_delivery = True
                delta_bytes = status['size'] - prior_state['size']
                delta_lines = line_count - prior_state['line_count']
                direction = 'grew' if status['grew'] else 'shrank'
                line_emoji = SLACK_GOOD_ITEM_EMOJI if file_size_characteristic == 'APPEND_ONLY' else SLACK_NEUTRAL_ITEM_EMOJI
                eval_affirmations.append(f"{line_emoji} {file_key}: new delivery detected -- {direction} from"
                                         f" {prior_state['size']:,} to {status['size']:,} bytes ({delta_bytes:+,}),"
                                         f" {delta_lines:+,} lines (now {line_count:,} total).")
            else:
                eval_affirmations.append(f"{SLACK_NEUTRAL_ITEM_EMOJI} {file_key}: unchanged at {status['size']:,} bytes, last modified {status['mtime_utc'].strftime('%Y-%m-%d')}.")

    process_utc_finish = datetime.now(tz_utc)
    detail = "\n".join(eval_affirmations)

    if any_delivery:
        summary_msg = (header +
                       f"{SLACK_GOOD_NEWS_EMOJI} New delivery detected. The process started at"
                       f" {process_utc_start.strftime('%Y-%m-%d %H:%M:%S %Z')} finished at"
                       f" {process_utc_finish.strftime('%Y-%m-%d %H:%M:%S %Z')}.\n"
                       f"{detail}\n"
                       f"{':truck: ' * 5}\n:package:")
        logger.info(summary_msg)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL
                                           ,msg=summary_msg
                                           ,mentions_dict=slack_user_id_mentions_on_success_dict)
    else:
        summary_msg = (header +
                       f"{SLACK_NEUTRAL_NEWS_EMOJI} Status check complete -- no problems found.\n"
                       f"{detail}\n"
                       f"{':package: ' * 5}\n:package:")
        logger.info(summary_msg)
        portfolio_utils.postToSlackChannel(channel=SLACK_NOTIFICATION_CHANNEL, msg=summary_msg)

    sys.exit(0)
