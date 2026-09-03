import sys
import os
import json
import logging
import configparser
import socket
import requests
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning

logger = logging.getLogger(__name__)

from datetime import datetime
from enum import Enum

def parse_datetime_flexible(value: str, alt_fmt_list: list = []) -> datetime:
    """Parse a datetime string against an ordered list of strptime format candidates.

    Formats in alt_fmt_list are tried first, followed by the built-in DEFAULT_FMT_LIST.
    The first format that parses without error wins.  If all candidates are exhausted,
    a ValueError is raised.

    DEFAULT_FMT_LIST covers the two format families seen in Globus log data:
      - 'YYYY-MM-DD HH:MM:SS.ffffff' and 'YYYY-MM-DD HH:MM:SS'  (CSV request_time field)
      - 'YYYYMMDDHHMMSSffffff'        and 'YYYYMMDDHHMMSS'       (gridftp Transfer stats)

    Args:
        value:        The datetime string to parse.
        alt_fmt_list: Optional list of strptime format strings to try before DEFAULT_FMT_LIST.
                      Defaults to the empty list.

    Returns:
        A datetime object parsed from value.

    Raises:
        ValueError: if value matches none of the candidate formats.
    """
    DEFAULT_FMT_LIST = [
        '%Y%m%d%H%M%S.%f',
        '%Y%m%d%H%M%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
    ]
    for fmt in alt_fmt_list + DEFAULT_FMT_LIST:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"parse_datetime_flexible: '{value}' did not match any of the"
                     f" candidate formats: {alt_fmt_list + DEFAULT_FMT_LIST}")
class LogFileStatusType(Enum):
    UNPROCESSED = 'unprocessed'
    PROCESSED_TO_JSON = 'processed to JSON'
    PROCESSED_TO_S3 = 'JSON uploaded to S3'

class LogExtractXferUtils:

    portfolio_config_dict = {}
    # If the process code calling this class's constructor indicates
    # Slack notifications should be disabled (e.g. during development),
    # set this global to True
    slack_notification_disabled = False

    def __init__(self, config_file_name:str, disable_slack_notifications=False):
        global slack_notification_disabled

        slack_notification_disabled = disable_slack_notifications

        #
        # Read configuration from the project INI file and set global constants
        #
        config = configparser.ConfigParser()

        if not os.path.exists(config_file_name):
            raise FileNotFoundError(f"Unable to find project initialization file {config_file_name}.")

        config.read(config_file_name)

        # Put the configuration read in the log when debugging
        logger.debug(f"Begin configuration read from config_file_name={config_file_name}:")
        for section in config.sections():
            logger.debug(f"[{section}]")
            for key, val in config.items(section):
                logger.debug(f"{key} = {val}")
        logger.debug(f"End configuration read from the initialization file.")
        
        self.portfolio_config_dict['TRANSFER_DETAIL_FILE'] = config.get('LocalServerSettings', 'TRANSFER_DETAIL_FILE')
        self.portfolio_config_dict['NONPUBLIC_GEO_DB'] = config.get('LocalServerSettings', 'NONPUBLIC_GEO_DB')
        self.portfolio_config_dict['JSON_FILE_NIGHTLY_DIR'] = config.get('LocalServerSettings', 'JSON_FILE_NIGHTLY_DIR')
        self.portfolio_config_dict['PROJECT_HIVE_DIR'] = config.get('LocalServerSettings', 'PROJECT_HIVE_DIR')
        self.portfolio_config_dict['PROJECT_DEV_DIR'] = config.get('LocalServerSettings', 'PROJECT_DEV_DIR')
        self.portfolio_config_dict['ABS_PATH_BASE_TO_REMOVE'] = config.get('LocalServerSettings', 'ABS_PATH_BASE_TO_REMOVE')

        self.portfolio_config_dict['AWS_ACCESS_KEY_ID'] = config.get('AWSS3ProjectSettings', 'AWS_ACCESS_KEY_ID')
        self.portfolio_config_dict['AWS_SECRET_ACCESS_KEY'] = config.get('AWSS3ProjectSettings', 'AWS_SECRET_ACCESS_KEY')
        self.portfolio_config_dict['AWS_S3_BUCKET_NAME'] = config.get('AWSS3ProjectSettings', 'AWS_S3_BUCKET_NAME')
        self.portfolio_config_dict['AWS_FOLDER_DELIM'] = config.get('AWSS3ProjectSettings', 'AWS_FOLDER_DELIM')
        self.portfolio_config_dict['AWS_ELASTICSEARCH_URL'] = config.get('AWSS3ProjectSettings', 'AWS_ELASTICSEARCH_URL')
        self.portfolio_config_dict['AWS_REGION_NAME'] = config.get('AWSS3ProjectSettings', 'AWS_REGION_NAME')
        self.portfolio_config_dict['SLACK_SUPPORTED_CHANNELS'] = config.get('SlackNotificationSettings', 'SLACK_SUPPORTED_CHANNELS')
        self.portfolio_config_dict['SLACK_CHANNEL_TOKEN'] = config.get('SlackNotificationSettings', 'SLACK_CHANNEL_TOKEN')
        self.portfolio_config_dict['SLACK_MAX_MSG_LENGTH'] = int(config.get('SlackNotificationSettings', 'SLACK_MAX_MSG_LENGTH'))
        
        print(f"Project configuration loaded from {config_file_name}.")

    def get_config(self)->dict:
        return self.portfolio_config_dict

    # Get a dict from the JSON which tracks the log files previously processed.
    def get_tracking_from_file(self, filename:str)->dict:
        with open(filename, 'r') as f:
            json_as_dict=json.load(f)
        return json_as_dict

    # Save a dict which tracks the log files previously processed as JSON.
    def overwrite_tracking_to_file(self, filename:str, pydict:dict):
        dict_as_json=json.dumps(obj=pydict
                                , indent=2
                                , sort_keys=True)
        if os.path.isfile(filename):
            logger.info(f"Overwriting file '{filename}'.")
        with open(filename, 'w') as jf:
            jf.write(dict_as_json)
        logger.info(f"Wrote {len(dict_as_json)} bytes of JSON to '{filename}'\n")

    # Given a non-empty list, log the reasons processing is being halted, then exit
    def exit_if_halt_reason(self, halt_reasons=[], slack_channel=None, mentions_dict:dict=None,
                            process_bad_news_emoji=':bangbang:', exit_code=2):
        if not halt_reasons:
            return
        bad_news = f"{process_bad_news_emoji} Processing halted for the following logged reasons:"
        logger.error(f" Processing halted for the following reasons:")
        for idx, halt_reason in enumerate(halt_reasons):
            logger.error(f"\t{halt_reason}")
            bad_news += f"\n{idx+1}) {halt_reason}"
        if slack_channel:
            self.postToSlackChannel(channel = slack_channel
                                    , msg = bad_news
                                    , mentions_dict = mentions_dict)
        sys.exit(exit_code)

    def get_nested_value_recursive(self, obj, dotted_path):
        if not dotted_path:
            return obj
        if not isinstance(obj, dict):
            return None

        if not '.' in dotted_path:
            return obj.get(dotted_path)
    
        head, tail = dotted_path.split('.', 1)
        return self.get_nested_value_recursive(obj.get(head), tail)

    def is_gzip_file(self, file_path):
        try:
            with open(file_path, 'rb') as f:
                return f.read(2) == b'\x1f\x8b'
        except Exception as e:
            logger.error(f"Could not check gzip magic number: {e}")
            return False

    ####################################################################################################
    ## Host / Container Context (for Slack messages)
    ####################################################################################################

    def get_short_hostname(self) -> str:
        """
        Return the short hostname (no domain suffix), e.g. "dtn03".

        On bare metal, this is equivalent to `hostname -s` / the
        `cat /etc/hostname | sed 's/\\..*//'` pipeline: socket.gethostname()
        reads the same kernel-level hostname, and .split(".")[0] strips any
        domain suffix the same way the sed does.

        Inside a container, though, the kernel-level hostname is normally the
        container's own hostname (often a random ID Docker assigns), NOT the
        name of the physical or VM host it's running on. To keep host identity
        meaningful after a move to Docker, a container launcher should export
        HOST_HOSTNAME (e.g. `-e HOST_HOSTNAME=$(hostname -s)` on `docker run`,
        or the equivalent in a compose file or wrapper script). When present,
        that value takes priority over the kernel hostname.
        """
        override = os.environ.get('HOST_HOSTNAME')
        if override:
            return override.split('.')[0]
        return socket.gethostname().split('.')[0]

    def is_running_in_container(self) -> bool:
        """
        Best-effort detection of whether this process is running inside a
        Docker (or Docker-like) container.

        Checks, in order:
          1. /.dockerenv -- present in essentially all Docker containers.
          2. /proc/1/cgroup -- looks for docker/containerd/kubepods markers,
             as a fallback for setups where /.dockerenv isn't present
             (e.g. some Kubernetes runtimes).
        """
        if os.path.exists('/.dockerenv'):
            return True

        try:
            with open('/proc/1/cgroup', 'rt') as f:
                content = f.read()
            if any(marker in content for marker in ('docker', 'containerd', 'kubepods')):
                return True
        except (FileNotFoundError, PermissionError):
            pass

        return False

    def get_slack_host_context(self) -> str:
        """
        A single string to drop into Slack messages, e.g.:
            "dtn03 (bare metal)"
            "dtn03 (container)"
        """
        host = self.get_short_hostname()
        location = 'container' if self.is_running_in_container() else 'bare metal'
        return f"{host} ({location})"

    ####################################################################################################
    ## Slack Notification
    ####################################################################################################

    # Send an email with the specified text in the body and the specified subject line to
    # the  data curation/ingest staff email addresses specified in the portfolio_config_dict MAIL_ADMIN_LIST entry.
    def email_admin_list(message_text, subject):
        global slack_notification_disabled

        if slack_notification_disabled:
            return
        msg = Message(  body=message_text
                        ,recipients=self.portfolio_config_dict['MAIL_ADMIN_LIST']
                        ,subject=subject)
        flask_mail.send(msg)

    """
    Post a string to target Slack channel

    Input
    --------
    POST request body data is a JSON object containing the following fields:
        message : str
            The message to be sent to the channel. Required.
        channel : str
            The target Slack channel. Optional, with default from configuration used if not specified.
        send_to_email : bool
            Indication if the message should also be sent via email to addresses configured in MAIL_ADMIN_LIST.
            Optional, defaulting to False when not in the JSON.
    Returns
    --------
    dict
        Dictionary with separate dictionary entries for 'Slack' and 'Email', each containing a summary of the notification.
    """
    def postToSlackChannel(self, channel:str, msg:str, mentions_dict:dict=None):
        global slack_notification_disabled
        
        if slack_notification_disabled:
            return
        
        # Not doing user authorization for this utility, which is only for use on internal apps

        if not channel:
            raise Exception('The Slack channel to post a message to must be specified.')
        if channel not in self.portfolio_config_dict['SLACK_SUPPORTED_CHANNELS']:
            raise Exception('A supported Slack channel must be specified to post a message.')

        if not msg:
            raise Exception('The message to post to Slack must be a non-blank string.')
        if len(msg) > self.portfolio_config_dict['SLACK_MAX_MSG_LENGTH']:
            raise Exception(f"The message to post to Slack must be"
                            f" {self.portfolio_config_dict['SLACK_MAX_MSG_LENGTH']}"
                            f" characters or less.")
        # If any mentions are to occur, pull them from mentions_dict for inclusion.
        mention_text = ''
        if mentions_dict:
            mention_text = '\n\nAttn:'
            for dev_name, slack_user_id in mentions_dict.items():
                mention_text += f" <@{slack_user_id}>"
                
        # Send message to Slack
        target_url = 'https://slack.com/api/chat.postMessage'
        request_header = {
            "Authorization": f"Bearer {self.portfolio_config_dict['SLACK_CHANNEL_TOKEN']}"
        }
        json_to_post = {
            "channel": channel
            , "text": msg + mention_text
        }

        logger.debug("======postToSlackChannel() json_to_post======")
        logger.debug(json_to_post)

        response = requests.post(url = target_url, headers = request_header, json = json_to_post, verify = False)

        notification_results = {'Slack': None}
        # Note: Slack API wraps the error response in the 200 response instead of using non-200 status code
        # Callers should always check the value of the 'ok' params in the response
        if response.status_code == 200:
            result = response.json()
            # 'ok' filed is boolean value
            if 'ok' in result:
                if result['ok']:
                    output = {
                        "channel": channel,
                        "message": msg
                    }
    
                    logger.debug("======notify() Sent Notification Summary======")
                    logger.info(output)

                    return output
                else:
                    logger.error(f"Unable to notify Slack channel: {channel} with the msg: {msg}")
                    logger.debug("======notify() response json from Slack API======")
                    logger.debug(result)

                    # https://api.slack.com/methods/chat.postMessage#errors
                    if 'error' in result:
                        raise Exception(result['error'])
                    else:
                        raise Exception("Slack API unable to process the request, 'error' param/field missing from Slack API response json")
            else:
                raise Exception("The 'ok' param/field missing from Slack API response json")
        else:
            raise Exception("Failed to send a request to Slack API")

        return # Shouldn't get here
