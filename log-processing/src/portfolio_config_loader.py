"""
portfolio_config_loader.py

Shared utility for loading the logProcessingProject.ini configuration file
used across all scripts in the usage-analytics log-processing portfolio.

Centralizes the candidate path search so individual scripts don't need to
replicate it, and keeps secrets out of scripts that might be committed to GitHub.

Usage:
    from portfolio_config_loader import load_portfolio_config

    portfolio_utils, portfolio_config = load_portfolio_config(
        portfolio_dir='/path/to/log-processing',
        disable_slack_notifications=False
    )
"""

import sys
from pathlib import Path

from log_extract_xfer_utils import LogExtractXferUtils


def load_portfolio_config(portfolio_dir: str = None,
                          disable_slack_notifications: bool = False) -> tuple:
    """
    Locate and load logProcessingProject.ini, returning an initialized
    LogExtractXferUtils instance and the resulting config dict.

    Searches the following candidate locations in order:
      1. logProcessingProject.ini                     (Docker WORKDIR / current directory)
      2. {portfolio_dir}/src/logProcessingProject.ini (vm001 default)
      3. ../../src/logProcessingProject.ini           (PyCharm dev)

    Parameters
    ----------
    portfolio_dir : str, optional
        The portfolio root directory (one level above individual project
        directories, e.g. .../log-processing). When provided, candidate 2
        is checked. May be None if running from Docker WORKDIR or PyCharm.
    disable_slack_notifications : bool
        Pass True to suppress Slack notifications, e.g. during testing.

    Returns
    -------
    tuple of (LogExtractXferUtils, dict)
        The initialized portfolio_utils instance and the portfolio_config dict.

    Raises
    ------
    SystemExit(3)
        If logProcessingProject.ini cannot be found in any candidate location,
        or if LogExtractXferUtils fails to initialize.
    """
    candidates = [
        Path('logProcessingProject.ini'),
    ]
    if portfolio_dir:
        candidates.append(Path(f'{portfolio_dir}/src/logProcessingProject.ini'))
    candidates.append(Path('../../src/logProcessingProject.ini'))

    config_file_location = None
    for candidate in candidates:
        if candidate.is_file():
            config_file_location = candidate.resolve()
            break

    if not config_file_location:
        searched = '\n  '.join(str(c.resolve()) for c in candidates)
        print(f"\a\nUnable to find logProcessingProject.ini in any expected location:\n  {searched}\n")
        sys.exit(3)

    try:
        portfolio_utils = LogExtractXferUtils(
            config_file_name=config_file_location,
            disable_slack_notifications=disable_slack_notifications
        )
        portfolio_config = portfolio_utils.get_config()
    except Exception as e:
        print(f"Error initializing LogExtractXferUtils from '{config_file_location}': {str(e)}")
        sys.exit(3)

    return portfolio_utils, portfolio_config
