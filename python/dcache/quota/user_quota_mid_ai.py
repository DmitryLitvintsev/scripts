#!/usr/bin/env python3

"""
Script to interact with dCache Tape quota API and manage user/group quotas.

Provides functionality to retrieve and display quota information for users and groups.
Uses grid security files for user and group mapping.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple, Optional, Any, NoReturn

import requests
from requests.exceptions import HTTPError, RequestException
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress only the specific SSL warning
urllib3.disable_warnings(InsecureRequestWarning)

# Constants
DEFAULT_CONFIG = {
    "base_url": "https://fndca:3880/api/v1/quota",
    "gid_file": "/etc/grid-security/group",
    "uid_file": "/etc/grid-security/passwd",
    "cache_timeout": 300,  # 5 minutes
    "request_timeout": 30,  # seconds
}

# Type aliases
UserInfo = Tuple[int, int, str, str]  # uid, gid, username, name
GidList = Dict[int, str]
UidList = Dict[int, UserInfo]


def load_config() -> dict[str, Any]:
    """Load configuration from file or use defaults.

    Returns:
        Dictionary containing configuration values.
    """
    config = DEFAULT_CONFIG.copy()
    config_file = Path.home() / ".config" / "dcache_quota.json"

    try:
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                user_config = json.load(f)
                config.update(user_config)
    except json.JSONDecodeError as e:
        logger.error("Error parsing config file: %s", e)
    except Exception as e:
        logger.error("Error loading config file: %s", e)

    return config


def exit_with_error(message: str, code: int = 1) -> NoReturn:
    """Exit the program with an error message and code.

    Args:
        message: Error message to display
        code: Exit code to use
    """
    logger.error(message)
    sys.exit(code)


@lru_cache(maxsize=128)
def load_grid_files() -> Tuple[GidList, UidList]:
    """Load and parse grid security files for group and user information.

    Returns:
        Tuple containing dictionaries for group and user information.
    """
    config = load_config()
    gid_list: GidList = {}
    uid_list: UidList = {}

    try:
        with open(config["gid_file"], "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(":")
                if len(parts) >= 3:
                    gid = int(parts[2].strip())
                    gname = parts[0].strip()
                    gid_list[gid] = gname
    except FileNotFoundError:
        logger.warning("Group file %s not found", config["gid_file"])
    except (ValueError, IndexError) as e:
        logger.error("Error parsing group file: %s", e)

    try:
        with open(config["uid_file"], "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(":")
                if len(parts) >= 5:
                    uid = int(parts[2].strip())
                    gid = int(parts[3].strip())
                    username = parts[0].strip()
                    name = re.sub('"', "'", parts[4].strip())
                    uid_list[uid] = (uid, gid, username, name)
    except FileNotFoundError:
        logger.warning("User file %s not found", config["uid_file"])
    except (ValueError, IndexError) as e:
        logger.error("Error parsing user file: %s", e)

    return gid_list, uid_list


class QuotaApi:
    """Class to interact with the dCache Quota API."""

    def __init__(self) -> None:
        """Initialize QuotaApi with configuration."""
        self.config = load_config()
        self.session = requests.Session()
        self.session.verify = False
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

    @lru_cache(maxsize=32)
    def get_user_quota(self) -> Optional[list[dict[str, Any]]]:
        """Retrieve user quota information.

        Returns:
            List of user quota data or None if request fails
        """
        try:
            response = self.session.get(
                f"{self.config['base_url']}/user",
                headers=self.headers,
                timeout=self.config["request_timeout"]
            )
            response.raise_for_status()
            return response.json()
        except RequestException as exc:
            logger.error("HTTP error occurred: %s", exc)
        except json.JSONDecodeError as exc:
            logger.error("Error parsing JSON response: %s", exc)
        except Exception as exc:
            logger.error("Error retrieving user quota: %s", exc)
        return None

    @lru_cache(maxsize=32)
    def get_group_quota(self) -> Optional[list[dict[str, Any]]]:
        """Retrieve group quota information.

        Returns:
            List of group quota data or None if request fails
        """
        try:
            response = self.session.get(
                f"{self.config['base_url']}/group",
                headers=self.headers,
                timeout=self.config["request_timeout"]
            )
            response.raise_for_status()
            return response.json()
        except RequestException as exc:
            logger.error("HTTP error occurred: %s", exc)
        except json.JSONDecodeError as exc:
            logger.error("Error parsing JSON response: %s", exc)
        except Exception as exc:
            logger.error("Error retrieving group quota: %s", exc)
        return None


def format_quota_data(data: list[dict[str, Any]]) -> str:
    """Format quota data for output.

    Args:
        data: List of quota data dictionaries

    Returns:
        Formatted JSON string
    """
    try:
        return json.dumps(data, indent=4, sort_keys=True)
    except Exception as e:
        logger.error("Error formatting output: %s", e)
        return json.dumps({"error": str(e)})


def main() -> None:
    """Process and display quota information for users or groups."""
    try:
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description="Example script to use dCache Tape quota API"
        )

        parser.add_argument(
            "-g",
            "--group",
            action="store_true",
            help="Display group quota information"
        )
        parser.add_argument(
            "-u",
            "--user",
            action="store_true",
            help="Display user quota information"
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Enable verbose logging"
        )

        args = parser.parse_args()

        if args.verbose:
            logger.setLevel(logging.DEBUG)

        if not (args.group or args.user):
            exit_with_error("Please specify either --group or --user")

        gid_list, uid_list = load_grid_files()
        api = QuotaApi()

        if args.group:
            group_data = api.get_group_quota()
            if group_data:
                for row in group_data:
                    gid = row.get("id")
                    gname = gid_list.get(gid, "unknown")
                    row["group"] = gname
                print(format_quota_data(group_data))
            else:
                exit_with_error("Failed to retrieve group quota data")

        if args.user:
            user_data = api.get_user_quota()
            if user_data:
                for row in user_data:
                    uid = row.get("id")
                    user_info = uid_list.get(uid)
                    uname = "unknown"
                    full_name = "unknown"

                    if user_info:
                        uname = user_info[2]
                        full_name = user_info[3]

                    row["name"] = full_name
                    row["username"] = uname
                print(format_quota_data(user_data))
            else:
                exit_with_error("Failed to retrieve user quota data")

    except KeyboardInterrupt:
        exit_with_error("Operation cancelled by user", 130)
    except Exception as e:
        exit_with_error(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
