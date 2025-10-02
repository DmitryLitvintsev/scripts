#!/usr/bin/env python3

"""
Script to interact with dCache Tape quota API and manage user/group quotas.

Provides functionality to retrieve and display quota information for users and groups.
Uses grid security files for user and group mapping.
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Dict, Tuple, Optional, Any

import requests
from requests.exceptions import HTTPError
import urllib3

urllib3.disable_warnings()

# Constants
BASE_URL = "https://fndca:3880/api/v1/quota"
GID_FILE = "/etc/grid-security/group"
UID_FILE = "/etc/grid-security/passwd"
GB = 1 << 30

# Type aliases
UserInfo = Tuple[int, int, str, str]  # uid, gid, username, name
GidList = Dict[int, str]
UidList = Dict[int, UserInfo]


def load_grid_files() -> Tuple[GidList, UidList]:
    """Load and parse grid security files for group and user information.

    Returns:
        Tuple containing dictionaries for group and user information.
    """
    gid_list: GidList = {}
    uid_list: UidList = {}

    try:
        with open(GID_FILE, "r", encoding="utf-8") as f:
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
        print(f"Warning: Group file {GID_FILE} not found")
    except (ValueError, IndexError) as e:
        print(f"Error parsing group file: {e}")

    try:
        with open(UID_FILE, "r", encoding="utf-8") as f:
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
        print(f"Warning: User file {UID_FILE} not found")
    except (ValueError, IndexError) as e:
        print(f"Error parsing user file: {e}")

    return gid_list, uid_list


class QuotaApi:
    """Class to interact with the dCache Quota API."""

    def __init__(self, url: str = BASE_URL) -> None:
        """Initialize QuotaApi with the given URL.

        Args:
            url: The base URL for the quota API
        """
        self.session = requests.Session()
        self.session.verify = False
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        self.url = url

    def get_user_quota(self) -> Optional[list[dict[str, Any]]]:
        """Retrieve user quota information.

        Returns:
            List of user quota data or None if request fails
        """
        try:
            response = self.session.get(
                f"{self.url}/user",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except HTTPError as exc:
            print(f"HTTP error occurred: {exc}")
        except Exception as exc:
            print(f"Error retrieving user quota: {exc}")
        return None

    def get_group_quota(self) -> Optional[list[dict[str, Any]]]:
        """Retrieve group quota information.

        Returns:
            List of group quota data or None if request fails
        """
        try:
            response = self.session.get(
                f"{self.url}/group",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except HTTPError as exc:
            print(f"HTTP error occurred: {exc}")
        except Exception as exc:
            print(f"Error retrieving group quota: {exc}")
        return None


def main() -> None:
    """Process and display quota information for users or groups."""
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

    args = parser.parse_args()
    if not (args.group or args.user):
        parser.error("Please specify either --group or --user")

    gid_list, uid_list = load_grid_files()
    api = QuotaApi()

    if args.group:
        group_data = api.get_group_quota()
        if group_data:
            for row in group_data:
                gid = row.get("id")
                gname = gid_list.get(gid, "unknown")
                row["group"] = gname
            print(json.dumps(group_data, indent=4, sort_keys=True))

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
            print(json.dumps(user_data, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
