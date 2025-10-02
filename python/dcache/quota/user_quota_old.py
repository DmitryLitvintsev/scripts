#!/usr/bin/env python

import argparse
import json
import requests
from requests.exceptions import HTTPError
import os
import sys
import time
import re

import urllib3
urllib3.disable_warnings()

base_url="https://fndca:3880/api/v1/quota"
GID_FILE = "/etc/grid-security/group"
UID_FILE = "/etc/grid-security/passwd"
GB=1<<30

gid_list={}
uid_list={}

with open(GID_FILE, "r") as f:
  for line in f:
    if not line : continue
    parts = line.strip().split(":")
    gid = int(parts[2].strip())
    gname = parts[0].strip()
    gid_list[gid] = gname

with open(UID_FILE, "r") as f:
  for line in f:
    if not line : continue
    parts = line.strip().split(":")
    uid = int(parts[2].strip())
    gid = int(parts[3].strip())
    username = parts[0].strip()
    name     =  re.sub("\"","'",parts[4].strip())
    uid_list[uid] = (uid, gid, username, name)

class QuotaApi:

    def __init__(self, url=base_url):
        self.session = requests.Session()
        self.session.verify = False

        self.headers = { "accept" : "application/json",
                         "content-type" : "application/json"}
        self.url = url


    def getUserQuota(self):
        payload = None
        try:
            r = self.session.get(self.url+"/user",
                                  headers=self.headers)
            r.raise_for_status()
            payload =  r.json()
        except HTTPError as exc:
            print(exc)

        return payload

#        if payload:
#            print(json.dumps(payload, indent=4,sort_keys=True))


    def getGroupQuota(self):
        payload = None
        try:
            r = self.session.get(self.url+"/group",
                                  headers=self.headers)
            r.raise_for_status()
            payload =  r.json()
        except HTTPError as exc:
            print(exc)

        return payload

#        if payload:
#            print(json.dumps(payload, indent=4,sort_keys=True))


def main():
    """
    main function
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Example script to use dCache Tape quota API"
        )

    parser.add_argument('-g', "--group",  action='store_true')
    parser.add_argument('-u', "--user",  action='store_true')

    args = parser.parse_args()

    api = QuotaApi()

    group_data = None
    user_data = None
    if args.group:
      group_data = api.getGroupQuota()
      # {'id': 9010, 'type': 'GROUP', 'custodial': 34582709661884824, 'replica': 3339680267549938, 'replicaLimit': 7664550941818880}
      for row in group_data:
        gid = row.get("id")
        gname = gid_list.get(gid, "unknown")
        row["group"] = gname
      print(json.dumps(group_data, indent=4,sort_keys=True))
    elif args.user:
      user_data = api.getUserQuota()
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
      print(json.dumps(user_data, indent=4,sort_keys=True))



if __name__ == "__main__":
    main()
