#!/usr/bin/env python

import argparse
import json
import requests
from requests.exceptions import HTTPError
import os
import sys
import time

import urllib3
urllib3.disable_warnings()

TOKEN_FILE = f"/run/user/{os.getuid()}/bt_u{os.getuid()}"

TOKEN = None
with open(TOKEN_FILE, "r") as f:
     TOKEN="".join(f.readlines()).strip("\n")

class TapeApi:

    def __init__(self, url):
        self.session = requests.Session()
        self.session.verify = "/etc/grid-security/certificates"

        self.headers = { "accept" : "application/json",
                         "content-type" : "application/json",
                         "Authorization" : f"Bearer {TOKEN}" }
        self.url = url


    def archiveinfo(self, files):
        #
        # check archiveinfo
        #
        data =  {
            "paths" : files,
        }

        payload = None
        try:
            r = self.session.post(self.url+"/archiveinfo",
                                  data=json.dumps(data),
                                  headers=self.headers)
            r.raise_for_status()
            payload =  r.json()
        except HTTPError as exc:
            print(exc)

        if payload:
            print(json.dumps(payload, indent=4,sort_keys=True))


    def stage(self, files):
        #
        # stage
        #
        paths = {"files" : [] }
        for f in files:
            paths["files"].append({"diskLifetime" : "P1D",
                                   "path" : f})
        r = self.session.post(self.url+"/stage",
                              data=json.dumps(paths),
                              headers=self.headers)
        r.raise_for_status()

        request_id = r.json().get("requestId")
        return request_id


    def status(self, request_id):
        #
        # stage
        #
        r = self.session.get(self.url+"/stage/"+request_id,
                              headers=self.headers)

        r.raise_for_status()
        print(json.dumps(r.json(), indent=4,sort_keys=True))


    def release(self, request_id, files):
        #
        # release request
        #
        data = {
            "paths" : files,
        }
        r = self.session.post(base_url+"/release/"+request_id,
                              data=json.dumps(data),
                              headers=self.headers)
        r.raise_for_status()
        print(r)
        #print(json.dumps(r.json(), indent=4,sort_keys=True))

def main():
    """
    main function
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Example script to use dCache Tape REST API"
        )

    parser.add_argument(
        "-s", "--stage",
        nargs='+',
        help='stage files',
        required=False,
        metavar="FILE")

    parser.add_argument(
        "-d", "--door",
        help='name of dCache door host (e.g. fndcadoor)',
        required=False,
        default="fndcadoor",
        metavar="DOOR")

    parser.add_argument(
        "-a", "--archiveinfo",
        nargs='+',
        help='check archiveinfo of files',
        required=False,
        metavar="FILE")

    parser.add_argument(
        "-q","--status",
        help='Query request status',
        required=False,
        metavar="UUID")

    parser.add_argument(
        "-r","--release",
        help='Release status',
        nargs='+',
        required=False,
        metavar="FILE")

    parser.add_argument(
        "-u","--uid",
        help='Release request uid',
        required=False,
        metavar="UID")

    args = parser.parse_args()

    door = args.door
    if not args.door.endswith(".fnal.gov"):
         door = args.door + ".fnal.gov"

    base_url = f"https://{door}:3880/api/v1/tape"

    api = TapeApi(base_url)

    if args.stage:
        request_id = api.stage(args.stage)
        print(f"Request id {request_id}")
    elif args.archiveinfo:
        api.archiveinfo(args.archiveinfo)
    elif args.status:
        api.status(args.status)
    elif args.release:
        if args.uid:
            api.release(args.uid, args.release)
        else:
            print("Specify request_id")
    else:
         parser.print_help(sys.stderr)

if __name__ == "__main__":
    main()
