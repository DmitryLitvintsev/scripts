#!/usr/bin/env python

import csv
import json
import pandas as pd
import re
import sys
from prettytable import prettytable

GID_FILE = "/etc/grid-security/group"
UID_FILE = "/etc/grid-security/passwd"
GB=1<<30

gid_list={}
uid_list={}

with open(GID_FILE, "r") as f:
  for line in f:
    if not line : continue
    parts = line.strip().split(":")
    gid = parts[2].strip()
    gname = parts[0].strip()
    gid_list[gid] = gname

with open(UID_FILE, "r") as f:
  for line in f:
    if not line : continue
    parts = line.strip().split(":")
    uid = parts[2].strip()
    gid = parts[3].strip()
    username = parts[0].strip()
    name     =  re.sub("\"","'",parts[4].strip())
    uid_list[uid] = (uid, gid, username, name)


def main():
    try:
        filename = sys.argv[1]
    except Exception as e:
        pass

    field_names = ("count", "volume", "uid", "gid")
    json_data = []
    with open(filename, "r") as f:
        reader = csv.DictReader(f, field_names)
        for row in reader:
            json_data.append(row)

    for row in json_data:
      uid = row["uid"]
      gid = row["gid"]
      user_data = uid_list.get(uid)
      uname = "unknown"
      full_name = "unknown"

      if user_data:
        uname = user_data[2]
        full_name = user_data[3]
        
    
      gname = gid_list.get(gid, "unknown")
      row["name"] = full_name
      row["username"] = uname
      row["group"] = gname
      row["volume"] = float(row["volume"])/GB

    
    df = pd.DataFrame(json_data, columns=json_data[0].keys())
    df_sorted = df.sort_values(by="volume", ascending=False)
    print(df_sorted.to_string(float_format='{:.2f}'.format))



if __name__ == "__main__":
    main()
