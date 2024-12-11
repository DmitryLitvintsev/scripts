#!/usr/bin/env python

import os
import sys
import time

"""
update srmlinkgroup set availablespaceinbytes=0 where id = 3;
"""

def print_error(text):
    sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                   time.localtime(time.time()))+
                     " : " +text+"\n")
    sys.stderr.flush()


def print_message(text):
    sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                   time.localtime(time.time()))+
                     " : " +text+"\n")
    sys.stdout.flush()

query = """
INSERT INTO
srmspacefile
(vogroup, vorole, spacereservationid, sizeinbytes, creationtime, pnfsid, state)
VALUES
"""

TOKEN = 5
VOGROUP = "/xenon.biggrid.nl"
ROLE="*"


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print_error("specify file name with comma separated values")
        sys.exit(1)

    with open("populate.sql", "w") as sql_file:
        sql_file.write(f"{query}")
        with open(sys.argv[1], "r") as f:
            for line in f:
                pnfsid, size, creation_time = line.strip().split(",")
                sql_file.write(f"('{VOGROUP}', '{ROLE}', {TOKEN}, {size}, {creation_time}, '{pnfsid}', 2),")

    with open("populate.sql", "rb+") as f:
        f.seek(-1, os.SEEK_END)
        f.truncate()
