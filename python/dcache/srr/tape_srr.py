#!/bin/env python

"""
Chown tree ownershop starting from pnfsid of root directory

"""

from __future__ import print_function
import json
import errno
import os
import sys
import tempfile
import time
import shutil
import yaml

import psycopg2
import psycopg2.extras

try:
    import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse


CONFIG_FILE = os.getenv("TAPE_SRR_CONFIG")
if not CONFIG_FILE:
    CONFIG_FILE = "tape_srr.yaml"

FILE_NAME="/var/www/www_pages/FNAL-WLCG-tape-statisics.json"


def print_error(text):
    """
    Print text string to stderr prefixed with timestamp
    and ERROR keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    sys.stderr.write(time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(time.time()))+" ERROR : " + text + "\n")
    sys.stderr.flush()


def print_message(text):
    """
    Print text string to stdout prefixed with timestamp
    and INFO keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    sys.stdout.write(time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(time.time()))+" INFO : " + text + "\n")
    sys.stdout.flush()

# create DB connection from URI
def create_connection(uri):
    result = urlparse.urlparse(uri)
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port)
    return connection


def select(con, sql, pars=None):
    """
    Select  database records

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    cursor = None
    try:
        cursor = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if pars:
            cursor.execute(sql, pars)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def update(con, sql, pars):
    """
    Update database record

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    return insert(con, sql, pars)


def insert(con, sql, pars):
    """
    Insert database record

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    cursor = None
    try:
        cursor = con.cursor()
        cursor.execute(sql, pars)
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass



def main():
    configuration = None
    try:
        mode = os.stat(CONFIG_FILE).st_mode
        if mode != 33152:
            print_error("Access to config file file %s is too permissive, do chmod 0600" %
                        (CONFIG_FILE,))
            sys.exit(1)
        with open(CONFIG_FILE, "r") as f:
            configuration = yaml.safe_load(f)
    except (OSError, IOError) as e:
        if e.errno == errno.ENOENT:
            print_error("Config file %s does not exist" % (CONFIG_FILE,))
        sys.exit(1)

    if not configuration:
        print_error("Failed to load configuration %s" % (CONFIG_FILE,))
        sys.exit(1)

    data = {
        "storageservice": {
        "name": "FNAL-CTA",
        "implementation": "CTA",
        "implementationversion": "5.11.10.1-1",
        "latestupdate": int(time.time()),
        "storageshares": [ ]
        }
        }


    with (create_connection(configuration.get("cta_db")) as cta_db):
        try:
            res = select(cta_db,
                         ("select vo.virtual_organization_name as vo_name, "
                          "count(*), "
                          "sum(t.master_data_in_bytes) as active, "
                          "sum(t.data_in_bytes) as occupied "
                          "from virtual_organization vo "
                          "inner join tape_pool tp on "
                          "tp.virtual_organization_id = vo.virtual_organization_id "
                          "inner join tape t on t.tape_pool_id = tp.tape_pool_id "
                          "where t.data_in_bytes > 0 "
                          "group by vo_name order by active desc"))
            for row in res:
                data["storageservice"]["storageshares"].append({
                    "name": row.get("vo_name").upper(),
                    "usedsize" : int(row.get("active", 0)),
                    "occupiedsize" : int(row.get("occupied", 0)),
                    "avgtaperemounts" : 0,
                    "readbytes24h" : 0,
                    "writebytes24h" : 0,
                    "timestamp": int(time.time()),
                    "vos" : [row.get("vo_name"), ]
                    })
            fd, name = tempfile.mkstemp(text=True)
            os.write(fd, str.encode(json.dumps(data, indent=4, sort_keys=True)))
            os.close(fd)
            shutil.move(name, FILE_NAME)
            os.chmod(FILE_NAME, 0o644)

        except Exception as e:
            print_error(f"query failed {e}")


if __name__ == "__main__":
    main()
