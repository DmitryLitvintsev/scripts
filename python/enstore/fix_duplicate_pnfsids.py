#!/bin/env python
"""
This script takes a file containing list of pnfsids, one pnfsid per line
and checks if:
  1) layer1 corresponds to non-deleted bfid in enstore
  2) marks all bfids deleted that are not equal to layer1 bfid
"""
from __future__ import print_function
import argparse
import errno
import multiprocessing
import os
import re
import socket
import stat
import subprocess
import sys
import time
import uuid
import traceback
import psycopg2
import psycopg2.extras
import datetime
import getpass
import yaml

try:
    import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse

printLock = multiprocessing.Lock()

def print_error(text):
    """
    Print text string to stderr prefixed with timestamp
    and ERROR keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    with printLock:
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
    with printLock:
        sys.stdout.write(time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time()))+" INFO : " + text + "\n")
        sys.stdout.flush()

def get_path(pnfsid):
    """
    Get path for pnfsid
    Uses mounts pnfs to do that
    """
    path = None
    with open("/pnfs/fnal.gov/usr/.(pathof)(%s)" % (pnfsid, ), "r") as fh:
        path = fh.readlines()[0].strip()
    return path

def get_pnfsid(path):
    """
    Get pnfsis for path
    """
    pnfsid = None
    dn = os.path.dirname(path)
    fn = os.path.basename(path)
    with open("%s/.(id)(%s)" % (dn, fn,), "r") as fh:
        pnfsid = fh.readlines()[0].strip()
    return pnfsid


def read_layer(path, layer):
    dirname = os.path.dirname(path)
    fname  = os.path.basename(path)
    layer_file = os.path.join(dirname,'.(use)(%s)(%s)' % (str(layer),fname))
    with open(layer_file,'r') as fd:
        return fd.readlines()


# create DB connection from URI
# e.g. "postgresql://enstore@enstore:8888/enstoredb"
def create_connection(uri):
    result = urlparse.urlparse(uri)
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port)
    return connection


class Worker(multiprocessing.Process):
    """
    Class that processed individual enstore volume
    """
    def __init__(self, queue, config):
        super(Worker, self).__init__()
        self.queue = queue
        self.config = config

    def run(self):
        try:
            # enstroe db
            enstore_db = create_connection(self.config.get("enstore_db"))
            for pnfsid in iter(self.queue.get, None):
                path = get_path(pnfsid)
                bfid = read_layer(path, 1)
                if bfid:
                    bfid = bfid[0]
                res = select(enstore_db,
                             "select bfid from file where pnfs_id = %s and deleted = 'n'",
                             (pnfsid,))
                bfids = [i[0] for i in res]
                if bfid not in bfids:
                    print_error("% has bfid in layer 1 which is marked deleted %s" % (pnfsid, bfid,))
                    continue
                other_bfids = [i for i in bfids if i != bfid]
                if not other_bfids:
                    continue
                print_message("%s %s %s" % (pnfsid, bfid, " ".join(other_bfids),))
                # mark other bfids deleted
                for i in other_bfids:
                    update(enstore_db,
                           "update file set deleted = 'y' where bfid = %s",
                           (i, ))
        except Exception as e:
            print_message("Exception %s" % (str(e)))
        finally:
            for i in (enstore_db,):
                if i:
                    try:
                        i.close()
                    except:
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
        res = cursor.execute(sql, pars)
        con.commit()
        return res
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass

def insert_returning(con, sql, pars):
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
        sql +=  "returning *"
        cursor = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql, pars)
        res = cursor.fetchone()
        con.commit()
        return res
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


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
        cursor = con.cursor()
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


def main():
    """
    main function
    """
    configuration = {"enstore_db" :
                     "postgresql://enstore@enstore00:8888/enstoredb"}

    parser = argparse.ArgumentParser()


    parser.add_argument(
        "--file",
        help="A file with list of pnfsids")


    args = parser.parse_args()

    if not args.file:
        parser.print_help(sys.stderr)
        sys.exit(1)

    pnfsids = None
    with open(args.file, "r") as f:
        pnfsids = [i.strip() for i in f.readlines()]

    if not pnfsids:
         print_error("**** No files found, quitting ***")
         sys.exit(1)

    print_message("**** Start processing %d  files ****" % (len(pnfsids), ))

    queue = multiprocessing.Queue(10000)
    workers = []
    cpu_count = multiprocessing.cpu_count()

    for i in range(cpu_count):
        worker = Worker(queue, configuration)
        workers.append(worker)
        worker.start()

    for pnfsid in pnfsids:
        queue.put(pnfsid)

    for i in range(cpu_count):
        queue.put(None)

    map(lambda x: x.join(), workers);


    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()
