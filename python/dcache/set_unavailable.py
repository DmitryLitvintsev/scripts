#!/bin/env python
"""

Script that sets files on a volume unavailable/available to dCache

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


CONFIG_FILE = os.getenv("MIGRATION_CONFIG")
if not CONFIG_FILE:
    CONFIG_FILE = "enstore2cta.yaml"

HOSTNAME = socket.getfqdn()

STOPPER="/tmp/STOP"


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


class Worker(multiprocessing.Process):
    """
    Class that processed individual enstore volume
    """
    def __init__(self, queue, state, config):
        super(Worker, self).__init__()
        self.queue = queue
        self.config = config
        self.state = state

    def run(self):
        chimera_db = None
        try:
            chimera_db = create_connection(self.config.get("chimera_db"))
            for pnfsid in iter(self.queue.get, None):
                res = update(chimera_db,
                             "update t_locationinfo set istate = %s "
                             "     where inumber = (select inumber from t_inodes where ipnfsid = %s) "
                             "        and itype = 0 "
                             "        and ilocation like 'cta%%'",
                             (self.state, pnfsid,))
        except Exception as e:
            print_message("Exception %s" % (str(e)))
            raise e
        finally:
            for i in (chimera_db,):
                if i:
                    try:
                        i.close()
                    except:
                        pass


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


def update(con, sql, pars=None):
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


def insert(con, sql, pars=None):
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
        if pars:
            res = cursor.execute(sql, pars)
        else:
            res = cursor.execute(sql)
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

def insert_returning(con, sql, pars=None):
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
        if pars:
            cursor.execute(sql, pars)
        else:
            cursor.execute(sql)
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


def parse_enstore_config(file_name):
    #
    # Parse enstore config
    #
    configdict = {}
    with open(file_name, "r") as f:
        lines = "".join(f.readlines())
        exec(lines)
    return configdict


def get_enstore_libraries(enstore_db):
    rows = select(enstore_db,
                  SELECT_LIBRARIES)
    libraries = [row["library"] for row in rows]
    return libraries

def main():

    if os.path.exists(STOPPER):
        print_error(f"Found {STOPPER} file. Quitting...")
        sys.exit(1)

    """
    main function
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="This script marks pnfsids available or not available on a label"
        )

    parser.add_argument(
        "--label",
        help="comma separated list of labels")

    parser.add_argument(
        "--state",
        action  = "store",
        type = int,
        default = 0,
        help="stat of file : 0 - fisabled,  1 - enabled")

    parser.add_argument(
        "--cpu_count",
        action  = "store",
        type = int,
        default =  multiprocessing.cpu_count(),
        help="override cpu count - number of simultaneously processed labels")

    args = parser.parse_args()

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

    print (configuration)

    if not args.label:
        parser.print_help(sys.stderr)
        sys.exit(1)

    cta_db  = None

    res = None
    try:
        cta_db = create_connection(configuration.get("cta_db"))
        res = select(cta_db,
                     "select af.disk_file_id as pnfsid "
                     "  from archive_file af "
                     "    inner join tape_file tf on "
                     "     af.archive_file_id = tf.archive_file_id "
                     "where tf.vid = %s",
                    (args.label,))
    except Exception as e:
        print_error(f"Failed to initialize connection to cta_db, quitting {e}")
        sys.exit(1)
    finally:
        if cta_db:
            cta_db.close()


    if not res:
         print_error(f"Failed to find files for label {args.label}")
         sys.exit(1)

    number_of_files = len(res)

    print_message(f"Found {number_of_files} files on label {args.label} setting to {args.state}")

    queue = multiprocessing.Queue(10000)
    workers = []
    cpu_count = args.cpu_count

    for i in range(cpu_count):
        worker = Worker(queue, args.state, configuration)
        workers.append(worker)
        worker.start()

    for row in res:
        queue.put(row["pnfsid"])

    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    print_message("**** FINISH ****")

if __name__ == "__main__":
    main()
