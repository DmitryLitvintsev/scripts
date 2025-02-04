#!/bin/env python

"""
This script does recursive QOS REPLICA/ONLINE -> CUSTODIAL/NEARLINE
transition on a directory provided
"""

from __future__ import print_function
import argparse
import errno
import multiprocessing
import os
import re
import socket
import subprocess
import sys
import time
import uuid

import paramiko
import psycopg2
import psycopg2.extras


UPDATE_QUERY = """
               update t_inodes set iaccess_latency = 0, iretention_policy = 0
                      where inumber = %s and iretention_policy = 2 and iaccess_latency = 1
               """

try:
    from DBUtils.PooledDB import PooledDB
except ModuleNotFoundError:
    from dbutils.pooled_db import PooledDB

import pandas as pd
from tabulate import tabulate

PNFS_HOME = "/pnfs/fs/usr"

printLock = multiprocessing.Lock()
kinitLock = multiprocessing.Lock()

UUID = str(uuid.uuid4())

HOSTNAME = socket.gethostname()
SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"


def unpin(ssh, pnfsid):
    """
    marks pnfsid on all locations as precious
    """
    result = execute_admin_command(ssh, "\sl " + pnfsid + " rep set sticky " + pnfsid + " off")
    #result = execute_admin_command(ssh, "\sl " + pnfsid + " rep set sticky -o=admin " + pnfsid + " off")
    return True



def execute_command(cmd):
    """
    Executes shell command

    :param cmd: command string
    :type cmd: str
    :return: shell command return code
    :rtype: int
    """
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    output, errors = p.communicate()
    rc = p.returncode
    return rc


KRB5CCNAME = "/tmp/krb5cc_root.migration-%s"%(UUID,)
os.environ["KRB5CCNAME"] = KRB5CCNAME


def kinit():
    """
    Create kerberos ticket for admin shell access
    """

    cmd = "/usr/bin/kinit -k host/%s"%(HOSTNAME)
    execute_command(cmd)


def get_shell():
    """
    Admin shell
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=SSH_HOST,
                port=SSH_PORT,
                username=SSH_USER,
                gss_auth=True,
                gss_kex=True)
    return ssh


def execute_admin_command(ssh, cmd):
    """
    Execute admin shell command
    """
    stdin, stdout, stderr = ssh.exec_command(cmd)
    if len(stderr.readlines()) > 0:
        raise RuntimeError(" ".join(stderr.readlines()))
    return [i.strip().replace(r"\r","\n") for i in stdout.readlines() if i.strip().replace(r"\r", "\n") != ""]


def is_cached(ssh, pnfsid):
    """
    Check if file is cached
    """
    result = execute_admin_command(ssh, "\sn cacheinfoof " + pnfsid)
    if not result:
        return False
    else:
        return True


def get_locations(ssh, pnfsid):
    result = execute_admin_command(ssh, "\sn cacheinfoof " + pnfsid)
    if result:
        return result[0].split()
    else:
        return []


def rep_ls(ssh, location, pnfsid):
    """
    run rep ls on location
    """
    result = execute_admin_command(ssh, "\s " + location + " rep ls " + pnfsid)
    if result:
        if result[0].find("Entry not in repository") != -1:
            return []
        return result[0].split()
    else:
        return []

def migrate(ssh, location, destination, pnfsid):
    """
    run rep ls on location
    """
    result = execute_admin_command(ssh,
                                   "\s " + location + " migration copy "
                                   + "-tmode=same+admin(604800) "
                                   + "-exclude=rw-stkendca2044*,rw-gm2-stkendca2044* "
                                   + "-select=random "
                                   + "-pnfsid=" + pnfsid
                                   + " -target=pgroup " + destination)



    if result:
        return result[0].split()
    else:
        return []

def pin(ssh, pin, duration):
    """
    run rep ls on location
    """
    result = execute_admin_command(ssh,
                                   "\s PinManager pin" + pnfsid + " " + str(duration))
    if result:
        return result[0].split()
    else:
        return []


def mark_precious(ssh, pnfsid):
    """
    marks pnfsid on all locations as precious
    """
    result = execute_admin_command(ssh, "\sl " + pnfsid + " rep set precious " + pnfsid)
    return True

def mark_precious_on_location(ssh, pool, pnfsid):
    """
    marks pnfsid on a pool as precious
    """
    result = execute_admin_command(ssh, "\s " + pool + " rep set precious " + pnfsid)
    print_message("Marked precious %s %s %s" % (pnfsid, pool, result, ))
    return True


def clear_file_cache_location(ssh, pool, pnfsid):
    """
    clear file cache location
    """
    result = execute_admin_command(ssh, "\sn clear file cache location  " + pnfsid + " " + pool)
    return True

def get_precious_fraction(ssh, pool):
    """
    return fraction of precious data on pool
    """
    result = execute_admin_command(ssh, "\s " + pool + " info -a")
    lines = [i.strip() for i in result]
    percentage = 0
    for line in lines:
        if line.find("Precious") != -1:
            percentage = float(re.sub("[\[-\]]","", line.split()[-1]))
            break
    return percentage


def get_active_pools_in_pool_group(ssh, pgroup):
    """
    Get list of pools in a  pool group
    """
    result = execute_admin_command(ssh, "\s PoolManager  psu ls pgroup -a " + pgroup)
    pools = []
    hasPoolList = False
    for line in result:
        i = line.strip()
        if not i:
            continue
        if i.strip().startswith("nested groups "):
            break
        if i.strip().startswith("poolList :"):
            hasPoolList = True
            continue
        if hasPoolList:
            parts = i.split()
            if parts[1].find("mode=disabled") != -1:
                continue
            pool = parts[0].strip()
            pools.append(pool)
    return pools


def stage(ssh, pool, pnfsid):
    """
    Schedule restore of file by pnfsid on a pool
    """
    cmd = "\s " + pool + " rh restore " + pnfsid
    result = execute_admin_command(ssh, cmd)


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

class KinitWorker(multiprocessing.Process):
    def __init__(self):
        super(KinitWorker, self).__init__()
        self.stop = False

    def run(self):
        while not self.stop:
            with kinitLock:
                kinit()
                time.sleep(14400)


class QosWorker(multiprocessing.Process):
    """
    This class is responsible for setting AL/RP = NEARLINE/CUSTODIAL
    setting files precious on location.
    """
    def __init__(self, queue):
        super(QosWorker, self).__init__()
        self.queue = queue

    def run(self):
        # admin shell
        ssh = get_shell()
        # db connection pool to enstore db
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host="localhost",
                        user="enstore",
                        database="chimera")

        connection = pool.connection()
        cursor = connection.cursor()

        for data in iter(self.queue.get, None):
            inumber, pnfsid, access_latency, retention_policy, file_name = data
            locations = get_locations(ssh, pnfsid)
            if not locations:
                print_message("NO LOCATIONS for pnfsid %s, Skipping" % (pnfsid,))
                continue

#            try:
#                rc = mark_precious_on_location(ssh, locations[0], pnfsid)
#                print_message("Marked %s precious on %s" % (pnfsid, locations[0],))
#                if access_latency == 1 and retention_policy == 2:
#                    try:
#                        cursor.execute(UPDATE_QUERY, (inumber, ))
#                        connection.commit()
#                    except Exception, e:
#                        connection.rollback()
#                        print_error("Failed to update %d %s %s %s"%(inumber, pnfsid, str(e), cursor.query),)
#                        continue
#            except Exception, e:
#                print_error("Failed to mark %s precious on location %s" % (pnfsid, locations[0], ))
#                continue

            print_message("Done %s on %s " % (pnfsid, locations[0],))

            res = unpin(ssh, pnfsid)


        ssh.close()
        for i in (cursor, connection):
            if i:
                try:
                    i.close()
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


def select(con, sql, pars):
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
        cursor.execute(sql, pars)
        return cursor.fetchall()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def get_label_system_inhibit(pool, label):
    """
    get label status
    """
    connection = None
    try:
        connection = pool.connection()
        res = select(connection, " select system_inhibit_0 from volume where label = %s", (label, ))
        return res[0][0]
    except Exception as e:
            print_error("%s Failed to query system inhibit for label %s " % (label, ))
            pass
    except Exception as e:
        print_error("%s Failed to get connection when querying system inhibit for label %s" % (label, ))
        pass
    finally:
        try:
            if connection:
                connection.close()
        except Exception:
            pass
    return None


def mark_migrated(pool, entry):
    """
    Mark source file migrated

    :param pool: database connection pool
    :type pool: PooledDB

    :param entry: (label, bfid, file, src) source data
    :type entry: tuple

    :param destination: destination full file path name
    :type destination: str

    :param check: Mark checked or nor
    :type check: bool

    :return: true / false
    :rtype: bool
    """
    connection = None
    label, bfid, pnfsid, crc, dcache_pool = entry
    try:
        connection = pool.connection()
        try:
            res = insert(connection, "insert into file_migrate (src_bfid, pnfsid) "
                                     "values (%s, %s)", (bfid, pnfsid,))

            res = update(connection, "update file set deleted = 'y' where bfid = %s", (bfid, ))
            return True
        except Exception as e:
            print_error("%s Failed to insert into file_migrate %s %s %s " % (label, bfid, pnfsid, str(e)))
            pass
    except Exception as e:
        print_error("%s Failed to get connection when inserting into file_migrate %s" % (label, str(e),))
        pass
    finally:
        try:
            if connection:
                connection.close()
        except Exception:
            pass
    return False


def main():
    """
    main function
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dir",
        help="top directory name")

    args = parser.parse_args()

    if not args.dir:
        parser.print_help(sys.stderr)
        sys.exit(1)

    if not os.path.exists(PNFS_HOME):
        print_error("PNFS is not mounted. Quitting.")
        sys.exit(1)

    if not os.path.exists(args.dir):
        print_error("Directry %s does not exist. Quitting.")
        sys.exit(1)

    pnfsid = get_pnfsid(args.dir)


    queue = multiprocessing.Queue(1000)
    workers = []

    kinitWorker = KinitWorker()
    kinitWorker.start()

    ssh = get_shell()
    ssh.close()

    cpu_count = multiprocessing.cpu_count()

    for i in range(cpu_count):
        worker = QosWorker(queue)
        workers.append(worker)
        worker.start()

    pool = PooledDB(psycopg2,
                    maxconnections=1,
                    maxcached=1,
                    blocking=True,
                    host="localhost",
                    user="enstore",
                    database="chimera")

    db = pool.connection()
    cursor = db.cursor('cursor_qos', cursor_factory=psycopg2.extras.DictCursor)


    QUERY = """
    WITH RECURSIVE paths(ino, path, pnfsid, access_latency, retention_policy, type) AS ( VALUES
    (pnfsid2inumber('%s'),'', '', 0::BIGINT, 0::BIGINT, 16384)
    UNION SELECT i.inumber, path||'/'||d.iname,i.ipnfsid, i.iaccess_latency,i.iretention_policy, i.itype FROM
    t_dirs d,t_inodes i, paths p WHERE p.type=16384 AND
    d.iparent=p.ino AND d.iname != '.' AND
    d.iname != '..' AND i.inumber=d.ichild )
    SELECT p.ino, p.pnfsid, p.access_latency, p.retention_policy, p.path FROM
    paths p WHERE p.type=32768 and p.pnfsid != '0000B9A5FFDE7FC148428BC5AE4EA0A9A7BA'
    """

    cursor.execute(QUERY % (pnfsid, ))

    total = 0
    t0 = time.time()

    while True:
        res = cursor.fetchmany(1000)
        if not res:
            break
        total += len(res)
        for r in res:
            queue.put(r)
        print_message("Changing %d,  queue size %d "%(total, queue.qsize()))
    cursor.close()
    db.close()


    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    kinitWorker.stop = True
    kinitWorker.terminate()

    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()
