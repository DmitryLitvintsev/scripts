#!/bin/env python
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

#migration copy -permanent -storage=GM2.gm2_daq_run5 -tmode=same+admin(604800) -sticky -smode=removable -select=random -target=pgroup GM2Pools

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
POOL_GROUP = "StagePools"


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
        return result[0].split()
    else:
        return []

def migrate(ssh, location, destination, pnfsid):
    """
    run rep ls on location
    """
    result = execute_admin_command(ssh,
                                   "\s " + location + " migration copy "
                                   + "-tmode=same+admin(1209600) "
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

def clear_file_cache_location(ssh, pool, pnfsid):
    """
    clear file cache location
    """
    result = execute_admin_command(ssh, "\sn clear file cache location  " + pnfsid + " " + pool)
    print_message("Cleared file cache location %s %s %s" % (pnfsid, pool, result, ))
    return True

def mark_precious(ssh, pnfsid):
    """
    marks pnfsid on all locations as precious
    """
    result = execute_admin_command(ssh, "\sl " + pnfsid + " rep set precious " + pnfsid)
    return True

def set_sticky(ssh, pnfsid):
    """
    marks pnfsid on all locations as precious
    """
    result = execute_admin_command(ssh, "\sl " + pnfsid + " rep set sticky -o=admin -l=1209600000 " + pnfsid + " on")
    return True

def set_sticky_on_location(ssh, pool, pnfsid):
    """
    marks pnfsid on all locations as precious
    """
    result = execute_admin_command(ssh, "\s " + pool + " rep set sticky -l=1209600000 " + pnfsid + " on")
    return True

def unpin(ssh, pnfsid):
    """
    unpin pnfsid on all locations
    """
    result = execute_admin_command(ssh, "\sl " + pnfsid + " rep set sticky " + pnfsid + " off")
    result = execute_admin_command(ssh, "\sl " + pnfsid + " rep set sticky -o=admin " + pnfsid + " off")
    return True


def reprm(ssh, pnfsid):
    """
    remove  pnfsid on all locations 
    """
    result = execute_admin_command(ssh, "\sl " + pnfsid + " rep rm -force " + pnfsid)
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
    print_message("Cleared file cache location %s %s %s" % (pnfsid, pool, result, ))
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
    dir_name = os.path.dirname(path)
    file_name = os.path.basename(path)
    id_file_name = os.path.join(dir_name,".(id)(%s)" % (file_name,))
    pnfsid = None
    with open(id_file_name, "r") as f:
        pnfsid = f.readlines()[0].strip()
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


class UnpinWorker(multiprocessing.Process):
    """
    This class is responsible for staging files into source dCache system
    """
    def __init__(self, queue, pool):
        super(UnpinWorker, self).__init__()
        self.queue = queue
        self.pool = str(pool)

    def run(self):
        """
        Main pre-staging loop

        :return: no value
        :rtype: none
        """

        # admin shell
        ssh = get_shell()
        total = 0
        expunged = 0
        for file_name in iter(self.queue.get, None):
            pnfsid = get_pnfsid(file_name)
            total += 1 
            locations = []
            try:
                locations = get_locations(ssh, pnfsid)
            except:
                pass
            if not locations:
                continue
            res = reprm(ssh, pnfsid)
            expunged += 1 
            # label is done here
        print_message("%s : Done,  %d expunged, %d total  " % (self.pool, expunged,  total))

            
        ssh.close()
        return


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
        "--file",
        help="file containing file list, a file per line")

    args = parser.parse_args()

    if not args.file:
        parser.print_help(sys.stderr)
        sys.exit(1)

    if not os.path.exists(PNFS_HOME):
        print_error("PNFS is not mounted. Quitting.")
        sys.exit(1)


    print_message("**** START ****")

    queue = multiprocessing.Queue(10000)
    unpin_workers = []

    kinitWorker = KinitWorker()
    kinitWorker.start()

    ssh = get_shell()
    cpu_count = 10
    ssh.close()

    for pool in range(cpu_count):
        worker = UnpinWorker(queue, pool)
        unpin_workers.append(worker)
        worker.start()

    if args.file:
        with open(args.file, "r") as f:
            for line in f:
                queue.put(line.strip())

    for i in range(cpu_count):
        queue.put(None)

    map(lambda x: x.join(), unpin_workers);

    kinitWorker.stop = True
    kinitWorker.terminate()

    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()
