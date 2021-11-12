#!/bin/env python

import argparse
import errno
import multiprocessing
import os
import random
import re
import socket
import stat
import subprocess
import sys
import time
import uuid

import gssapi
import paramiko
import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

printLock = multiprocessing.Lock()
kinitLock = multiprocessing.Lock()

UUID = str(uuid.uuid4())

"""
The SQL below creates table in Enstore database 
to keep track of migration. This script does not 
run this query. IT has to be run in advance. Only once
(cut&past, execute from sql prompt) 

-- DROP TABLE IF EXISTS file_migrate;
CREATE TABLE file_migrate (
       src_bfid varchar(19),
       dst_bfid varchar(19),
       pnfsid varchar(36)
);

ALTER TABLE ONLY file_migrate
      ADD CONSTRAINT pk_file_migrate PRIMARY KEY (src_bfid);

ALTER TABLE ONLY file_migrate
    ADD CONSTRAINT fk_file_migrate FOREIGN KEY (src_bfid)
    REFERENCES file(bfid)
    ON UPDATE CASCADE ON DELETE CASCADE;

grant select on  file_migrate to enstore_reader;

"""

HOSTNAME = socket.gethostname()
SSH_HOST = "localhost"
SSH_PORT = 22224
SSH_USER = "admin"
POOL_GROUP = "readonlyPools"
ENSTORE_DB_HOST = "localhost"
CHIMERA_DB_HOST = "localhost"

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


def kinit():
    """
    Create kerberos ticket for admin shell access
    """
    KRB5CCNAME = "/tmp/krb5cc_root.migration-%s"%(UUID,)
    os.environ["KRB5CCNAME"] =  KRB5CCNAME

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
    return stdout.readlines()


def is_cached(ssh, pnfsid):
    """
    Check if file is cached
    """
    result = execute_admin_command(ssh, "\sn cacheinfoof " + pnfsid)
    payload = result[0].strip()
    if payload == "":
        return False
    else:
        return True


def mark_precious(ssh, pnfsid):
    """
    marks pnfsid on all locations as precious
    """
    result = execute_admin_command(ssh, "\sl " + pnfsid  + " rep set precious " + pnfsid)
    #print_message("Marked precious %s" % ( result, ))
    return True


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


class KinitWorker(multiprocessing.Process):
    def __init__(self):
        super(KinitWorker, self).__init__()
        self.stop = False

    def run(self):
        while not self.stop:
            with kinitLock:
                kinit()
                time.sleep(14400)


class StageWorker(multiprocessing.Process):
    """
    This class is responsible for staging files into source dCache system
    """
    def __init__(self, stage_queue, pool):
        super(StageWorker, self).__init__()
        self.stage_queue = stage_queue
        self.pool = pool

    def run(self):
        """
        Main pre-staging loop

        :return: no value
        :rtype: none
        """

        # admin shell
        ssh = get_shell()
        # db connection pool to enstore db
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host=ENSTORE_DB_HOST,
                        port=8888,
                        user="enstore",
                        database="enstoredb")

        # db connection pool to chimera db
        chimera_pool = PooledDB(psycopg2,
                                maxconnections=1,
                                maxcached=1,
                                blocking=True,
                                host=CHIMERA_DB_HOST,
                                user="enstore",
                                database="chimera")
        while True:
            label = self.stage_queue.get()
            if label is None:
                print_message("%s: Exiting" % self.name)
                self.stage_queue.task_done()
                break
            files = []
            connection = cursor = None
            try:
                connection = pool.connection()
                cursor = connection.cursor()
                try:
                    #
                    # Mark volume "migrating"
                    #
                    cursor.execute("update volume set system_inhibit_1 = 'migrating' where label = %s",
                                   (label,))
                    connection.commit()
                except Exception as e:
                    print_error("%s failed to update system_inhibit_1: %s" % (label, str(e)))
                    connection.rollback()
                    self.stage_queue.task_done()
                    continue

                try:
                    #
                    # get list of eligible files 
                    # 
                    cursor.execute("select f.bfid, f.pnfs_id, f.crc, f.size "
                                   "from file f inner join volume v on v.id = f.volume "
                                   "left outer join file_migrate fm on f.bfid = fm.src_bfid where v.label = %s "
                                   "and f.deleted = 'n' and fm.src_bfid is null order by f.location_cookie asc", (label, ))
                    res = cursor.fetchall()
                    if not res:
                        print_error("All files migrated for label %s" % (label, ))
                        self.stage_queue.task_done()
                        continue
                    files = []
                    for i in res:
                        try:
                            #
                            # extract file path by pnfsid
                            #
                            p = get_path(i[1])
                            files.append((i[0], i[1], i[2], i[3]))
                        except (OSError, IOError) as e:
                            if e.errno == errno.ENOENT:
                                try:
                                    print_error("%s %s %s Does not exist, mark deleted "%(label, i[0], i[1]))
                                    cursor.execute("update file set deleted = 'y' where bfid = %s", (i[0],))
                                    connection.commit()
                                except Exception as e:
                                    print_error("%s %s Failed to set file deleted: %s" % (label, i[0], str(e)))
                                    connection.rollback()
                                    continue
                            else:
                                raise

                except Exception as e:
                    print_error("Failed to retrieve files for label %s %s" % (label, str(e)))
                    self.stage_queue.task_done()
                    continue
            except Exception as e:
                print_error("Failed to get connection to enstoredb %s" % str(e))
                self.stage_queue.task_done()
                continue
            finally:
                for i in (cursor, connection):
                    if i:
                        try:
                            i.close()
                        except Exception:
                            pass

            number_of_files = len(files)
            total = number_of_files
            print("Doing label %s, number of files %d" % (label, number_of_files))
            cached = loop = count = 0
            while files:
                count += 1 
                bfid, pnfsid, crc, fsize = files.pop(0)
                cached_check = is_cached(ssh, pnfsid)
                if not cached_check:
                    files.append((bfid, pnfsid, crc, fsize))
                    stage(ssh, self.pool, pnfsid)
                else:
                    # file is online
                    cached += 1
                    #print_message("%s File is online, calling mark_precious %s %s" % (label, bfid, pnfsid))
                    rc = mark_precious(ssh, pnfsid)
                    if rc:
                        rc = bust_layers(chimera_pool, (label, bfid, pnfsid, crc, self.pool))
                        rc = mark_migrated(pool, (label, bfid, pnfsid, crc, self.pool))
                if count == number_of_files and files:
                    loop += 1
                    number_of_files = len(files)
                    print_message("%s, %s : %d staged, %d total, %d remain,  %d pass" %
                                  (self.pool, label, cached, total,  number_of_files, loop))
                    count = 0 
                    print_message("%s, %s Sleeping" % (self.pool, label, ))
                    time.sleep(1200)

            # label is done here
            print_message("%s, %s : Done" % (self.pool, label, ))
            self.stage_queue.task_done()
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


def bust_layers(pool, entry):
    """"
    Delete layers
    """
    connection = None
    label, bfid, pnfsid, crc, dcache_pool = entry
    try:
        connection = pool.connection()
        try:
            res = select(connection, "select pnfsid2inumber(%s)", (pnfsid, ))
            inumber = res[0][0]
            res = insert(connection, "delete from t_level_1 where inumber=%s", (inumber, ))
            res = insert(connection, "delete from t_level_4 where inumber=%s", (inumber, ))
            res = insert(connection, "delete from t_storageinfo where inumber=%s", (inumber, ))
            res = insert(connection, "delete from t_locationinfo where inumber=%s and itype=0", (inumber, ))
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
        help="file containing label list, a label per line")

    parser.add_argument(
        "--label",
        help="comma separated list of labels")

    args = parser.parse_args()

    if not args.file and not args.label:
        parser.print_help(sys.stderr)
        sys.exit(1)

    if args.file:
        with open(args.file, "r") as f:
            labels = [i.strip() for i in f]

    if args.label:
        labels = args.label.strip().split(",")


    labels = []

    if args.file:
        with open(args.file, "r") as f:
            labels = [i.strip() for i in f]

    if args.label:
        labels = args.label.strip().split(",")

    if not labels:
        print_error("No labels found")
        parser.print_help(sys.stderr)
        sys.exit(1)

    print_message("**** START ****")

    print_message("Found %d labels" % (len(labels)))

    stage_queue = multiprocessing.JoinableQueue()
    stage_workers = []

    kinitWorker = KinitWorker()
    kinitWorker.start()

    ssh = get_shell()
    pools = get_active_pools_in_pool_group(ssh, POOL_GROUP)
    cpu_count = len(pools)
    ssh.close()

    for pool in pools:
        worker = StageWorker(stage_queue, pool)
        stage_workers.append(worker)
        worker.start()

    for label in labels:
        stage_queue.put(label)

    for i in range(cpu_count):
        stage_queue.put(None)

    stage_queue.join()

    kinitWorker.stop = True 
    kinitWorker.terminate()

    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()

