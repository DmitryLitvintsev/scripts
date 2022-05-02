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
SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"
POOL_GROUP = "CdfWritePools"

UPDATER_SQL = """
UPDATE file_migrate
SET dst_bfid = t.bfid
FROM
  (SELECT fm.src_bfid AS src_bfid,
          f.bfid AS bfid
   FROM file_migrate fm
   INNER JOIN FILE f ON fm.pnfsid = f.pnfs_id
   INNER JOIN volume v ON f.volume = v.id
   AND f.deleted = 'n'
   AND v.media_type = 'LTO8'
   AND fm.dst_bfid IS NULL) AS t
WHERE t.src_bfid = file_migrate.src_bfid
  AND file_migrate.dst_bfid IS NULL
"""

PROGRESS_SQL="""
SELECT to_char(sum(CASE
                       WHEN dst_bfid IS NOT NULL THEN f.size
                       ELSE 0
                   END)/1024./1024./1024./1024., '99999D9') AS migrated,
       to_char(sum(CASE
                       WHEN dst_bfid IS NOT NULL THEN 0
                       ELSE f.size
                   END)/1024./1024./1024./1024., '999D9') AS precious,
       v.storage_group
FROM FILE f
INNER JOIN volume v ON v.id = f.volume
INNER JOIN file_migrate fm ON f.bfid = fm.src_bfid
GROUP BY v.storage_group
"""

PROGRESS_FOR_SG_SQL="""
SELECT to_char(sum(CASE
                       WHEN dst_bfid IS NOT NULL THEN f.size
                       ELSE 0
                   END)/1024./1024./1024./1024., '99999D9') AS migrated,
       to_char(sum(CASE
                       WHEN dst_bfid IS NOT NULL THEN 0
                       ELSE f.size
                   END)/1024./1024./1024./1024., '999D9') AS precious
FROM FILE f
INNER JOIN volume v ON v.id = f.volume
INNER JOIN file_migrate fm ON f.bfid = fm.src_bfid
WHERE v.storage_group = '%s'
"""


def print_progress(sg=None):
    connection = None
    try:
        connection = psycopg2.connect(dbname="enstoredb",
                                      host="enstore00",
                                      port=8888,
                                      user="enstore")
        if sg:
            data = pd.read_sql_query(PROGRESS_FOR_SG_SQL % (sg, ),
                                     connection)
        else:
            data = pd.read_sql_query(PROGRESS_SQL,
                                     connection)
        print(tabulate(data,
                       headers='keys',
                       tablefmt='psql'))
    finally:
        if connection:
            try:
                connection.close()
            except:
                pass


def updater():
    cursor = connection = None
    try:
        connection = psycopg2.connect(dbname="enstoredb",
                                      host="enstore00",
                                      port=8888,
                                      user="enstore")
        cursor = connection.cursor()
        cursor.execute(UPDATER_SQL)
        connection.commit()
        return 0
    except:
        return 1
    finally:
        for i in (cursor, connection):
            try:
                i.close()
            except:
                pass


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
                        host="enstore00",
                        port=8888,
                        user="enstore",
                        database="enstoredb")

        # db connection pool to chimera db
        chimera_pool = PooledDB(psycopg2,
                                maxconnections=1,
                                maxcached=1,
                                blocking=True,
                                host="enstore01",
                                user="enstore",
                                database="chimera")
        while True:
            # before next label
            precious_fraction = get_precious_fraction(ssh, self.pool)
            while precious_fraction > 0.1:
                print_message("%s pool has %d percent precious, sleeping" % (self.pool, int(precious_fraction * 100),))
                time.sleep(600)
                precious_fraction = get_precious_fraction(ssh, self.pool)

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
                    cursor.execute("update volume set system_inhibit_1 = 'migrating' where label = %s",
                                   (label,))
                    connection.commit()
                except Exception as e:
                    print_error("%s failed to update system_inhibit_1: %s" % (label, str(e)))
                    connection.rollback()
                    self.stage_queue.task_done()
                    continue

                try:
                    cursor.execute("select f.bfid, f.pnfs_id, f.crc, f.size "
                                   "from file f inner join volume v on v.id = f.volume "
                                   "left outer join file_migrate fm on f.bfid = fm.src_bfid where v.label = %s "
                                   "and f.deleted = 'n' and f.package_id is null and "
                                   "fm.src_bfid is null order by f.location_cookie asc", (label, ))
                    res = cursor.fetchall()
                    if not res:
                        print_error("All files migrated for label %s" % (label, ))
                        self.stage_queue.task_done()
                        continue
                    files = []
                    pnfs_mounted = True
                    for i in res:
                        try:
                            p = get_path(i[1])
                            files.append((i[0], i[1], i[2], i[3]))
                        except (OSError, IOError) as e:
                            if e.errno == errno.ENOENT:
                                try:
                                    if os.path.exists(PNFS_HOME):
                                        print_error("%s %s %s Does not exist, mark deleted "%(label, i[0], i[1]))
                                        cursor.execute("update file set deleted = 'y' where bfid = %s", (i[0],))
                                        connection.commit()
                                    else: 
                                        pnfs_mounted = False
                                        break
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
            if not pnfs_mounted: 
                print_error("%s %s %s: PNFS is not mounted, mount pnfs. Quitting" % (self.pool, label,))
                self.stage_queue.task_done()
                break

            number_of_files = len(files)
            total = number_of_files
            print("Doing label %s, number of files %d" % (label, number_of_files))
            cached = loop = count = 0
            pools = get_active_pools_in_pool_group(ssh, POOL_GROUP)
            while files:
                count += 1 
                bfid, pnfsid, crc, fsize = files.pop(0)
                locations = get_locations(ssh, pnfsid)
                location = ""
                for i in locations:
                    if i in pools:
                        location = i
                if not location: 
                    files.append((bfid, pnfsid, crc, fsize))
                    stage(ssh, self.pool, pnfsid)
                else:
                    # file is online
                    cached += 1
                    #print_message("%s File is online, calling mark_precious %s %s" % (label, bfid, pnfsid))
                    #rc = mark_precious(ssh, pnfsid)
                    rc = mark_precious_on_location(ssh, location, pnfsid)
                    rc = bust_layers(chimera_pool, (label, bfid, pnfsid, crc, self.pool))
                    rc = mark_migrated(pool, (label, bfid, pnfsid, crc, self.pool))
                if count == number_of_files and files:
                    loop += 1
                    number_of_files = len(files)
                    print_message("%s, %s : %d staged, %d total, %d remain,  %d pass" %
                                  (self.pool, label, cached, total,  number_of_files, loop))
                    count = 0 
                    #
                    # Check that label is still OK 
                    #
                    inhibit = get_label_system_inhibit(pool, label)
                    if inhibit in ('NOACCESS', 'NOTALLOWED',):
                        print_error("%s, %s : %s, Skipping " % (self.pool, label, inhibit, ))
                        break
                    print_message("%s, %s Sleeping" % (self.pool, label, ))
                    pools = get_active_pools_in_pool_group(ssh, POOL_GROUP)
                    time.sleep(600)

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
        print_error("%s Failed to get connection when querying system inhibit for lanel %s" % (label, ))
        pass
    finally:
        try:
            if connection:
                connection.close()
        except Exception:
            pass
    return None


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

    parser.add_argument(
        "--progress", action='store_true',
        help="print overall migration progress by storage group")

    parser.add_argument(
        "--sg", 
        help="storage group")

    parser.add_argument(
        "--update", action='store_true',
        help="run updater")

    args = parser.parse_args()
    
    if args.progress:
        if args.sg:
            print_progress(args.sg)
        else:
            print_progress()
        sys.exit(0)

    if args.update:
        sys.exit(updater())

    if not args.file and not args.label:
        parser.print_help(sys.stderr)
        sys.exit(1)
        
    if args.file:
        with open(args.file, "r") as f:
            labels = [i.strip() for i in f]

    if args.label:
        labels = args.label.strip().split(",")


    labels = []
    
    if not os.path.exists(PNFS_HOME):
        print_error("PNFS is not mounted. Quitting.")
        sys.exit(1)
        

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

