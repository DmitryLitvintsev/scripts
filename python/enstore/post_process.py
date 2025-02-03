#!/bin/env python
from __future__ import print_function
import argparse
import errno
import json
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

SWAP_PACKAGE  = """
select swap_package(%s, %s)
"""

UPDATE_STATUS="""
UPDATE FILE
SET archive_status='ARCHIVED',
    cache_status = 'PURGED',
    original_library = 'TFF2-LTO9M,CD-LTO8G2' 
WHERE bfid=%s
"""


def print_progress(sg=None):
    connection = None
    try:
        connection = psycopg2.connect(database="enstoredb",
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
        connection = psycopg2.connect(database="enstoredb",
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


def get_bfid_info(connection, bfid):
    """
    get bfid info
    """
    Q = ("select v.label as external_label,"
         "       f.location_cookie as location_cookie,"
         "       f.size as size,"
         "       f.pnfs_path as path,"
         "       f.pnfs_id as pnfsid,"
         "       f.bfid as bfid,"
         "       f.drive as drive,"
         "       f.crc as complete_crc "
         "from file f inner join volume v on v.id = f.volume "
         "where f.bfid = %s")
        
    try:
        res = select(connection, 
                     Q,
                     (bfid, ), 
                     cursor_factory=psycopg2.extras.RealDictCursor)
        return res[0]
            
    except Exception as e:
        print_error("Failed to query file for bfid %s %s" % (bfid, str(e) ))
        pass

    return None


def write_layer(path, text, layer):
    dirname = os.path.dirname(path)
    fname  = os.path.basename(path)
    layer_file = os.path.join(dirname,'.(use)(%s)(%s)'%(str(layer),fname))
    with open(layer_file,'w') as fd:
        fd.write(text)

def write_layer_1(path,text):
    dirname=os.path.dirname(path)
    fname  = os.path.basename(path)
    layer_file = os.path.join(dirname,'.(use)(1)(%s)'%(fname))
    with open(layer_file,'w') as fd:
        fd.write(text)

def write_layer_4(path,text):
    dirname = os.path.dirname(path)
    fname  = os.path.basename(path)
    layer_file = os.path.join(dirname,'.(use)(4)(%s)'%(fname))
    with open(layer_file,'aw') as fd:
        fd.write(text)

class SfaFixWorker(multiprocessing.Process):
    """
    This class is responsible for file recovery
    """
    def __init__(self, queue):
        super(SfaFixWorker, self).__init__()
        self.queue = queue

    def run(self):
        """
        Main recovery loop

        :return: no value
        :rtype: none
        """
        # db connection pool to enstore db
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host="enstore00",
                        port=8888,
                        user="enstore",
                        database="enstoredb")

        connection = pool.connection()
        cursor = connection.cursor()

        for data in iter(self.queue.get, None):
            label, src_bfid, pnfsid, dst_bfid = data
            # 
            #  find old copy
            # 
            print_message("%s %s src_bfid = %s, dst_bfid = %s" % (label, pnfsid, src_bfid, dst_bfid))
            
            rows = select(connection, 
                          "SELECT fcm.alt_bfid, f.package_id, f.active_package_files_count from file_copies_map fcm "
                          "INNER JOIN file f on fcm.alt_bfid = f.bfid WHERE fcm.bfid = %s and f.active_package_files_count > 0",
                          (src_bfid,))
            if not rows:
                print_error("%s %s src_bfid = %s, dst_bfid = %s FOUND NO OLD COPY" % 
                            (label, pnfsid, src_bfid, dst_bfid))
                continue

            if len(rows) > 1:
                print_error("%s %s src_bfid = %s, dst_bfid = %s FOUND MULTIPLE OLD COPIES" % 
                            (label, pnfsid, src_bfid, dst_bfid))
                continue

            old_copy_bfid = rows[0][0]
            old_copy_package_id = rows[0][1]
            old_copy_active_package_files_count = 0
            if rows[0][2]: 
                old_copy_active_package_files_count = int(rows[0][2])
                
            if old_copy_active_package_files_count == 0:
                print_error("%s %s src_bfid = %s, dst_bfid = %s, old_copy = %s,  old_copy_active_package_files_count = 0 " %
                            (label, pnfsid, src_bfid, dst_bfid, old_copy_bfid))
                continue

            if old_copy_package_id != old_copy_bfid:
                print_error("%s %s src_bfid = %s, dst_bfid = %s, old_copy = %s != old_copy_package_id = %s " %
                            (label, pnfsid, src_bfid, dst_bfid, old_copy_bfid, old_copy_package_id))
                continue
                 
            # 
            #  find new copy
            # 
            rows = select(connection, 
                          "SELECT fcm.alt_bfid, f.package_id, f.active_package_files_count from file_copies_map fcm "
                          "INNER JOIN file f on fcm.alt_bfid = f.bfid WHERE fcm.bfid = %s and f.deleted != 'u'",
                          (dst_bfid,))
            
            if not rows:
                print_error("%s %s src_bfid = %s, dst_bfid = %s FOUND NO NEW COPIES " % 
                            (label, pnfsid, src_bfid, dst_bfid))
                continue

            if len(rows) > 1:
                print_error("%s %s src_bfid = %s, dst_bfid = %s FOUND TOO MANY NEW COPIES " % 
                            (label, pnfsid, src_bfid, dst_bfid))
                continue 
            
            new_copy_bfid = rows[0][0]
            new_copy_package_id = rows[0][1]
            new_copy_active_package_files_count = 0
            if rows[0][2]:
                new_copy_active_package_files_count = int(rows[0][2])

            if new_copy_bfid == new_copy_package_id and new_copy_active_package_files_count > 0:
                print_message("%s, %s, src_bfid = %s, dst_bfid = %s, old_copy = %s, new_copy = %s. Already swapped." % 
                              (label,
                               pnfsid,
                               src_bfid,
                               dst_bfid, 
                               old_copy_bfid,
                               new_copy_bfid,))
                continue
                
            if new_copy_active_package_files_count > 0:
                print_error("%s %s src_bfid = %s, dst_bfid = %s, old_copy = %s, new_copy = %s new_copy_active_package_files_count = %d " %
                            (label, pnfsid, src_bfid, dst_bfid, old_copy_bfid, new_copy_bfid, new_copy_active_package_files_count))
                continue

            print_message("%s %s src_bfid =%s, dst_bfid = %s , Swapping %s -> %s " % (label,
                                                                                      pnfsid,
                                                                                      src_bfid,
                                                                                      dst_bfid,
                                                                                      old_copy_bfid,
                                                                                      new_copy_bfid,))
            cursor.execute(SWAP_PACKAGE, 
                           (old_copy_bfid, new_copy_bfid))
            connection.commit()
            cursor.execute(UPDATE_STATUS, (new_copy_bfid,))
            connection.commit()
            
        connection.close()
        pool.close()


class SrcFixWorker(multiprocessing.Process):
    """
    This class is responsible for file recovery
    """
    def __init__(self, queue):
        super(SrcFixWorker, self).__init__()
        self.queue = queue

    def run(self):
        """
        Main recovery loop

        :return: no value
        :rtype: none
        """
        # db connection pool to enstore db
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host="enstore00",
                        port=8888,
                        user="enstore",
                        database="enstoredb")

        connection = pool.connection()
        cursor = connection.cursor()

        for data in iter(self.queue.get, None):
            src  = data.get("src")
            dst  = data.get("dst")
            dst.sort()
            comment = "=> " + " ".join(dst)
            cursor.execute("update volume "
                           "set comment = %s, system_inhibit_1 = 'migrated2', "
                           "si_time_1 = '2023-06-05 19:17:00' "
                           "where label = %s",
                           (comment, src,))
            connection.commit()
            
        connection.close()
        pool.close()


class DstFixWorker(multiprocessing.Process):
    """
    This class is responsible for file recovery
    """
    def __init__(self, queue):
        super(DstFixWorker, self).__init__()
        self.queue = queue

    def run(self):
        """
        Main recovery loop

        :return: no value
        :rtype: none
        """
        # db connection pool to enstore db
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host="enstore00",
                        port=8888,
                        user="enstore",
                        database="enstoredb")

        connection = pool.connection()
        cursor = connection.cursor()

        for data in iter(self.queue.get, None):
            tape_dict = data 

            dst  = data.get("dst")
            src  = data.get("src")
            src.sort()
            comment = "<= " + " ".join(src)
            cursor.execute("update volume set comment = %s where label = %s",
                           (comment, dst,))
            connection.commit()

#            rows = select(connection, 
#                          "SELECT f.pnfs_path, f.pnfs_id, f.location_cookie "
#                          "FROM file f inner join volume v on v.id = f.volume "
#                          "WHERE v.label = %s "
#                          "AND f.deleted = 'n' "
#                          "AND (v.file_family like '%%MIGRATION2' or v.file_family = 'migration_merge') "
#                          "order by f.location_cookie",
#                          (dst,))
#            
#            file_family = None
#            for row in rows:
#                pnfsid = row[1]
#                try:
#                    file_family = get_file_family(pnfsid)
#                    break
#                except:
#                    pass
#
#            if file_family:
#                cursor.execute("update volume set file_family = %s where label = %s",
#                               (file_family, dst,))
#                connection.commit()
            
        connection.close()
        pool.close()

def get_file_family(pnfsid):
    file_name = get_path(pnfsid)
    dir_name = os.path.dirname(file_name)
    ff = None
    with open("%s/.(tag)(file_family)" % (dir_name,), "r") as tag:
        ff = tag.readlines()[0].strip()
    return ff
    


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


def select(con, sql, pars, cursor_factory=None):
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
        if cursor_factory:
            cursor = con.cursor(cursor_factory=cursor_factory)
        else:
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


def bust_layers(pool, entry):

    """
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
            print_error("%s Failed to drop layers  %s %s %s " % (label, bfid, pnfsid, str(e)))
            pass
    except Exception as e:
        print_error("%s Failed to get connection when trying to drop layers %s %s %s" % (label, bfid, pnfsid, str(e),))
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

    queue = multiprocessing.Queue(1000)

    cpu_count = multiprocessing.cpu_count()
    processes = []

    cpu_count = 10

    for i in range(cpu_count):
        worker = SrcFixWorker(queue)
        processes.append(worker)
        worker.start()

    count = 0 
    with open("supermap_src_cms.json", "r") as json_file:
        for line in json_file: 
            json_line = json.loads(line)
            count += 1 
            queue.put(json_line)
            if not count % 1000 : 
                print_message("Processing src %d " % (count, ))

    for i in range(cpu_count):
        queue.put(None)

    for process in processes:
        process.join()

    del processes[:]
    
    for i in range(cpu_count):
        worker = DstFixWorker(queue)
        processes.append(worker)
        worker.start()

    count = 0 
    with open("supermap_dst_cms.json", "r") as json_file:
        for line in json_file: 
            json_line = json.loads(line)
            count += 1 
            queue.put(json_line)
            if not count % 1000 : 
                print_message("Processing dst %d " % (count, ))

    for i in range(cpu_count):
        queue.put(None)

    for process in processes:
        process.join()


if __name__ == "__main__":
    main()
