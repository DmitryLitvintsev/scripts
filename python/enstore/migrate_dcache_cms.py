#!/bin/env python

import argparse
import errno
import json
import multiprocessing
import os
import random
import re
import socket
import stat
import subprocess
import sys
import time
import paramiko
import gssapi

import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

try:
    from urllib2 import Request, urlopen
except ImportError:
    from urllib.request import Request, urlopen
    pass

printLock = multiprocessing.Lock()
kinitLock =  multiprocessing.Lock()




"""
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


HOSTNAME=socket.gethostname()
SSH_HOST = "cmsdcatapehead.fnal.gov"
SSH_PORT = 22224
SSH_USER = "admin"
POOL_GROUP = "readonlyPools"

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
    KRB5CCNAME = "/tmp/krb5cc_root.migration"
    os.environ["KRB5CCNAME"] =  KRB5CCNAME

    cmd = "/usr/bin/kinit -k host/%s"%(HOSTNAME)
    execute_command(cmd)


def get_shell():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname = SSH_HOST,
                port = SSH_PORT,
                username = SSH_USER,
                gss_auth = True,
                gss_kex = True)
    return ssh

def execute_admin_command(ssh, cmd):
    stdin, stdout, stderr = ssh.exec_command(cmd)
    if len(stderr.readlines()) > 0:
        raise RuntimeError(" ".join(stderr.readlines()))
    return stdout.readlines()


def is_cached(ssh, pnfsid):
    result = execute_admin_command(ssh, "\sn cacheinfoof " + pnfsid)
    payload = result[0].strip()
    if payload == "":
        return False
    else:
        return True


def mark_precious(ssh, pool, pnfsid):
    #result = execute_admin_command(ssh, "\s " + pool + " rep set precious " + pnfsid)
    result = execute_admin_command(ssh, "\sl " + pnfsid  + " rep set precious " + pnfsid)
    print_message("Marked precious %s" % ( result, ))
    return True

def get_locations(ssh, pnfsid):
    result = execute_admin_command(ssh, "\sn cacheinfoof " + pnfsid)
    payload = result[0].strip()
    if payload == "":
        return []
    else:
        return payload.split();

def get_active_pools_in_pool_group(ssh, pgroup):
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

def get_restore_queue(ssh, pool):
    cmd = "\s " + pool + " rh ls"
    result = execute_admin_command(ssh, cmd)
    print result
    ids = set()
    for line in result:
        i = line.strip()
        if not i:
            continue
        print i
        parts = i.strip().split()
        id = parts[-2]
        ids.add(id)
    return ids

def stage(ssh, pool, pnfsid):
    cmd = "\s " + pool + " rh restore " + pnfsid
    result = execute_admin_command(ssh, cmd)
    print_message("%s stage %s %s "%(pool, pnfsid, result, ))

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


BASE = 65521


def convert_0_adler32_to_1_adler32(crc, filesize):
    """
    Converts ADLER32 seeded 0 checksum to ADLER32 seeded 1 (correct) checksum

    :param crc: ADLER32 seeded 0 checksum value
    :type crc: str

    :param  filesize: file size
    :type filesize: int

    :return: ADLER32 seeded 1 checksum value with leading 0x dropped
    :rtype: str
    """
    crc = int(crc)
    filesize = int(filesize)
    size = int(filesize % BASE)
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) & 0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    return hex((s2 << 16) + s1).split('x')[1]


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
    #if rc != 0:
    #   print_error(errors)
    return rc


CDF_GFTP = ("gsiftp://cdfdca1.fnal.gov", "gsiftp://cdfdca2.fnal.gov", "gsiftp://cdfdca3.fnal.gov")

CDF_DCAP = ("pnfs://cmsdcatape.fnal.gov:22125/",)


def wrapper(func, args=None):
    """
    Execute a function wih retry loop

    :param func: function
    :type func: function
    :param args: function parameter list
    :type args: list
    :return: result of function execution
    """
    if not args:
        args = ()
    count = 0
    while count < 10:
        try:
            return func(*args)
        except (OSError, IOError) as e:
            count += 1
            if e.errno in (errno.EBUSY, errno.ELOOP, errno.ESTALE, errno.ENOENT, errno.EIO):
                if count < 10:
                    time.sleep(0.5)
                else:
                    raise

def get_path(pnfsid):
    path = None
    with open("/pnfs/fnal.gov/usr/.(pathof)(%s)"%(pnfsid,), "r") as fh:
        path = fh.readlines()[0].strip()
    return path

def get_random_door(door_list):
    """
    Select a random item from a list

    :param door_list: list of doors
    :type door_list: list
    :return: a random item from the list
    """
    length = len(door_list)
    return door_list[random.randint(0, length - 1)]


def get_ftp_doors(system="fndca3b.fnal.gov"):
    """
    Get list of FTP doors from info provoder of destination dCache system
    :param system: name of the host running info provider
    :type system: string
    :return: list of GFTP doors
    :type: list
    """
    url = "http://%s:2288/info/doors?format=json" % (system,)
    request = Request(url)
    request.add_header("Accept", "application/json")
    response = urlopen(request)
    data = json.load(response)
    doors = [i for i in data.values() if i.get("protocol").get("family") == "gsiftp"]
    door_list = []
    fqdn = None
    for d in doors:
        for key, value in d.get("interfaces").items():
            if value.get("scope") != "global":
                continue
            fqdn = socket.gethostbyaddr(value.get("address"))[0]
        port = d.get("port")
        protocol = d.get("protocol").get("family")
        door = protocol + "://" + fqdn + ":" + str(port)
        door_list.append(door)
    return door_list


def dc_stage(path):
    """
    Pre-stage file (full path) in the source dCache system

    :param path: full file path name
    :type path: str
    :return: 0 - success, 1 - failure
    :type: int
    """
    door = CDF_DCAP[0]
    url = door + path
    rc = execute_command("dccp -P " + url)
    return rc


def dc_check(pnfsid):
    """
    Check if filer file (full path) is online in the source dCache system

    :param path: full file path name
    :type path: str
    :return: 0 - online, 1 - offline (or any other failure)
    :type: int
    """
    door = CDF_DCAP[0]
    url = door + pnfsid
    rc = 1
    count = 0
    cmd = "dccp -P -t -2 " + url
    while rc != 0 and count < 3:
        rc = execute_command(cmd)
        time.sleep(0.1)
        count += 1
    return rc


def copy(source, dest):
    """
    Copy source to dest using globus-url-cpoy

    :param source:  source file full path
    :type source: str

    :param dest:  destination file full path
    :type dest: str

    :return: 0 - success, 1 - failure
    :type: int
    """
    source_door = get_random_door(CDF_GFTP)
    dest_door = "gsiftp://fndca4b.fnal.gov:2811"
    try:
        dest_door = get_random_door(get_ftp_doors())
    except Exception:
        print_error("Failed to get randoom door, using default %s " % (dest_door, ))
        pass
#    cmd = ("globus-url-copy -checksum-alg adler32 -verify-checksum -nodcau -p 4 -fast "
#           "-cd  -vb %s/%s %s/%s" % (source_door,
#                                     source,
#                                     dest_door,
#                                     dest))
    cmd = ("globus-url-copy -nodcau -p 4 -fast "
           "-cd  -vb %s/%s %s/%s" % (source_door,
                                     source,
                                     dest_door,
                                     dest))
    rc = execute_command(cmd)
    if rc != 0:
        cmd = ("globus-url-copy -nodcau -p 4 -fast "
               "-cd  -vb %s/%s %s/%s" % (source_door,
                                         source,
                                         dest_door,
                                         dest))
        rc = execute_command(cmd)
	if rc != 0:
	    try:
                os.chmod(source, stat.S_IWRITE | stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH )
	    except Exception as e:
                print_error("Failed to chmod %s %s"%(source, str(e)))
                pass
    return rc


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
    def __init__(self, stage_queue, copy_queue, pool):
        """
        Constructor, takes two arguments: stage_queue and copy_queue

        :param stage_queue: input queue containing volume labels
        :type stage_queue: JoinableQueue

        :param copy_queue: a queue that collects staged files for subsequent copy
        :type copy_queue: Queue
        """
        super(StageWorker, self).__init__()
        self.stage_queue = stage_queue
        self.copy_queue = copy_queue
        self.pool = pool

    def run(self):
        """
        Main pre-staging loop

        :return: no value
        :rtype: none
        """
        ssh = get_shell()
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host="enstore00",
                        port=8888,
                        user="enstore",
                        database="enstoredb")

        chimera_pool = PooledDB(psycopg2,
                                maxconnections=1,
                                maxcached=1,
                                blocking=True,
                                host="cmsdcatapedb",
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
                                   "and f.deleted = 'n' and fm.src_bfid is null order by f.location_cookie asc", (label, ))
                    res = cursor.fetchall()
                    if not res:
                        print_error("All files migrated for label %s" % (label,))
                        self.stage_queue.task_done()
                        continue
                    files = []
                    for i in res:
                        try:
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
            print("Doing label %s, number of files %d" % (label, number_of_files))
            stages = count = cached = loop = 0
            while files:
                bfid, pnfsid, crc, fsize = files.pop(0)
                count += 1
                rc = dc_check(pnfsid)
                if rc != 0:
                    files.append((bfid, pnfsid, crc, fsize))
                    stages += 1
                    rc = stage(ssh, self.pool, pnfsid)
                else:
                    # file is online
                    cached += 1
                    print_message("%s File is online, calling mark_precious %s %s" % (label, bfid, pnfsid))
                    rc = mark_precious(ssh, self.pool, pnfsid)
                    if rc:
                        rc = CopyWorker.bust_layers(chimera_pool, (label, bfid, pnfsid, crc, self.pool))
                        rc = CopyWorker.mark_migrated(pool, (label, bfid, pnfsid, crc, self.pool))

                if (stages == 1500 or count == number_of_files) and files:
                    loop += 1
                    print_message("%s : %d staged, %d remain, %d pass" %
                                  (label, cached, count - cached, loop))
                    number_of_files = len(files)
                    stages = count = cached = 0
                    print_message("%s Sleeping" % (label,))
                    time.sleep(1200)
            # label is done here
            try:
                connection = pool.connection()
                cursor = connection.cursor()
                try:
                    cursor.execute("update volume set system_inhibit_1 = 'migrating' where label = %s", (label,))
                    connection.commit()
                except Exception as e:
                    connection.rollback()
                    print_error("%s failed to update system_inhibit_1: %s" % (label, str(e)))
                    continue
            except Exception as e:
                print_error("% Failed to connect to enstoredb to update system_inhibit_1 %s " % (label, str(e), ))
                continue
            finally:
                for i in (cursor, connection):
                    if i:
                        try:
                            i.close()
                        except Exception:
                            pass
            self.stage_queue.task_done()
        ssh.close()
        return


class CopyWorker(multiprocessing.Process):
    """
    This class is responsible for copying files between two dCache systems
    """
    def __init__(self, copy_queue):
        """
        Constructor takes copy_queue containing tuple (label, bfid, file, crc) files to be copied

        :param copy_queue: queue of files to be copied
        :type copy_queue: Queue
        """
        super(CopyWorker, self).__init__()
        self.copy_queue = copy_queue


    @staticmethod
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
        return CopyWorker.insert(con, sql, pars)

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def bust_layers(pool, entry):
        connection = None
        label, bfid, pnfsid, crc, dcache_pool = entry
        try:
            connection = pool.connection()
            try:
                res = CopyWorker.select(connection, "select pnfsid2inumber(%s)",(pnfsid,))
                inumber = res[0][0]
                res = CopyWorker.insert(connection, "delete from t_level_1 where inumber=%s",(inumber,))
                res = CopyWorker.insert(connection, "delete from t_level_4 where inumber=%s",(inumber,))
                res = CopyWorker.insert(connection, "delete from t_storageinfo where inumber=%s",(inumber,))
                res = CopyWorker.insert(connection, "delete from t_locationinfo where inumber=%s and itype=0",(inumber,))
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


    @staticmethod
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
                res = CopyWorker.insert(connection, "insert into file_migrate (src_bfid, pnfsid) "
                                                    "values (%s, %s)", (bfid, pnfsid,))

                res = CopyWorker.update(connection, "update file set deleted = 'y' where bfid = %s", (bfid, ))
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


    def run(self):
        """
        Main copy loop
        :return: no value
        :rtype: none
        """
        ssh = get_shell()
        chimera_pool = PooledDB(psycopg2,
                                maxconnections=1,
                                maxcached=1,
                                blocking=True,
                                host="cmsdcatapedb",
                                user="enstore",
                                database="chimera")

        enstoredb_pool = PooledDB(psycopg2,
                                  maxconnections=1,
                                  maxcached=1,
                                  blocking=True,
                                  host="enstore00",
                                  port=8888,
                                  user="enstore",
                                  database="enstoredb")

        while True:
            entry = self.copy_queue.get()
            if not entry:
                break
            label, bfid, pnfsid, crc, dcache_pool = entry
            try:
                    rc = mark_precious(ssh,dcache_pool, pnfsid)
                    if rc:
                        rc = CopyWorker.bust_layers(chimera_pool, entry)
                        rc = CopyWorker.mark_migrated(enstoredb_pool, entry)
            except:
                pass

        ssh.close()
        return


def main():
    """
    main function
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--file",
        help="file containing label list")

    parser.add_argument(
        "--label",
        help="comma separated list of labels")


    args = parser.parse_args()


    print_message("**** START ****")

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
    labels = []

    if args.file:
        with open(args.file, "r") as f:
            labels = [i.strip() for i in f]

    if args.label:
        labels = args.label.strip().split(",")

    if not labels:
        print_error("No lanels specified ")

    print_message("Found %d labels" % (len(labels)))

    stage_queue = multiprocessing.JoinableQueue()
    copy_queue = multiprocessing.Queue()
    stage_workers = []

    kinitWorker = KinitWorker()
    kinitWorker.start()

    ssh = get_shell()
    pools = get_active_pools_in_pool_group(ssh, POOL_GROUP)
    cpu_count = len(pools)
    ssh.close()

    for i in range(cpu_count):
        worker = StageWorker(stage_queue, copy_queue, pools[i])
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
