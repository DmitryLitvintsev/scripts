#!/usr/bin/env python

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

import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

try:
    from urllib2 import Request, urlopen
except ImportError:
    from urllib.request import Request, urlopen
    pass

printLock = multiprocessing.Lock()


"""
-- DROP TABLE IF EXISTS file_migrate;
CREATE TABLE file_migrate (
       src_bfid character varying,
       dst_path character varying,
       copied timestamp with time zone default NOW(),
       checked boolean, 
       stored boolean default false
);
ALTER TABLE ONLY file_migrate
      ADD CONSTRAINT pk_file_migrate PRIMARY KEY (src_bfid);

ALTER TABLE ONLY file_migrate
    ADD CONSTRAINT fk_file_migrate FOREIGN KEY (src_bfid)
    REFERENCES file(bfid)
    ON UPDATE CASCADE ON DELETE CASCADE;

grant select on  file_migrate to enstore_reader;

"""

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

CDF_DCAP = ("dcap://cdfdca1.fnal.gov:22125",
            "dcap://cdfdca1.fnal.gov:25125",
            "dcap://cdfdca1.fnal.gov:25136",
            "dcap://cdfdca1.fnal.gov:25137",
            "dcap://cdfdca1.fnal.gov:25138",
            "dcap://cdfdca1.fnal.gov:25139",
            "dcap://cdfdca1.fnal.gov:25140",
            "dcap://cdfdca1.fnal.gov:25141",
            "dcap://cdfdca1.fnal.gov:25143",
            "dcap://cdfdca1.fnal.gov:25144",
            "dcap://cdfdca2.fnal.gov:25145",
            "dcap://cdfdca2.fnal.gov:25146",
            "dcap://cdfdca2.fnal.gov:25147",
            "dcap://cdfdca2.fnal.gov:25148",
            "dcap://cdfdca2.fnal.gov:25149",
            "dcap://cdfdca2.fnal.gov:25150",
            "dcap://cdfdca2.fnal.gov:25151",
            "dcap://cdfdca2.fnal.gov:25152",
            "dcap://cdfdca2.fnal.gov:25153",
            "dcap://cdfdca2.fnal.gov:25154",
            "dcap://cdfdca3.fnal.gov:25155",
            "dcap://cdfdca3.fnal.gov:25156",
            "dcap://cdfdca3.fnal.gov:25157",
            "dcap://cdfdca3.fnal.gov:25158",
            "dcap://cdfdca3.fnal.gov:25159",
            "dcap://cdfdca3.fnal.gov:25160",
            "dcap://cdfdca3.fnal.gov:25161",
            "dcap://cdfdca3.fnal.gov:25162",
            "dcap://cdfdca3.fnal.gov:25163",
            "dcap://cdfdca3.fnal.gov:25164")


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
    door = get_random_door(CDF_DCAP)
    url = door + path
    rc = execute_command("dccp -P " + url)
    return rc


def dc_check(path):
    """
    Check if filer file (full path) is online in the source dCache system

    :param path: full file path name
    :type path: str
    :return: 0 - online, 1 - offline (or any other failure)
    :type: int
    """
    door = get_random_door(CDF_DCAP)
    url = door + path
    rc = 1
    count = 0
    while rc != 0 and count < 3:
        rc = execute_command("dccp -P -t -2 " + url)
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
    cmd = ("globus-url-copy -checksum-alg adler32 -verify-checksum -nodcau -p 4 -fast "
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


class StageWorker(multiprocessing.Process):
    """
    This class is responsible for staging files into source dCache system
    """
    def __init__(self, stage_queue, copy_queue):
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

    def run(self):
        """
        Main pre-staging loop

        :return: no value
        :rtype: none
        """
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host="cdfensrv0n",
                        port=8888,
                        user="enstore",
                        database="enstoredb")

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
                    cursor.execute("select f.bfid, f.pnfs_path, f.crc, f.size "
                                   "from file f inner join volume v on v.id = f.volume "
                                   "left outer join file_migrate fm on f.bfid = fm.src_bfid where v.label = %s "
                                   "and f.deleted = 'n' and fm.src_bfid is null order by f.location_cookie asc", (label, ))
                    res = cursor.fetchall()
                    if not res:
                        print_error("All files migrated for label %s" % (label,))
                        self.stage_queue.task_done()
                        continue
                    # files = [(i[0], re.sub("usr/usr", "usr", i[1]), i[2], i[3]) for i in res]
                    files = [(i[0], re.sub("/pnfs.*cdfen/","/pnfs/fnal.gov/usr/cdfen/", i[1]), i[2], i[3]) for i in res]
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
                bfid, f, crc, fsize = files.pop(0)
                count += 1
                if not os.path.exists(f):
                    print_error("%s : file %s does not exist" % (label, f))
                    continue
                crc1 = convert_0_adler32_to_1_adler32(crc, fsize)
                rc = dc_check(f)
                if rc != 0:
                    files.append((bfid, f, crc, fsize))
                    stages += 1
                    rc = dc_stage(f)
                    if rc != 0:
                        print_error("%s Failed to stage %s %s %d" % (label, bfid, f, rc))
                else:
                    # file is online
                    cached += 1
                    self.copy_queue.put((label, bfid, f, crc1))
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
    def get_db_checksum(pool, dest):
        """
        Extracts checksum value from the destination chimera DB

        :param pool: database connection pool
        :type pool: PooledDB

        :param dest: full path of the destination file name
        :type dest: str

        :return: checksum value
        :rtype: str
        """
        pnfsid = CopyWorker.get_pnfsid(dest)
        connection = None
        try:
            connection = pool.connection()
            res = CopyWorker.select(connection, "select isum from t_inodes_checksum where itype = 1 and "
                                                "inumber = (select inumber from t_inodes where ipnfsid = %s)",
                                    (pnfsid, ))
            return res[0][0]
        finally:
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass

    @staticmethod
    def get_local_checksum(f):
        """
        Extract checksum using .(get)(file name)(checksum) dot command

        :param f: full file name
        :type f: str

        :return: checksum value string
        :rtype: str
        """
        dname = os.path.dirname(f)
        fname = os.path.basename(f)
        csum_fn = dname + "/.(get)(" + fname + ")(checksum)"
        retries = 10
        for i in range(retries):
            try:
                with open(csum_fn, "r") as fh:
                    csums = [i.strip() for i in fh.readlines() if i.startswith("ADLER32")]
                    return csums[0].split(":")[1]
            except IOError:
                if i == retries - 1:
                    raise
                time.sleep(1)

    @staticmethod
    def get_pnfsid(f):
        """
        Return pnfsid for a given full file path name

        :param f: full file path name
        :type f: str

        :return: pnfsid string
        :rtype: str
        """
        dname = os.path.dirname(f)
        fname = os.path.basename(f)
        id_fn = dname + "/.(id)(" + fname + ")"
        retries = 10
        for i in range(retries):
            try:
                pnfsid = None
                with open(id_fn, "r") as fh:
                    pnfsid = [i.strip() for i in fh.readlines()][0]
                return pnfsid
            except IOError:
                if i == retries - 1:
                    raise
                time.sleep(1)

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
    def check(pool, entry, destination):
        """
        Check source and destination after successful copy

        :param pool: database connection pool
        :type pool: PooledDB

        :param entry: (label, bfid, file, crc) source tuple
        :type entry: tuple

        :param destination: destination full file path name
        :type destination: str

        :return: result
        :rtype: bool
        """
        label, bfid, f, crc = entry
        dst_stat = os.stat(destination)
        src_stat = os.stat(f)
        if dst_stat.st_size != src_stat.st_size:
            print_error("%s size mismatch %s %d %d %s" %
                        (label, bfid, dst_stat.st_size, src_stat.st_size, destination, ))
            return False
        try:
            dst_checksum = CopyWorker.get_local_checksum(destination)
            #dst_checksum = CopyWorker.get_db_checksum(dest_pool, destination)
        except Exception as e:
            print_error("%s Failed to get destination checksum %s %s" % (label, destination, str(e)))
            return False
        if dst_checksum.lstrip("0") == crc.lstrip("0"):
            return True
        print_error("%s checksum mismatch %s %s" % (label, dst_checksum, crc))
        pnfsid = CopyWorker.get_pnfsid(f)
        if pnfsid is None:
            print_error("%s Failed to get pnfsid %s %s" % (label, bfid, f, ))
            return False
        try:
            connection = pool.connection()
            res = CopyWorker.select(connection,
                                    "select isum from t_inodes_checksum where ipnfsid = %s", (pnfsid,))
            if not res:
                print_error("%s Failed to get source checksum %s %s" % (label, bfid, f, ))
                return True
            source_checksum = res[0][0]
            if source_checksum == dst_checksum:
                return True
        except Exception as e:
            print_error("Failed to get connection %s" % (str(e),))
            pass
        finally:
            try:
                if connection:
                    connection.close()
            except Exception:
                pass
        return False

    @staticmethod
    def mark_migrated(pool, entry, destination, check=False):
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
        label, bfid, f, crc = entry
        try:
            connection = pool.connection()
            try:
                res = CopyWorker.insert(connection, "insert into file_migrate (src_bfid, dst_path, checked) "
                                                    "values (%s, %s, %s)", (bfid, destination, check))
            except Exception as e:
                print_error("%s Failed to insert into file_migrate %s %s %s %s" % (label, bfid, f, destination, str(e)))
                try:
                    res = CopyWorker.update(connection, "update file_migrate set checked = true where "
                                                        "src_bfid=%s ", (bfid,))
                except Exception as e:
                    print_error("%s Failed to update file_migrate %s %s %s" % (label, bfid, f, destination))
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
    def touch(source, destination):
        """
        Set destination atime, mtime to be the same as tha of tyhe source

        :param source: full source file path
        :type source: str

        :param destination: full destination file path
        :type destination: str
        """
        stat = os.stat(source)
        wrapper(os.utime, (destination, (stat.st_atime, stat.st_mtime)))

    def run(self):
        """
        Main copy loop
        :return: no value
        :rtype: none
        """
        chimera_pool = PooledDB(psycopg2,
                                maxconnections=1,
                                maxcached=1,
                                blocking=True,
                                host="cdfensrv1n",
                                user="enstore_reader",
                                database="chimera")

        dest_chimera_pool = PooledDB(psycopg2,
                                     maxconnections=1,
                                     maxcached=1,
                                     blocking=True,
                                     host="stkensrv1n",
                                     user="enstore_reader",
                                     database="chimera")

        enstoredb_pool = PooledDB(psycopg2,
                                  maxconnections=1,
                                  maxcached=1,
                                  blocking=True,
                                  host="cdfensrv0n",
                                  port=8888,
                                  user="enstore",
                                  database="enstoredb")

        while True:
            entry = self.copy_queue.get()
            if not entry:
                print_message("%s: Exiting %d" % (self.name, self.copy_queue.qsize()))
                #self.copy_queue.task_done()
                break
            label, bfid, f, crc = entry
            # dest = re.sub("cdfen","cdf",source)
            # dest = re.sub("cdfen","cdf/backup/litvinse/cdfen",f)
            dest = re.sub("cdfen", "cdf/data", f)
            if os.path.exists(dest):
                print_error("%s %s File %s already exists" % (label, bfid, dest, ))
                if CopyWorker.check(chimera_pool, entry, dest):
                    CopyWorker.touch(f, dest)
                    CopyWorker.mark_migrated(enstoredb_pool, entry, dest, True)
                    #self.copy_queue.task_done()
                    continue
                else:
                    print_error("%s %s Failed to check destination %s %s" % (label, bfid, f, dest, ))
                    try:
                        os.unlink(dest)
                    except IOError as e:
                        print_error("%s %s Failed to remove destination %s %s " % (label, bfid, dest, str(e)))
                        #self.copy_queue.task_done()
                        continue
            count = 0
            rc = 1
            while rc != 0 and count < 10:
                count += 1
                rc = copy(f, dest)
            if rc != 0:
                print_error("%s Failed to copy %s %s %s" % (label, bfid, f, dest))
            else:
                print_message("%s %s Copied %s %s " % (label, bfid, f, dest))
                CopyWorker.touch(f, dest)
                if CopyWorker.check(chimera_pool, entry, dest):
                    CopyWorker.mark_migrated(enstoredb_pool, entry, dest, True)
                else:
                    print_error("%s Failed to check destination after successful copy %s %s %s " %
                                (label, bfid, f, dest))

            #self.copy_queue.task_done()
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

    parser.add_argument(
        "--processes",
        default=multiprocessing.cpu_count(),
        type=int,
        help="Number of parallel stage processes to spawn. Default : %(default)s (number of CPUs).")

    args = parser.parse_args()

    cpu_count = args.processes

    print_message("**** START ****")

    os.environ["X509_USER_KEY"] = "/etc/grid-security/hostkey.pem"
    os.environ["X509_USER_CERT"] = "/etc/grid-security/hostcert.pem"
    os.environ["X509_CERT_DIR"] = "/etc/grid-security/certificates"
    os.environ["X509_USER_PROXY"] = "/etc/grid-security/hostcert.pem"

    pool = PooledDB(psycopg2,
                    maxconnections=1,
                    maxcached=1,
                    blocking=True,
                    host="cdfensrv0n",
                    port=8888,
                    user="enstore_reader",
                    database="enstoredb")

    connection = pool.connection()
    cursor = connection.cursor()
    labels = []
#    try:
#        cursor.execute("select label from volume "
#                       "where library in ('CDF-10KCGS','CDF-10KCF1') "
#                       "and label not like '%.DELETED' "
#                       "and system_inhibit_0 = 'none' order by label limit 10")
#        labels = [i[0] for i in cursor.fetchall()]
#    except Exception as e:
#        print_error("Failed to retrieve labels %s" % str(e))
#        sys.exit(1)
#
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

    copy_workers = []

    for i in range(cpu_count):
        worker = StageWorker(stage_queue, copy_queue)
        stage_workers.append(worker)
        worker.start()

    for label in labels:
        stage_queue.put(label)

    for i in range(40):
        worker = CopyWorker(copy_queue)
        copy_workers.append(worker)
        worker.start()

    for i in range(cpu_count):
        stage_queue.put(None)

    stage_queue.join()

    print_message("---1----")

    while copy_queue.qsize():
        time.sleep(20)

    print_message("**** FINISH ****")

    for i in range(40):
        copy_queue.put(None)

    print_message("---2----")

    #copy_queue.join()

    print_message("---3---")


if __name__ == "__main__":
    main()

