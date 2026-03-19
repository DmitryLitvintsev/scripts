#!/bin/env python
"""
set AL/RP of scratch file from NEARLINE/CUSTODIAL to NEARLINE/OUPTUT
and removing sticky
"""
from __future__ import print_function
import errno
import multiprocessing
import os
import psycopg2
import psycopg2.extras
import re
import socket
import subprocess
import sys
import time
import uuid
import paramiko
import yaml

try:
    import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse

printLock = multiprocessing.Lock()
kinitLock = multiprocessing.Lock()
UUID = str(uuid.uuid4())
CONFIG_FILE = os.getenv("MIGRATION_CONFIG")
if not CONFIG_FILE:
    CONFIG_FILE = "migration.yaml"


HOSTNAME = socket.gethostname()
SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"
POOL_GROUP = "readWritePools"


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


def get_shell(host=SSH_HOST, port=SSH_PORT, user=SSH_USER):
    """
    Admin shell
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host,
                port=port,
                username=user,
                gss_auth=True,
                gss_kex=True)
    return ssh


def execute_admin_command(ssh, cmd):
    """
    Execute admin shell command
    """
    stdin, stdout, stderr = ssh.exec_command(cmd)
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


def get_non_volatile_pools(ssh):
    """
    Get list of pools in a  pool group
    """
    result = execute_admin_command(ssh, "\l ")
    pools = []
    for line in result:
        i = line.strip()
        if not i:
            continue

        for prefix in ("rw-", "w-", "r-"):
            if i.startswith(prefix):
                pools.append(i)
                break
    return pools

def get_volatile_pools(ssh):
    """
    Get list of pools in a  pool group
    """
    result = execute_admin_command(ssh, "\l ")
    pools = []
    for line in result:
        i = line.strip()

        if not i:
            continue

        if i.startswith("v"):
            pools.append(i)

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

            
class KinitWorker(multiprocessing.Process):
    def __init__(self):
        super().__init__()
        self.stop = False

    def run(self):
        while not self.stop:
            with kinitLock:
                kinit()
                time.sleep(14400)


class Worker(multiprocessing.Process):
    """
    This class is responsible for staging files into source dCache system
    """
    def __init__(self, queue, config):
        super().__init__()
        self.queue = queue
        self.config = config

    def run(self):
        # chimera_db
        chimera_db = create_connection(self.config.get("chimera_db"))
        # admin shell
        ssh = get_shell(self.config.get("admin").get("host", SSH_HOST),
                        self.config.get("admin").get("port", SSH_PORT),
                        self.config.get("admin").get("user", SSH_USER))

        for pool in iter(self.queue.get, None):
            print_message(f"Doing pool {pool}")
            repls = execute_admin_command(ssh, f"\s {pool} rep ls -l=p")
            if repls:
                for line in repls:
                    parts = line.strip().split()
                    pnfsid = parts[0]
                    sc = parts[-1]
                    print_message(f"{pool} {pnfsid} {sc}")
                    try:
                        update(chimera_db,
                               "update t_inodes set iaccess_latency = 0, iretention_policy = 1 where ipnfsid = %s",
                               (pnfsid,))
                        rc = execute_admin_command(ssh,
                                                   f"\s {pool} rep set cached {pnfsid}")
                    except Exception as e:
                        print_error(f"file {pnfsid} failed {e}")
                        pass
                
            print_message(f"Done pool {pool}")
        chimera_db.close()

def main():
    """
    main function
    """
    try:
        with open(CONFIG_FILE, "r") as f:
            configuration = yaml.safe_load(f)
    except (OSError, IOError) as e:
        if e.errno == errno.ENOENT:
            print_error("Config file %s does not exist" % (CONFIG_FILE,))
        sys.exit(1)

    if not configuration:
        print_error("Failed to load configuration %s" % (CONFIG_FILE,))
        sys.exit(1)

    print_message("**** START ****")


    kinitWorker = KinitWorker()
    kinitWorker.start()

    ssh = get_shell(configuration.get("admin").get("host", SSH_HOST),
                    configuration.get("admin").get("port", SSH_PORT),
                    configuration.get("admin").get("user", SSH_USER))

    pools = get_volatile_pools(ssh)


    ssh.close()

    if not pools:
        print_error("found no pools")
        sys.exit(1)

    queue = multiprocessing.Queue(1000)
    cpu_count = multiprocessing.cpu_count()
    
    workers = []
    for i in range(cpu_count):
        worker = Worker(queue, configuration)
        workers.append(worker)
        worker.start()

    for pool in pools:
         queue.put(pool)

    for i in range(cpu_count):
        queue.put(None)

    list(map(lambda x : x.join(600), workers))

    kinitWorker.stop = True
    kinitWorker.terminate()

    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()
