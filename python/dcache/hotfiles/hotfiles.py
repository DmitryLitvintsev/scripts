#!/bin/env python
from __future__ import print_function
import argparse
import errno
import multiprocessing
import os
import random
import re
import socket
import subprocess
import sys
import time
import uuid
import paramiko
import yaml

printLock = multiprocessing.Lock()
kinitLock = multiprocessing.Lock()

UUID = str(uuid.uuid4())

CONFIG_FILE = os.getenv("HOTFILES_CONFIG")
if not CONFIG_FILE:
    CONFIG_FILE = "hotfiles.yaml"


HOSTNAME = socket.gethostname()
SSH_HOST = "example.org"
SSH_PORT = 22224
SSH_USER = "admin"
POOL_GROUP = "flushPools"

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
    if len(stderr.readlines()) > 0:
        raise RuntimeError(" ".join(stderr.readlines()))
    return [i.strip().replace(r"\r","\n") for i in stdout.readlines() if i.strip().replace(r"\r", "\n") != ""]


def pp_get(ssh, source_pool, destination_pool, pnfsid):
    """
    trgger pp get file on destination_pool from source pool
    """
    cmd = "\s %s pp get file %s %s "
    cmd = cmd % (destination_pool,
                 pnfsid,
                 source_pool)
    print_message(cmd)
    result = execute_admin_command(ssh, cmd)

    return result


def migrate(ssh, pool, destination, pnfsid, copies):
    """
    trigget migration copy of pnfs on pool.
    """
    cmd = "\s %s migration copy -tmode=cached -select=random -pnfsid=%s -replicas=%d -target=pgroup %s"
    cmd = cmd % (pool,
                 pnfsid,
                 copies,
                 destination,)
    print_message(cmd)
    result = execute_admin_command(ssh, cmd)

    return result


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


def moverls(ssh, pool):
    result = execute_admin_command(ssh, "\s " + pool + " mover ls")
    return result

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
        if i.strip().find("nested groups") != -1:
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


class HotFileReplicator(multiprocessing.Process):
    """
    This class is responsible for replicating file on pools
    """
    def __init__(self, queue, config):
        super(HotFileReplicator, self).__init__()
        self.config = config
        self.queue = queue
        self.threshold = config.get("threshold")
        self.number_of_copies = config.get("number_of_copies")


    def run(self):
        """
        :return: no value
        :rtype: none
        """

        # admin shell
        ssh = get_shell(self.config.get("admin").get("host", SSH_HOST),
                        self.config.get("admin").get("port", SSH_PORT),
                        self.config.get("admin").get("user", SSH_USER))

        all_pools = get_active_pools_in_pool_group(ssh,
                                                   self.config.get("pool_group"))

        for pool in iter(self.queue.get, None):
            all_pools.remove(pool)
            mover_ls_output = moverls(ssh, pool)
            data = {}
            counter = 0
            for line in mover_ls_output:
                parts = line.strip().split()
                pnfsid = parts[4]
                locations = None
                try:
                    locations = get_locations(ssh, pnfsid)
                    counter += 1
                except RuntimeError as e:
                    continue
                if pnfsid not in data:
                    data[pnfsid] = { "queue" : 1,
                                     "count" : len(locations),
                                     "locations" : locations}
                else:
                    data[pnfsid]["queue"] += 1
            files = [(i[0], self.number_of_copies-i[1]["count"], i[1]["locations"]) for i in data.items()
                     if i[1]["queue"] > self.threshold
                     and i[1]["count"] < self.number_of_copies]
            for i in files:
                pnfsid = i[0]
                replicas_to_make = i[1]
                locations = i[2]
                pools = [p for p in all_pools if p not in locations]
                if len(pools) >= replicas_to_make:
                    destination_pools = random.sample(pools, replicas_to_make)
                    for destination_pool in destination_pools:
                        try:
                            r = pp_get(ssh,
                                       pool,
                                       destination_pool,
                                       pnfsid)
                        except RuntimeError as e:
                            print_error("%s failed to pp get on pools %s %s" % (pool,
                                                                                destination_pool,
                                                                                pnfsid,))
#                try:
#                    r = migrate(ssh,
#                                pool,
#                                self.config.get("pool_group"),
#                                pnfsid,
#                                1)
#                    print_message("%s migrating %s %d" %(pool, pnfsid, replicas_to_make))
#                except RuntimeError as e:
#                    print_error("%s failed to migrate %s" % (pool, pnfsid,))
        ssh.close()
        return

def main():
    """
    main function
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="This script replicates hot files in dCache. "
        "Hot file - is a file taht has more than threshold trandfers in mover queue. "
        )

    args = parser.parse_args()


    configuration = None
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

    queue = multiprocessing.Queue()
    workers = []

    kinitWorker = KinitWorker()
    kinitWorker.start()

    ssh = get_shell(configuration.get("admin").get("host", SSH_HOST),
                    configuration.get("admin").get("port", SSH_PORT),
                    configuration.get("admin").get("user", SSH_USER))

    pools = get_active_pools_in_pool_group(ssh,
                                           configuration.get("pool_group"))

    ssh.close()

    cpu_count = multiprocessing.cpu_count()

    for i in range(cpu_count):
        worker = HotFileReplicator(queue,
                                   configuration)
        workers.append(worker)
        worker.start()

    for pool in pools:
        queue.put(pool)

    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    kinitWorker.stop = True
    kinitWorker.terminate()

    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()
