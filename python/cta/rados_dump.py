import json
import multiprocessing
import os
import rados
import subprocess
import sys
import time

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
    return ( output, errors , rc)


class Worker(multiprocessing.Process):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def run(self):
        for key in iter(self.queue.get, None):
            output, errors, rc = execute_command(f"cta-objectstore-dump-object  rados://ctaprdqueue@rados.cta.ctaprdqueue:ctaprdqueue {key}")
            if rc == 0:
                str_parts = output.decode("utf-8").strip().split("\n")
                str_whole = "\n".join(str_parts[11:])
                data = json.loads(str_whole)
                #print_message(json.dumps(data, indent=4))
                status = data.get("isfailed", "N/A")
                print_message(f"{key} failed: {status}")

def main():

    queue = multiprocessing.Queue(10000)
    cpu_count = multiprocessing.cpu_count()


    workers = []
    for i in range(cpu_count):
        worker = Worker(queue)
        workers.append(worker)
        worker.start()

    
    cluster = rados.Rados(conffile="/etc/ceph/ceph.conf", rados_id="ctaprdqueue")
    cluster.connect()
    pools = cluster.list_pools()

    # Open an I/O context to interact with a specific pool
    pool_name = "rados.cta.ctaprdqueue"
    ioctx = cluster.open_ioctx(pool_name)
    ioctx.set_namespace("ctaprdqueue")

    for obj in ioctx.list_objects():
        key = obj.key
        queue.put(key)

    for i in range(cpu_count):
        queue.put(None)

    list(map(lambda x : x.join(600), workers))
    

if __name__ == "__main__":
    main()

