#!/usr/bin/env python
"""
script that populates data in BASE_PATH directory

"""

from __future__ import print_function
import multiprocessing
import os
import random
import sys
import time
import uuid
import stat

printLock = multiprocessing.Lock()
STOPPER="/tmp/STOP"

def get_path(pnfsid):
    """
    Get path for pnfsid
    Uses mounts pnfs to do that
    """
    path = None
    with open("/pnfs/fnal.gov/usr/.(pathof)(%s)" % (pnfsid, ), "r") as fh:
        path = fh.readlines()[0].strip()
    return path


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


class Worker(multiprocessing.Process):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue

    def run(self):
        counter = 0
        for pnfsid in iter(self.queue.get, None):
            if not pnfsid:
                return
            try:
                file_name = get_path(pnfsid)
                print_message(f"Removing {pnfsid} {file_name}")
                try:
                    os.unlink(file_name)
                except:
                    print_message(f"Failed {pnfsid} {file_name}")
            except FileNotFoundError as e:
                pass


def main():

    queue = multiprocessing.Queue(10000)
    cpu_count = multiprocessing.cpu_count() // 2
    cpu_count = 16

    #
    # start workers
    #
    workers = []
    for i in range(cpu_count):
        worker = Worker(queue)
        workers.append(worker)
        worker.start()


    file_list = sys.argv[1]

    with open(file_list, "r") as fd:
        for line in fd:
            pnfsid = line.strip()
            parts = pnfsid.split(",")
            if len(parts) > 2:
                pnfsid = parts[-1].strip()
            queue.put(pnfsid)

    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()
