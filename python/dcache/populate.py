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

BASE_PATH = "/pnfs/fs/usr/ssa_test/CTA/small"
BASE_PATH = "/pnfs/fs/usr/ssa_test/CTA/large"

MB = 1<<20

printLock = multiprocessing.Lock()

def write_file(name, file_size):
    with open(name, "a") as f:
        f.write(os.urandom(file_size))


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
        for name, size in iter(self.queue.get, None):
            write_file(name, size)
            counter += 1
            if not counter % 1000 :
                print_message("%s , Done %d " % (self.name, counter, ))



def main():
    queue = multiprocessing.Queue(10000)
    cpu_count = multiprocessing.cpu_count()

    #
    # start workers
    #
    workers = []
    for i in range(cpu_count):
        worker = Worker(queue)
        workers.append(worker)
        worker.start()

    LIMIT = 8000000000000
    file_size = 50*MB
    file_size = 2000*MB

    total_size = 0
    count = 0
    while total_size < LIMIT:
        total_size += file_size
        number = random.randrange(100)
        name = "%s/%d/%s.data" % (BASE_PATH, number, str(uuid.uuid4()))
        count += 1
        queue.put((name, file_size))

    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    print_message("**** FINISH ****")


if __name__ == "__main__":
    main()
