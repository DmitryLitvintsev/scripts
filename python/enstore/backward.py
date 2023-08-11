#!/usr/bin/env python

import time
import os
import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB
import re
import types
import sys 
import multiprocessing

crc_match = re.compile("[:;]c=1:[a-zA-Z0-9]{8}")
size_match = re.compile("[:;]l=[0-9]*")

printLock = multiprocessing.Lock()

BASE = 65521


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



class Checker(multiprocessing.Process):
    def __init__(self,queue):
        super(Checker,self).__init__()
        self.queue = queue

    def run(self):
        pool = PooledDB(psycopg2,
                        maxconnections=1,
                        maxcached=1,
                        blocking=True,
                        host="enstore01",
                        user="enstore",
                        database="chimera")
        
        connection = pool.connection()
        cursor = connection.cursor()

        for data in iter(self.queue.get, None):
            label, bfid, pnfsid, path  = data 

            Q= """
            select ipnfsid from t_inodes where ipnfsid = %s
            """
            cursor.execute(Q, (pnfsid, ))
            r = cursor.fetchall()
            if not r:
                print_error("File does not exist %s %s %s %s" % (label, bfid, pnfsid, path ))
            connection.commit()
        cursor.close()
        connection.close()
        pool.close()


if __name__ == "__main__":

    enstoreDbPool = PooledDB(psycopg2,
                             maxconnections=1,
                             maxcached=10,
                             blocking=True,
                             host="enstore00",
                             port=8888,
                             user="enstore",
                             database="enstoredb")


    queue = multiprocessing.Queue(10000)

    cpu_count = multiprocessing.cpu_count()
    processes = []

    for i in range(cpu_count):
        checker = Checker(queue)
        processes.append(checker)
        checker.start()

    db = enstoreDbPool.connection()
    cursor = db.cursor('cursor_for_backward_scan', cursor_factory=psycopg2.extras.DictCursor)
    Q="""
    select v.label, f.bfid, f.pnfs_id, f.pnfs_path 
           from file f inner join volume v on v.id = f.volume where
           f.deleted = 'n' and f.pnfs_id != '' and v.storage_group not in  ('nova', 'cms')
           and v.file_family not like '%copy_1'
    """
    
    cursor.execute(Q)
    total = 0
    files=0
    t0 = time.time()
    while True:
        res = cursor.fetchmany(10000)
        ll = len(res)
        total += ll
        print_message("Doing %d " % (total, ))
        if len(res) == 0:
            break
        for r in res:
            queue.put(r)
        t1 = time.time()
        t0 = t1
    cursor.close()
    db.close()

    for i in range(cpu_count):
        queue.put(None)

    for process in processes:
        process.join()


