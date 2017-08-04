#!/usr/bin/env python
"""
Script populates access latency and retention_policy columns
"""

import multiprocessing
import sys
import time

import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

printLock = multiprocessing.Lock()


UPDATE_QUERY = """
               update t_inodes set iaccess_latency = %s,
                                   iretention_policy=%s
                      where ipnfsid = %s
               """

def print_error(text):
    with printLock:
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(time.time()))+
                         " : " +text+"\n")
        sys.stderr.flush()


def print_message(text):
    with printLock:
        sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(time.time()))+
                         " : " +text+"\n")
        sys.stdout.flush()


class Worker(multiprocessing.Process):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue
        self.pool = PooledDB(psycopg2,
                             maxconnections=1,
                             maxcached=1,
                             blocking=True,
                             host="localhost",
                             port=8888,
                             user="enstore",
                             database="chimera")
    def __del__(self):
        self.pool.close()

    def run(self):
        connection = pool.connection()
        cursor = connection.cursor()
        count = 0
        for data in iter(self.queue.get, None):
            pnfsid = data[1]
            al = data[2]
            rp = data[3]
            count += 1
            try:
                cursor.execute(UPDATE_QUERY, (al, rp, pnfsid, ))
                connection.commit()
            except Exception, e:
                connection.rollback()
                print_error("Failed to update %s %s "%(pnfsid, str(e)))

if __name__ == "__main__":

    print_message("Start")
    pool = PooledDB(psycopg2,
                    maxconnections=1,
                    maxcached=1,
                    blocking=True,
                    host="localhost",
                    port=8888,
                    user="enstore",
                    database="chimera")

    queue = multiprocessing.Queue(10000)
    cpu_count = multiprocessing.cpu_count()
    processes = []
    cpu_count = multiprocessing.cpu_count()

    for i in range(cpu_count):
        checker = Worker(queue)
        processes.append(checker)
        checker.start()

    db = pool.connection()
    cursor = db.cursor('cursor_for_al_rp', cursor_factory=psycopg2.extras.DictCursor)

    QUERY = """
            SELECT ti.imtime,
                   ti.ipnfsid,
                   al.iaccesslatency,
                   rp.iretentionpolicy
            FROM t_inodes ti
            JOIN t_access_latency al ON al.ipnfsid=ti.ipnfsid
            JOIN t_retention_policy rp ON rp.ipnfsid=ti.ipnfsid
            AND ti.itype=32768
            AND ti.imtime < '2017-08-04 15:57:00'
            """
    cursor.execute(QUERY)
    total = 0
    t0 = time.time()

    while True:
        res = cursor.fetchmany(10000)
        if not res:
            break
        total += len(res)
        for r in res:
            mtime, pnfsid, access_latency, retention_policy = r
            queue.put((mtime, pnfsid, access_latency, retention_policy))
        print_message("Ingesting %d,  queue size %d "%(total, queue.qsize()))
    cursor.close()
    db.close()

    for i in range(cpu_count):
        queue.put(None)

    for process in processes:
        process.join()

    print_message("Finish")

