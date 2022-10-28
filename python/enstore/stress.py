#!/usr/bin/env python

"""
script to stress file clerk
"""

import time
import os
import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB
import enstore_functions2
import configuration_client
import re
import types
import sys
import multiprocessing
import info_client
import e_errors

printLock = multiprocessing.Lock()

class Worker(multiprocessing.Process):
    def __init__(self,queue,fcc):
        super(Worker,self).__init__()
        self.queue = queue
        self.fcc    = fcc

    def run(self):
        for data in iter(self.queue.get, None):
            bfid = data
            bfid_info = self.fcc.bfid_info(bfid)
            if bfid_info['status'][0] != e_errors.OK:
                try:
                    printLock.acquire()
                    print "Failed to retrieve bfid ", bfid, bfid_info['status']
                finally:
                    printLock.release()
            continue


if __name__ == "__main__":

    csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                      enstore_functions2.default_port()))

    fcc = info_client.infoClient(csc)
    db_info = csc.get("database")

    enstoreDbPool = PooledDB(psycopg2,
                             maxconnections=1,
                             maxcached=10,
                             blocking=True,
                             host=db_info.get('dbhost', 'localhost'),
                             port=db_info.get('dbport', 8800),
                             user=db_info.get('dbuser', 'enstore'),
                             database=db_info.get('dbname', 'enstoredb'))

    queue = multiprocessing.Queue(10000)

    cpu_count = multiprocessing.cpu_count() * 10
    processes = []

    for i in range(cpu_count):
        worker = Worker(queue,fcc)
        processes.append(worker)
        worker.start()


    db = enstoreDbPool.connection()
    cursor = db.cursor('cursor_for_scan', cursor_factory=psycopg2.extras.DictCursor)
    Q="""
    select f.bfid from file f, volume v where
           v.id=f.volume and v.system_inhibit_0!='DELETED' and f.deleted<>'y'
    """

    cursor.execute(Q)
    total = 0
    files=0
    t0 = time.time()
    while True:

        res = cursor.fetchmany(10000)
        ll = len(res)
        total += ll
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
