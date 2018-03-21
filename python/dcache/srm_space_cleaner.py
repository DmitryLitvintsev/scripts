#!/usr/bin/env python

import argparse
import datetime
import multiprocessing
import sys

import psycopg2
import psycopg2.extras

DESCRIPTION="""
This script scans srmspacefile table and for each pnfsid
checks if it is present in chimera t_inodes table. If the 
record is not found it is removed from srmspacefile table.
"""


CHIMERA_DB_NAME = "wuppertal_chimera"
CHIMERA_DB_USER = "enstore"
CHIMERA_DB_HOST = "localhost"
CHIMERA_DB_PORT = 5432


SPACEMANAGER_DB_NAME = "wuppertal_srm"
SPACEMANAGER_DB_USER = "enstore"
SPACEMANAGER_DB_HOST = "localhost"
SPACEMANAGER_DB_PORT = 5432


DELETE_SPACEFILE_ENTRY="""
DELETE FROM srmspacefile WHERE pnfsid= %s
"""

SPACEMANAGER_SCAN_QUERY="""
select pnfsid from srmspacefile
"""

CHIMERA_CHECK_QUERY="""
select * from t_inodes where ipnfsid = %s
"""

def print_error(text):
    sys.stderr.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + " : " +text+"\n")
    sys.stderr.flush()

def print_message(text):
    sys.stdout.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + " : " +text+"\n")
    sys.stdout.flush()


printLock = multiprocessing.Lock()

class Worker(multiprocessing.Process):
    def __init__(self,queue,spacemanager,chimera):
        super(Worker,self).__init__()
        self.chimera_connection =  psycopg2.connect(database=chimera[0],
                                                    user=chimera[1],
                                                    host=chimera[2],
                                                    port=chimera[3])
        self.chimera_cursor     =  self.chimera_connection.cursor()

        self.spacemanager_connection =  psycopg2.connect(database=spacemanager[0],
                                                    user=spacemanager[1],
                                                    host=spacemanager[2],
                                                    port=spacemanager[3])
        self.spacemanager_cursor     =  self.spacemanager_connection.cursor()
        self.queue = queue

    def run(self):
        for data in iter(self.queue.get, None):
            pnfsid = data
            """
            check if pnfsid exist in chimera
            """
            self.chimera_cursor.execute(CHIMERA_CHECK_QUERY,(pnfsid,))
            res = self.chimera_cursor.fetchall()
            if not res:
                with printLock:
                    print_message("Removing %s from srmspacefile"%(pnfsid,))
                try:
                    self.spacemanager_cursor.execute(DELETE_SPACEFILE_ENTRY,(pnfsid,))
                    self.spacemanager_connection.commit()
                except Exception, msg:
                    with printLock:
                        print_message("Failed Removing %s from srmspacefile : %s"%(pnfsid,str(msg)))
                    self.spacemanager_connection.rollback()

if __name__ == "__main__":

    """
    parse arguments
    """

    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument(
        "--chimera-host",
        default=CHIMERA_DB_HOST,
        help="chimera db host (default: %s)"% CHIMERA_DB_HOST)

    parser.add_argument(
        "--chimera-user",
        default=CHIMERA_DB_USER,
        help="chimera db user (default: %s)"% CHIMERA_DB_USER)

    parser.add_argument(
        "--chimera-dbname",
        default=CHIMERA_DB_NAME,
        help="chimera db name (default: %s)"% CHIMERA_DB_NAME)

    parser.add_argument(
        "--chimera-port",
        default=CHIMERA_DB_PORT,
        help="chimera db port (default: %d)"% CHIMERA_DB_PORT)


    parser.add_argument(
        "--spacemanager-host",
        default=SPACEMANAGER_DB_HOST,
        help="spacemanager db host (default: %s)"% SPACEMANAGER_DB_HOST)

    parser.add_argument(
        "--spacemanager-user",
        default=SPACEMANAGER_DB_USER,
        help="spacemanager db user (default: %s)"% SPACEMANAGER_DB_USER)

    parser.add_argument(
        "--spacemanager-dbname",
        default=SPACEMANAGER_DB_NAME,
        help="spacemanager db name (default: %s)"% SPACEMANAGER_DB_NAME)

    parser.add_argument(
        "--spacemanager-port",
        default=SPACEMANAGER_DB_PORT,
        help="spacemanager db port (default: %d)"% SPACEMANAGER_DB_PORT)

    
    args = parser.parse_args()

    """
    create database connection
    """

    connection = psycopg2.connect(database=args.spacemanager_dbname,
                                  user=args.spacemanager_user,
                                  host=args.spacemanager_host,
                                  port=args.spacemanager_port)

    cursor = connection.cursor('cursor_for_spacemanager_scan',
                               cursor_factory=psycopg2.extras.DictCursor)

    """
    create queue 
    """
    queue = multiprocessing.Queue(10000)
    cpu_count = multiprocessing.cpu_count()
    processes = []

    """
    start workers
    """
    total = 0 
    for i in range(cpu_count):
        worker = Worker(queue,
                        (args.spacemanager_dbname,
                         args.spacemanager_user,
                         args.spacemanager_host,
                         args.spacemanager_port),
                        (args.chimera_dbname,
                         args.chimera_user,
                         args.chimera_host,
                         args.chimera_port))
                        
        processes.append(worker)
        worker.start()


    try:
        cursor.execute(SPACEMANAGER_SCAN_QUERY)
        while True:
            res = cursor.fetchmany(10000)
            if not res:
                break 
            total+=len(res)
            for r in res:
                pnfsid = r[0]
                queue.put(pnfsid)
            with printLock:
                print_message("Processed %d"%(total,))

    except Exception, msg:
        print "Error "+str(msg)
    finally:
        if connection: connection.close()

    """
    stop workers
    """

    for i in range(cpu_count):
        queue.put(None)

    for process in processes:
        process.join()

    print_message("Finish")
