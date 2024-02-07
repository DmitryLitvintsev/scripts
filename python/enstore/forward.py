#!/usr/bin/env python

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

crc_match = re.compile("[:;]c=1:[a-zA-Z0-9]{8}")
size_match = re.compile("[:;]l=[0-9]*")

printLock = multiprocessing.Lock()

BASE = 65521



def convert_0_adler32_to_1_adler32(crc, filesize):
    crc = long(crc)
    filesize = long(filesize)
    size = int(filesize % BASE)
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) &  0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    return (s2 << 16) + s1


class Checker(multiprocessing.Process):
    def __init__(self,queue,db):
        super(Checker,self).__init__()
        self.queue = queue
        self.db    = db

    def run(self):
        for data in iter(self.queue.get, None):
            f=data
            if f.bfid: 
                enstoreDB = self.db.connection()
                enstoreCursor = enstoreDB.cursor()
                q= "select f.*, v.file_family, v.label from file f, volume v where v.id=f.volume and bfid='{}'".format(f.bfid)
                enstoreCursor.execute(q)
                try: 
                    bfid_info= enstoreCursor.fetchall()[0]
                    enstoreCursor.close()
                    enstoreDB.close()
                    errors = []
                    warnings = []
                    info = []

                    if f.layer2:
                        if not f.layer2.crc :
                            warnings.append('no layer 2 crc')
                        if not f.layer2.size:
                            warnings.append('no layer 2 size')
                    if not f.layer4:
                        errors.append('no layer 4')
                    else:
                        if f.get_layer4_original_name() != bfid_info[6]:
                            warnings.append('original_name != pnfs_name0')
                        if f.get_layer4_volume() != bfid_info[25]:
                            warnings.append('layer4 volume != external_label')
                            warnings.append(bfid_info[25])
                        if f.get_layer4_size() != int(bfid_info[10]):
                            warnings.append('layer4 size != size')
                        if f.get_layer4_file_family() != bfid_info[24]:
                            warnings.append('layer4 file != family')
                        if f.get_layer4_location_cookie() != bfid_info[5]:
                            warnings.append('layer4 location_cookie != location_cookie')
                        if f.get_layer4_pnfsid() != bfid_info[7]:
                            warnings.append('layer4 pnfsid != pnfsid')
                        if f.get_layer4_bfid() != bfid_info[0]:
                            warnings.append('layer4 bfid != bfid')
                        if f.get_layer4_crc() != int(bfid_info[1]):
                            warnings.append('layer4 crc != crc')
                except Exception, msg:
                    errors.append(str(msg))
                errors_and_warnings(f.pnfsid,errors,warnings,info)


                

class Layer2:
    def __init__(self,data):
        lines = data.split('\n')
        self.crc = None
        self.size = None
        
        for l in lines:
            if not l: continue
            match = crc_match.search(l)
            if match:
                self.crc = match.group().split(":")[-1]
            match = size_match.search(l)
            if match:
                try:
                    self.size = int(match.group().split("=")[-1])
                except:
                    pass 
                    

class Layer4:
    def __init__(self,data):
        """
        VP6362
        0000_000000000_0012297
        53339
        fardet_data
        /pnfs/fnal.gov/usr/minos/fardet_data/2014-06/F00061014_0000.mdaq.root
        
        0000148F88B823494729A4D973C8AEFB79B2
        
        CDMS140314378602481
        stkenmvr213a:/dev/rmt/tps5d0n:1310250822
        235446335
        """
        lines = data.split('\n')
        self.volume = None
        self.location_cookie = None
        self.size = None
        self.file_family = None
        self.original_name = None
        self.pnfsid = None
        self.bfid = None
        self.drive = None
        self.crc = None

        try:
            self.volume = lines[0].strip()
            self.location_cookie = lines[1].strip()
            self.size = int(lines[2].strip())
            self.file_family = lines[3].strip()
            self.original_name = lines[4].strip()
            self.pnfsid = lines[6].strip()
            self.bfid = lines[8].strip()
            self.drive = lines[9].strip()
            self.crc = int(lines[10].strip())
        except:
            pass

    def  __repr__(self):
        info = """
        {}
        {}
        {}
        {}
        {}

        {}

        {}
        {}
        {}
        """
        return info.format(self.volume,
                           self.location_cookie,
                           self.size,
                           self.file_family,
                           self.original_name,
                           self.pnfsid,
                           self.bfid,
                           self.drive,
                           self.crc)
    
    

class File:
    def __init__(self, row):
        self.pnfsid=row[0]
        self.size=long(row[1])
        self.bfid=row[2] if row[2] else None
        self.layer2=Layer2(row[3]) if row[3] else None
        self.layer4=Layer4(row[4]) if row[4] else None

    def get_layer2_crc(self):
        if self.layer2:
            return self.layer2.crc
        else:
            return None

    def get_layer2_size(self):
        if self.layer2:
            return self.layer2.size
        else:
            return None

    def get_layer4_crc(self):
        if self.layer4:
            return self.layer4.crc
        else:
            return None

    def get_layer4_size(self):
        if self.layer4:
            return self.layer4.size
        else:
            return None

    def get_layer4_volume(self):
        if self.layer4:
            return self.layer4.volume
        else:
            return None

    def get_layer4_location_cookie(self):
        if self.layer4:
            return self.layer4.location_cookie
        else:
            return None

    def get_layer4_file_family(self):
        if self.layer4:
            return self.layer4.file_family
        else:
            return None

    def get_layer4_pnfsid(self):
        if self.layer4:
            return self.layer4.pnfsid
        else:
            return None

    def get_layer4_bfid(self):
        if self.layer4:
            return self.layer4.bfid
        else:
            return None

    def get_layer4_drive(self):
        if self.layer4:
            return self.layer4.drive
        else:
            return None

    def get_layer4_original_name(self):
        if self.layer4:
            return self.layer4.original_name
        else:
            return None

    
def errors_and_warnings(fname, error, warning, information):

    printLock.acquire()
    
    print fname +' ...',
    # print warnings
    for i in warning:
        print i + ' ...',
    # print errors
    for i in error:
        print i + ' ...',
    # print information
    for i in information:
        print i + ' ...',
    if error:
        print 'ERROR'
    elif warning:
        print 'WARNING'
    elif information:
        print 'OK'
    else:
        print 'OK'

    printLock.release()
         


if __name__ == "__main__":

#    csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
#                                                      enstore_functions2.default_port()))

    enstoreDbPool = PooledDB(psycopg2,
                             maxconnections=20,
                             maxcached=10,
                             blocking=True,
                             host="localhost",
                             port=8888,
                             user="enstore_reader",
                             database="enstoredb")


    pool = PooledDB(psycopg2,
                    maxconnections=1,
                    maxcached=1,
                    blocking=True,
                    host="localhost",
                    port=5432,
                    user="enstore",
                    database="chimera")

    queue = multiprocessing.Queue(10000)

    cpu_count = multiprocessing.cpu_count()
    processes = []

    for i in range(cpu_count):
        checker = Checker(queue,enstoreDbPool)
        processes.append(checker)
        checker.start()

    db = pool.connection()
    cursor = db.cursor('cursor_for_scan', cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("select t_inodes.ipnfsid, t_inodes.isize,\
                       encode(l1.ifiledata,'escape') as layer1, \
                       encode(l2.ifiledata,'escape') as layer2, \
                       encode(l4.ifiledata,'escape') as layer4 from \
                       t_inodes  left outer join t_level_4 l4 on (l4.ipnfsid=t_inodes.ipnfsid) \
                       left outer join t_level_1 l1 on (l1.ipnfsid=t_inodes.ipnfsid) \
                       left outer join t_level_2 l2 on (l2.ipnfsid=t_inodes.ipnfsid) where \
                       t_inodes.itype=32768")

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
            f = File(r)
            if f.bfid : files += 1 
            queue.put(f)
        t1 = time.time()
        t0 = t1
    cursor.close()
    db.close()

    for i in range(cpu_count):
        queue.put(None)

    for process in processes:
        process.join()


