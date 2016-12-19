#!/usr/bin/env python

import httplib2
import StringIO
import json
import Queue
import sys
import string
import threading

"""
this scrit queries store queues on each pool
and aggregates them by storage group per pool group
"""

SERVER_NAME      = "example.org"
POOL_MANAGER_URL = "http://%s:2288/api/PoolManager"%(SERVER_NAME,)
POOL_QUEUE_URL   = "http://"+SERVER_NAME+":2288/api/%s/queue"
HSM_ATTACHED_COMMON_POOLGROUPS = ("readWritePools", "ArchivePools")
NUMBER_OF_THREADS = 50

lock=threading.Lock()

class Worker(threading.Thread):

    def __init__(self,queue):
        super(Worker,self).__init__()
        self.queue = queue
        self.client = httplib2.Http()


    def run(self):
        for data in iter(self.queue.get, None):
            poolName = data[0]
            pool_map = data[1]
            url = POOL_QUEUE_URL%(poolName,)
            resp, content = self.client.request(POOL_QUEUE_URL%(poolName,), "GET")
            if resp['status'] != "200": continue
            try:
                f = StringIO.StringIO(content)
                json_data = json.load(f)
                f.close()
                flushInfos = []
                for info in json_data.get("flushInfos"):
                    storage_group = info["storageClass"].split(".")[0]
                    flushInfos.append({"active" : int(info["activeCount"]),
                                       "total"  : int(info["requestCount"]),
                                       "size"   : int(info["totalPendingFileSize"]),
                                       "failed" : int(info["failedRequestCount"]),
                                       "storage_group" : storage_group})
                with lock:
                    pool_map[poolName]['stores'] = flushInfos[:]
            except:
                pass

if __name__ == "__main__":

    h = httplib2.Http()
    resp, content = h.request(POOL_MANAGER_URL, "GET")


    f = StringIO.StringIO(content)
    json_data = json.load(f)
    f.close()

    pools =  json_data['psu']['pools']
    pool_group_map = {}

    queue = Queue.Queue(1000)

    workers = []
    for i in range(NUMBER_OF_THREADS):
        worker = Worker(queue)
        workers.append(worker)
        worker.start()

    for pool,data in pools.iteritems():
        if len(data['poolGroupsMemberOf']) == 0 : continue
        pool_group = data['poolGroupsMemberOf'][0].split()[0].strip()
        if pool_group not in HSM_ATTACHED_COMMON_POOLGROUPS : continue
        pool_group_map[pool] = { 'pgroup' : pool_group, 'stores' : []  }
        queue.put([pool,pool_group_map])

    for i in range(NUMBER_OF_THREADS):
        queue.put(None)

    for worker in workers:
        worker.join()

    pool_group_summary = {}

    for pool, data in  pool_group_map.iteritems():
        pgroup = data['pgroup']
        if pgroup not in pool_group_summary:
            pool_group_summary[pgroup] = { 'stores' : {}  }
        for i in data['stores']:
            storage_group = i['storage_group']
            if storage_group not in  pool_group_summary[pgroup]['stores']:
                 pool_group_summary[pgroup]['stores'][storage_group] = {'active' : 0,
                                                                        'total'  : 0,
                                                                        'size'   : 0,
                                                                        'failed' : 0,
                                                                        }
            pool_group_summary[pgroup]['stores'][storage_group]['active'] += i['active']
            pool_group_summary[pgroup]['stores'][storage_group]['total'] += i['total']
            pool_group_summary[pgroup]['stores'][storage_group]['failed'] += i['failed']
            pool_group_summary[pgroup]['stores'][storage_group]['size'] += i['size']

    f=open('dcache_stores.json','w')
    f.write(json.dumps(pool_group_summary, indent=4,sort_keys=True))
    f.close()





