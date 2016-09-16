#!/usr/bin/env python
"""
a script that concurrently removes 1/2 of entries in each
directory under directory tree

"""

import time
import os
import sys
import threading
import Queue
import stat

printLock = threading.Lock()

def walk_and_remove(directory):
    if (os.path.isdir(directory)):
        d_stat = os.stat(directory)
        total =  (d_stat[stat.ST_NLINK]-2)//2
        counter = total
        direntries=os.listdir(directory)
        print_flag=False
        for entry in direntries:
            d=os.path.abspath(os.path.join(directory,entry))
            if os.path.isdir(d):
                walk_and_remove(d)
            else:
                if counter > 0:
                    os.unlink(d)
                    counter-=1
                else:
                    if not print_flag:
                        with printLock:
                            print "Removed ", total, " from ",directory
                        print_flag=True
                    continue
    else:
        return

class Worker(threading.Thread):
    def __init__(self,queue):
        super(Worker,self).__init__()
        self.queue = queue

    def run(self):
        for data in iter(self.queue.get, None):
            walk_and_remove(data)


if __name__ == "__main__":
    queue = Queue.Queue(10000)

    workers = []

    for i in range(20):
        worker = Worker(queue)
        workers.append(worker)
        worker.start()

    directory="/pnfs/fs/usr/test/arossi/online"
    direntries=os.listdir(os.path.abspath(directory))

    for entry in direntries:
        d=os.path.abspath(os.path.join(directory,entry))
        if (os.path.isdir(d)):
            queue.put(d)

    for i in range(len(workers)):
        queue.put(None)

    for worker in workers:
        worker.join()


