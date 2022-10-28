#!/usr/bin/env python

import multiprocessing
import os

import configuration_client
import enstore_functions2

printLock = multiprocessing.Lock()


class Worker(multiprocessing.Process):
    def __init__(self, name):
        super(Worker,self).__init__(name=name)
        self.csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                               enstore_functions2.default_port()))

    def run(self):
        counter = 0
        while True:
            if os.path.exists("/tmp/STOP"):
                break
            config = self.csc.dump_and_save(10, 3)
            counter =+ 1
            if not counter % 1000 :
                with printLock:
                    print("%s Processed %s" %  (self.name, counter, ))


if __name__ == "__main__":

    cpu_count = multiprocessing.cpu_count() * 10
    processes = []

    for i in range(cpu_count):
        worker = Worker("Process-%d" % (i, ))
        processes.append(worker)
        worker.start()


    for process in processes:
        process.join()
