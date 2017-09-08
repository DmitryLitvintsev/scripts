import json
import multiprocessing
import os
import urllib2
import StringIO
import socket
import subprocess
import sys
import time

import test
import pprint

printLock = multiprocessing.Lock()

def print_error(text):
    with printLock:
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
        sys.stderr.flush()


def print_message(text):
    with printLock:
        sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
        sys.stdout.flush()

def execute_command(cmd):
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    output, errors = p.communicate()
    rc=p.returncode
    if rc:
        with printLock:
            print_error("Command \"%s\" failed: rc=%d, error=%s"%(cmd,rc,errors.replace('\n',' ')))
    return rc

class Worker(multiprocessing.Process):
    def __init__(self,queue,report):
        super(Worker,self).__init__()
        self.queue = queue
        self.report = report

    def run(self):
        for t in iter(self.queue.get, None):
            rc = t.run()
            if rc :
                print_message("FAILURE for %s \n %s"%(t,t.error))
            else:
                print_message("SUCCESS for %s"%(t,))
            report[t] = { 'rc' : rc,
                          'error' : t.error }

if __name__ == "__main__":

    manager = multiprocessing.Manager()
    report = manager.dict()

    SYSTEM  = "dmsdca06"
    try:
        SYSTEM = sys.argv[1]
    except:
        pass

    url = "http://%s:2288/info/doors?format=json"%(SYSTEM,)

    request = urllib2.Request(url)
    request.add_header("Accept","application/json")
    response = urllib2.urlopen(request)
    data = json.load(response)

    """
    setup environment
    """

    os.environ["X509_USER_KEY"] = "/etc/grid-security/hostkey.pem"
    os.environ["X509_USER_CERT"] = "/etc/grid-security/hostcert.pem"
    os.environ["X509_CERT_DIR"] = "/etc/grid-security/certificates"
    os.environ["X509_USER_PROXY"] =  "/tmp/pagedcache/x509up_pagedcache"

    rc=execute_command("grid-proxy-init")
    if rc :
        print_error("Failed to create proxy")
        sys.exit(1)

    hostname = socket.gethostname()

    os.environ["KRB5CCNAME"] = "/tmp/krb5cc_enstore_pagedcache"
    cmd="kinit -k -t /etc/krb5.keytab ftp/%s"%(hostname,)

    rc=execute_command(cmd)
    if rc :
        print_error("Failed to create kerberos ticket")
        sys.exit(1)


    queue = multiprocessing.Queue(100)
    cpu_count = multiprocessing.cpu_count()
    workers = []

    for i in range(cpu_count):
        worker = Worker(queue,report)
        workers.append(worker)
        worker.start()

    for name, value in data.iteritems():
        t = test.createTest(name,value)
        if not t : continue
        queue.put(t)

    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    print_message("Finish")
    #pp = pprint.PrettyPrinter(indent=4)



