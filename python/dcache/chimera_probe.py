#!/usr/bin/env python

"""
   This script:
      connects to chimera DB
      choses random PNFSID
      determines PATH
      rusn stat on that path
      if stat does not return within specified timeout - an e-mail is sent
      and NFS domain is taken thread dump of and restarted

   Input argument - timeout in seconds (optional). Deault is 10 seconds.
   NB: e-mail addresses need to be edited to tailor to your case

"""

import multiprocessing
import os
import signal
import smtplib
import sys
import time

import psycopg2

GET_RANDOM_PNFSID="""
select ipnfsid from t_inodes where itype=32768 OFFSET random()*10000 LIMIT 1
"""

GET_PATH="""
select inode2path(%s)
"""

def print_message(text):
    sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
    sys.stdout.flush()

def print_error(text):
    sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
    sys.stderr.flush()


def signal_handler(signum, frame):
    raise Exception("TIMEDOUT")

def send_mail(subject, message):
    from_addr = "enstore@localhost"
    to_addr = "joe@example.com, doe@example.com"
    msg = ["From: %s"%(from_addr),
           "To: %s"%(to_addr),
           "Subject: %s"%(subject),
           ""] + [message]
    return smtplib.SMTP('localhost').sendmail(from_addr, [to_addr], '\n'.join(msg))

printLock = multiprocessing.Lock()

def wrapper(func,args=()):
    dt=-time.time()
    res = func(args)
    dt+=time.time()
    with printLock:
        print_message("Took %3.2f seconds to execute %s, on %s, result %s"%(dt,func.__name__, str(args), str(res)))
    return res

class Worker(multiprocessing.Process):
    def __init__(self,queue):
        super(Worker,self).__init__()
        self.queue = queue

    def run(self):
        for data in iter(self.queue.get,None):
            path = data
            f_stat = wrapper(os.stat,(path))

if __name__ == "__main__":
    timeout = 10
    if len(sys.argv)> 1:
        timeout = int(sys.argv[1])
    con = None
    cursor = None
    workers = []
    try:
        con = psycopg2.connect(host     = 'localhost',
                               database = 'chimera',
                               port     = 5432,
                               user     = 'enstore')
        cursor = con.cursor()
        query_time=-time.time()
        """
        get random PNFSID
        """
        cursor.execute(GET_RANDOM_PNFSID)
        res = cursor.fetchall()
        query_time += time.time()
        if not res :
            raise Exception("Failed to find random pnfsid")

        """
        convert PNFSID to path
        """
        cursor.execute(GET_PATH,res[0])
        pres = cursor.fetchall()
        if not pres :
            raise Exception("Failed to get path for pnfsid={}".format(res[0][0]))
        path = pres[0][0]
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(timeout)

        stat_time = time.time()
        queue = multiprocessing.Queue(2)
        worker = Worker(queue);
        workers.append(worker)
        worker.start()
        queue.put(path)
        queue.put(None)
        sys.exit(0)
    except Exception as e:
        t, v, tb = sys.exc_info()
        if str(e) == "TIMEDOUT":
            send_mail("NFS SERVER TIMEOUT", "Timed out after %3.2f seconds, query time %3.2f on %s"%(time.time()-stat_time,
                                                                                                     query_time, path))
            send_mail("RESTARTING NFS SERVER ", "Timed out after %d seconds on %s"%(timeout, path))
            map(lambda x: x.terminate(), workers)
            rc=os.system("dcache dump threads nfsDomain")
            rc=os.system("dcache restart nfsDomain")
            sys.exit(1)
        else:
            print_error("Exception occured {}".format(str(e)))
            sys.exit(1)
    finally:
        map(lambda x: x.join(), workers)
        if con: con.close()
