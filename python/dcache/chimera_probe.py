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
   NB: e-mail addressed need to be edited to tailor to your case

"""

import signal
import smtplib
import time
import os
import sys

import psycopg2

GET_RANDOM_PNFSID="""
select ipnfsid from t_inodes where itype=32768 OFFSET random()*10000 LIMIT 1
"""

GET_PATH="""
select inode2path('{}')
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
    from_addr = "enstore@localost"
    to_addr = "joe@example.com, doe@example.com"
    msg = ["From: %s"%(from_addr),
           "To: %s"%(to_addr),
           "Subject: %s"%(subject),
           ""] + [message]
    return smtplib.SMTP('localhost').sendmail(from_addr, [to_addr], '\n'.join(msg))

def wrapper(func,args=()):
    t1=time.time()
    res = func(args)
    t2=time.time()
    print_message("Took %3.2f seconds to execute %s, on %s, result %s"%(t2-t1,func.__name__, str(args), str(res)))
    return res

if __name__ == "__main__":
    timeout = 10
    if len(sys.argv)> 1:
        timeout = int(sys.argv[1])
    con = None
    cursor = None
    try:
        con = psycopg2.connect(host     = 'localhost',
                               database = 'chimera',
                               port     = 5432,
                               user     = 'enstore')
        cursor = con.cursor()
        query_time=time.time()
        cursor.execute(GET_RANDOM_PNFSID)
        res = cursor.fetchall()
        query_time = time.time() - query_time
        stat_time = time.time()
        if res and len(res) > 0 :
            cursor.execute(GET_PATH.format(res[0][0]))
            pres = cursor.fetchall()
            if pres and len(pres) > 0:
                path = pres[0][0]
                signal.signal(signal.SIGALRM, signal_handler)
                signal.alarm(timeout)
                try:
                    f_stat = wrapper(os.stat,(path))
                except Exception as e :
                    if str(e) == "TIMEDOUT":
                        send_mail("NFS SERVER TIMEOUT", "Timed out after %3.2f seconds, query time %3.2f on %s"%(time.time()-stat_time,
                                                                                                                               query_time, path))
                        send_mail("RESTARTING NFS SERVER ", "Timed out after %d seconds on %s"%(timeout, path))
                        rc=os.system("dcache dump threads nfsDomain")
                        rc=os.system("dcache restart nfsDomain")
                        if rc :
                            send_mail("FAILED RESTARTING NFS SERVER", "FAILED RESTARTING NFS SERVER")
    except Exception as e:
        print_error(str(e))
        pass
    finally:
        if cursor : cursor.close()
        if con : con.close()


