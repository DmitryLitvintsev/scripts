#!/usr/bin/env python

"""
This script generates time series of data in directory 
identified by pnfsid 

"""


QUERY="""
WITH RECURSIVE paths(pnfsid, path, type, fsize, mtime)
  AS (
  VALUES (%s,
          '',
          16384,
          0::BIGINT,
          now())
  UNION
  SELECT d.ipnfsid,
         path||'/'||d.iname,
         i.itype,
         i.isize,
         i.imtime
  FROM t_dirs d,
       t_inodes i,
       paths p
  WHERE p.TYPE=16384
    AND d.iparent=p.pnfsid
    AND d.iname != '.'
    AND d.iname != '..'
    AND i.ipnfsid=d.ipnfsid)
SELECT SUM (p.fsize::bigint) as total,
  count(*),
  date_trunc('day',mtime) as day
FROM paths p
WHERE p.TYPE=32768
GROUP BY day
ORDER BY day asc
"""


import time
import os
import string
import sys
import psycopg2

from optparse import OptionParser

def help():
    txt = "usage %prog [options] pnfsid"

if __name__ == "__main__":

    parser = OptionParser(usage=help())

    parser.add_option("-p", "--pnfsid",
                      metavar="PNFSID",type=str,default="000046208B08903B491AA4D25F2B54296B6D",
                      help="pnfsid of the directory")

    
    (options, args) = parser.parse_args()

    f=open("/tmp/gnuplot.data","w")

    conn = None 
    cursor = None
    fail = False
    try: 
        conn = psycopg2.connect(database='chimera', user='enstore', host='localhost')
        cursor = conn.cursor()
        cursor.execute(QUERY,(options.pnfsid,))
        res = cursor.fetchall()
        total = 0 
        count = 0 
        for row in res:
            t = row[-1]
            bytes = int(row[0])
            files = int(row[1])
            total+=bytes
            count+=files
            f.write("%s %d %d %d %d\n"%(t,bytes,total,files,count))
    except Exception, msg:
        print msg
        fail = True
    finally:
        if cursor : cursor.close()
        if conn   : conn.close()
    f.close()

    if fail : 
        sys.exit(1)

    

    BODY="""
set terminal svg size 600,400 dynamic enhanced fname 'arial'  fsize 10 
set output 'unmerged.svg'
set title 'sum of files sizes in unmerged directory'
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by 90
set y2tics border
set mxtics 2
set ylabel "sum(size)/day [GiB/day]"
set y2label "cumulative [TiB]"
set xlabel "date"
set xdata time
set grid
set key top left 
set timefmt "%Y-%m-%d %H:%M:%S"
set format x '%Y-%m-%d'
plot '/tmp/gnuplot.data' using 1:($3/1024./1024./1024.) with impulses lw 5 t 'per day', '' using 1:($4/1024./1024./1024./1024.) axes x1y2 with lines lw 2 t 'cumulative'
"""
    with open("/tmp/gnuplot.cmd","w") as f :
        f.write(BODY)
    f.closed

    rc=os.system("gnuplot /tmp/gnuplot.cmd")
    os.unlink("/tmp/gnuplot.cmd")    
    os.unlink("/tmp/gnuplot.data")
    sys.exit(0)



    
    
    

    
