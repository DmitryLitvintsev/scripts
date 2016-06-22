#!/usr/bin/env python 

import sys
import chimera
import info_client
import configuration_client
import enstore_functions2
import string
import e_errors
import chimera
import file_utils
import dbaccess 
import copy
from optparse import OptionParser


CHIMERA_QUERY="""
SELECT t_inodes.ipnfsid,
       encode(l1.ifiledata,'escape') AS layer1,
       encode(l2.ifiledata,'escape') AS layer2,
       encode(l4.ifiledata,'escape') AS layer4
FROM t_inodes
LEFT OUTER JOIN t_level_4 l4 ON (l4.ipnfsid=t_inodes.ipnfsid)
LEFT OUTER JOIN t_level_1 l1 ON (l1.ipnfsid=t_inodes.ipnfsid)
LEFT OUTER JOIN t_level_2 l2 ON (l2.ipnfsid=t_inodes.ipnfsid)
WHERE t_inodes.ipnfsid=%s
"""

DUPLICATE_QUERY="""
SELECT count(*),
       v.label,
       f.location_cookie
FROM file f
INNER JOIN volume v ON v.id=f.volume
WHERE f.deleted='n'
  AND v.media_type NOT IN ('null',
                           'disk')
  AND v.system_inhibit_0 != 'DELETED'
  AND f.update > CURRENT_DATE - interval '7 days'
GROUP BY f.location_cookie,
         v.label
HAVING count(*)>1
"""

GET_PNFSIDS_FOR_LOCATION_COOKIE="""
SELECT bfid,
       pnfs_id
FROM file f
INNER JOIN volume v ON v.id=f.volume
WHERE f.location_cookie = %s
  AND v.label = %s
"""


def help():
    txt = "usage %prog [options] file_name"


if __name__ == "__main__":

    parser = OptionParser(usage=help())

    parser.add_option("-f", "--file",
                      metavar="FILE",type=str,default=None,
                      help="file containing query result")

    parser.add_option("-q", "--query",default=False,
                      metavar="QUERY",type=str,
                      help="print query and print  [default: %default] ")

    (options, args) = parser.parse_args()

    csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                      enstore_functions2.default_port()))

    namespaceDictionary = csc.get('namespace',None)
    
    if not e_errors.is_ok(namespaceDictionary):
        sys.stderr.write("Got error retrieving namespace dictionary from config %s\n"%(str(namespaceDictionary['status'])))
        sys.exit(1)

    del namespaceDictionary['status']

    if not namespaceDictionary:
        sys.stderr.write("No namespace dictionary in configuration root.\n")
        sys.exit(1)

    dbInfo =  csc.get("database")

    if not dbInfo: 
        sys.stderr.write("No enstorre database information")
        sys.exit(1)
        
    enstoredb = dbaccess.DatabaseAccess(maxconnections=1,
                                        host = dbInfo.get("dbhost","localhost"),
                                        port = dbInfo.get("dbport","8888"),
                                        user = dbInfo.get("dbuser","enstore"),
                                        database = dbInfo.get("dbname","enstoredb"))


    if options.file :
        with open(options.file,"r") as f:
            data = [ i.strip().split() for i in f.readlines()]
        f.closed
    else:
        data = enstoredb.query(DUPLICATE_QUERY)


    fcc = info_client.infoClient(csc)

    databases = []


    for key,value in  namespaceDictionary.iteritems():
        databases.append(dbaccess.DatabaseAccess(maxconnections=1,
                                                 host=value.get('dbhost','localhost'),
                                                 port=value.get('dbport',5432),
                                                 user=value.get('dbuser','enstore'),
                                                 database=value.get('dbname','chimera')))

    pnfsids={}
    bad_ids = set()

    for datum in data:
        if not datum : continue
        count = datum[0]
        label = datum[1]
        cookie = datum[2]
        res=enstoredb.query(GET_PNFSIDS_FOR_LOCATION_COOKIE,(cookie,label,))

        for row in res:
            pnfsid = row[1]
            bfid   = row[0]

            if pnfsid not in pnfsids:
                pnfsids[pnfsid] = { 'bfid' : None, 'label' : None, 'location_cookie' : None, 'bfids' : [] }
                theDB = None
                doContinue = True
                path=None
                bfid_l1=None
                bfid_l4=None
                pnfs_label = None
                pnfs_cookie = None

                for db in databases:
                    res = db.query("select inode2path(%s)",(pnfsid,))
                    if len(res) > 0 and res[0][0] != '':
                        theDB = db 
                        path = res[0][0]
                        layers = db.query_getresult(CHIMERA_QUERY,(pnfsid,))
                        
                        l1 = layers[0][1]
                        l4 = layers[0][3]
                        
                        if not l1 or not l4 : 
                            print "No layers", pnfsid, path
                            doContinue = False 
                            break

                        bfid_l1 = l1
                        bfid_l4 = l4.split("\n")[8]
                        pnfs_label =  l4.split("\n")[0]
                        pnfs_cookie =  l4.split("\n")[1]
                    
                        if bfid_l1 != bfid_l4:
                            print "layer 1 != layer 4", pnfsid, path
                            doContinue=False
                            break
                        print pnfsid, bfid_l4, pnfs_label, pnfs_cookie, path
                        break
                if not theDB or not doContinue: 
                    bad_ids.add(pnfsid)
                else:
                    pnfsids[pnfsid]['bfid'] = bfid_l4
                    pnfsids[pnfsid]['location_cookie'] = pnfs_cookie
                    pnfsids[pnfsid]['label'] = pnfs_label
            pnfsids[pnfsid]['bfids'].append(bfid) 


    for pnfsid, values in pnfsids.iteritems():
        if pnfsid in bad_ids:
            #print "Not doing", pnfsid, values['bfids']
             for bfid in values['bfids']:
                 enstoredb.update("update file set deleted='y' where bfid=%s",(bfid,))
             continue
        l4_bfid = values['bfid']
        bfids = filter(lambda x: x!=l4_bfid, values['bfids'])
        for bfid in bfids:
            print "Marking bfid {} , unknown".format(bfid)
            #enstoredb.update("update file set deleted='u' where bfid=%s",(bfid,))

    for db in databases:
        db.close()
    enstoredb.close()

    
