#!/usr/bin/env python 

"""
   This script works with a file containing a list of 
   file cache locations extracted from alarms

grep "does not exist in cache. It will be skipped, but please investigate" /srv2/enstore/enstore-log/enstore_alarms.txt  | awk '{print $10}' | sort -u > dne_files.txt


/volumes/aggwrite/cache/002/a73/0000AB5C4EFBD5314E0CB96D1A0A5BA73A59
/volumes/aggwrite/cache/00c/ee0/000071628D1803BC44959F6291B613EE061F
/volumes/aggwrite/cache/014/d72/00001922DFB754014D4A8947C04D6BD72D7F




"""

import sys
import info_client
import configuration_client
import enstore_functions2
import string
import e_errors
import dbaccess 
import copy
import os
import time
import re

QUERY="""
select t_inodes.ipnfsid,encode(l1.ifiledata,'escape') as layer1, encode(l2.ifiledata,'escape') as layer2, encode(l4.ifiledata,'escape') as layer4 
       from t_inodes  left outer join t_level_4 l4 on (l4.ipnfsid=t_inodes.ipnfsid) 
       left outer join t_level_1 l1 on (l1.ipnfsid=t_inodes.ipnfsid) 
       left outer join  t_level_2 l2 on (l2.ipnfsid=t_inodes.ipnfsid) 
       where t_inodes.ipnfsid=%s
"""

"""
common:nova.nova_production.cpio_odc
/volumes/aggwrite/cache/002/a73/0000AB5C4EFBD5314E0CB96D1A0A5BA73A59
19770
nova_production
/pnfs/fs/usr/nova/production/logs/01/91/12/05/01/fardet_r00021443_s18_t00_R16-03-03-prod2reco.d_v1_data.restrictedcaf.log.bz2

0000AB5C4EFBD5314E0CB96D1A0A5BA73A59

CDMS146077252300000
common:/volumes/aggwrite/cache:0
3773467572

"""

"""
cat "/pnfs/fs/usr/nova/production/logs/01/91/12/05/01/.(use)(1)(fardet_r00021443_s18_t00_R16-03-03-prod2reco.d_v1_data.restrictedcaf.log.bz2)"
"""


def get_path(pnfsid):
    path=None
    with open("/pnfs/fs/usr/.(pathof)(%s)"%(pnfsid,),"r") as fd:
        path=fd.readlines()[0].strip()
    fd.closed
    return path

def write_layer(path,text,layer):
    dirname=os.path.dirname(path)
    fname  = os.path.basename(path)
    layer_file = os.path.join(dirname,'.(use)(%s)(%s)'%(str(layer),fname))
    with open(layer_file,'w') as fd:
        fd.write(text)
    fd.closed

def write_layer_1(path,text):
    dirname=os.path.dirname(path)
    fname  = os.path.basename(path)
    layer_file = os.path.join(dirname,'.(use)(1)(%s)'%(fname))
    with open(layer_file,'w') as fd:
        fd.write(text)
    fd.closed


def write_layer_4(path,text):
    dirname=os.path.dirname(path)
    fname  = os.path.basename(path)
    layer_file = os.path.join(dirname,'.(use)(4)(%s)'%(fname))
    with open(layer_file,'w') as fd:
        #fd.write("%s\n"%(text),)
        fd.write(text)
    fd.closed

if __name__ == "__main__":
    db = dbaccess.DatabaseAccess(maxconnections=1,
                                 host     = "localhost",
                                 database = "chimera",
                                 port     = 5432,
                                 user     = "enstore")

    
    csc   = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                      enstore_functions2.default_port()))
    fcc = info_client.infoClient(csc)

    db_info = csc.get('database')

    if db_info['status'][0]  != e_errors.OK:
        print "Failed to get database info",  db_info['status'][0] 
        sys.exit(1)


    edb = dbaccess.DatabaseAccess(maxconnections=1,
                                 host     = db_info.get('dbhost'),
                                 database = db_info.get('dbname'),
                                 port     = db_info.get('dbport'),
                                 user     = db_info.get('dbuser'))

    with open("dne_files.txt","r") as f:
        pnfsids=[ x.strip().split("/")[-1] for x in f.readlines()]
    f.closed

    for pnfsid in pnfsids:
        f_info = fcc.find_file_by_pnfsid(pnfsid)
        if not f_info['status'][0]  in (e_errors.OK, e_errors.TOO_MANY_FILES):
            print "Failed to retrieve bfid ", pnfsid, f_info['status']
            continue
        """
        determine path
        """
        path=get_path(pnfsid)

        res = db.query_getresult(QUERY,(pnfsid,))
        if len(res) < 1:
            print "pnfsid not found in public dcache chimera", pnfsid
            continue
        l1 = res[0][1]
        l4 = res[0][3]
        
        if not l1 or not l4 : 
            print "No layers", pnfsid
            continue
                
        bfid_l1 = l1
        bfid_l4 = l4.split("\n")[8]

        if bfid_l1 != bfid_l4:
            print "layer 1 != layer 4", pnfsid
            continue

        bfid_infos=f_info['file_list']

        bfids_stored=filter(lambda x : x['deleted']=='no' and 
                            x['tape_label'].find("common") == -1, bfid_infos)
        bfids_not_stored=filter(lambda x : x['deleted']=='no' and 
                                x['tape_label'].find("common") != -1, bfid_infos)

        if len(bfids_stored) == 0 :
            print "File is not stored",pnfsid
            continue

        """
        check if any of stored bfids correspond to layer bfid
        """

        bfids_stored_checked = filter(lambda x : x['bfid'] == bfid_l1, bfids_stored)
        bfids_not_stored_checked = filter(lambda x : x['bfid'] == bfid_l1, bfids_not_stored)
        
        print pnfsid, len(bfids_stored_checked), len(bfids_not_stored_checked)

        if len(bfids_stored_checked) == 1:
            """
            we have a bfid that is in l1 and it is good, mark the rest of them unknown
            """
            bfids_to_mark_unknown=filter(lambda x : x['deleted']=='no' and x['bfid'] != bfids_stored_checked[0]['bfid'], bfid_infos)
            for bfid_info in bfids_to_mark_unknown:
                edb.update("update file set deleted='u' where bfid=%s",(bfid_info['bfid'],))
        
        elif  len(bfids_stored_checked) == 0:
            """
            Choose a bfid we will retain
            """
            chosen_bfid=bfids_stored[0]['bfid']
            l4_doctored=re.sub(bfid_l1,chosen_bfid,l4)
            write_layer_4(path,l4_doctored)
            write_layer_1(path,chosen_bfid)
            """
            mark the rest of the bfid unknown
            """
            bfids_to_mark_unknown=filter(lambda x : x['deleted']=='no' and x['bfid'] != chosen_bfid, bfid_infos)
            for bfid_info in bfids_to_mark_unknown:
                print "marking ", bfid_info['bfid'], " unknown", pnfsid
                edb.update("update file set deleted='u' where bfid=%s",(bfid_info['bfid'],))
                
            for bfid_info in bfids_not_stored:
                print "deleting ", bfid_info['bfid'], "from files in transition ", pnfsid
                edb.update("delete from files_in_transition where bfid=%s",(bfid_info['bfid'],))
        else:
            print "How did that happen, we have more than one stored identical bfid", pnfsid
            continue

    edb.close()
    db.close()

    
