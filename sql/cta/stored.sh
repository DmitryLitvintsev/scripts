#!/bin/bash

function get_path() {
    pf=`cat "/mnt/.(pathof)($1)"`
    echo $pf
}

function get_tag() {
    dn=`dirname $1`
    fn=`basename $1`
    cd ${dn}
    tag=`cat ".(tag)($2)"`
    echo $tag
}

#inumber | ihsmname | istoragegroup |            istoragesubgroup
#---------+----------+---------------+----------------------------------------
#      14 | cta      | cms           | cms11
#    1558 | cta      | cms           | cms11-MIGRATION
#    1556 | cta      | cms           | cms11-MIGRATION


#chimera=# select * from t_locationinfo where itype = 0;
# inumber | itype | ipriority |         ictime          |         iatime          | istate |                              ilocation
#---------+-------+-----------+-------------------------+-------------------------+--------+---------------------------------------------------------------------
#      20 |     0 |        10 | 2024-03-15 09:31:57.866 | 2024-03-15 09:31:57.866 |      1 | cta://cta/000018F1B7B7832B45C3AA45AD7AB03D7433?archiveid=56242042



cat $1 | awk -F ',' '{print $1,$2,$3}' | while read archive_id pnfsid inumber
do
    pf=`get_path ${pnfsid}`
    sg=`get_tag /mnt/${pf} storage_group`
    hsm=`get_tag /mnt/${pf} hsmInstance`
    ff=`get_tag /mnt/${pf} file_family`
    echo $sg $hsm $ff
    psql -U enstore chimera -c "insert into t_storageinfo (inumber, ihsmname, istoragegroup, istoragesubgroup) values (${inumber}, '${hsm}', '${sg}', '${ff}')"
    psql -U enstore chimera -c "insert into t_locationinfo (inumber, itype, ipriority, ictime, iatime, istate, ilocation) values (${inumber}, 0, 10, now(), now(), 1, '${hsm}://${hsm}/${pnfsid}?archiveid=${archive_id}')"
    echo "\sl ${pnfsid} rep set cached ${pnfsid}" | ssh -p 22224 admin@localhost
    echo "\sl ${pnfsid} st kill ${pnfsid}" | ssh -p 22224 admin@localhost
done
