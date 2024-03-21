#!/bin/bash

label="VR1863M8"

psql -t -F ' ' -A -U enstore -p 8888 -h localhost enstoredb -c "select f.bfid, f.pnfs_path  from file f inner join volume v on f.volume = v.id where label = '${label}' and f.package_id = f.bfid" | while read bfid pnfs_path
do
    dn=`dirname ${pnfs_path}`
    fn=`basename ${pnfs_path}`
    echo $bfid $pnfs_path
    if [ ! -e ${dn} ]; then
	mkdir -p ${dn}
    fi
    if [ ! -e ${pnfs_path} ]; then
	psql -U enstore -p 8888 -h localhost enstoredb -c "update file set deleted = 'y' where bfid = '${bfid}'"
	enstore file --restore ${bfid}
    fi

    pnfs_id=`cat "${dn}/.(id)(${fn})"`

    psql -t -F ' ' -A -U enstore -p 8888 -h localhost enstoredb -c "select f.pnfs_id, v.storage_group, v.file_family from file f inner join volume v on f.volume = v.id where f.package_id = '${bfid}'" | while read id sg ff
    do
	psql -U enstore -h fndcaitb1 chimera -c "delete from t_locationinfo where inumber = (select inumber from t_inodes where ipnfsid = '${id}') and ilocation like 'dcache%'"
	location="dcache://dcache/?store=${sql}&group=chimera&bfid=${id}:${pnfs_id}"
	psql -U enstore -h fndcaitb1 chimera -c "insert into t_locationinfo (inumber, itype, ipriority, ictime, iatime, istate, ilocation) values \
((select inumber from t_inodes where ipnfsid='${id}'), 0, 10, now(), now(), 1, '${location}')"
	echo ${location}
    done
done
