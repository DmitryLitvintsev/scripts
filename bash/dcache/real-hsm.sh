#!/bin/bash 

#set -e 

exec > /dev/null 2>&1 <&-

function get_location_cookie() {
python - <<EOF $1
import sys
import os
pnfsid = sys.argv[1]
pnfsid_hex = int('0x'+pnfsid, 16)
first = '%03x'%((pnfsid_hex & 0xFFF) ^ ( (pnfsid_hex >> 24) & 0xFFF),)
second = '%03x'%((pnfsid_hex>>12) & 0xFFF,)
path = os.path.join('volumes/aggwrite/cache',first, second, pnfsid)
print path
EOF
}


#get 00006319F31D42B942939CDC96EE26806317 /storage/data2/rw-pool-2/data/00006319F31D42B942939CDC96EE26806317 -si=size=52428800;new=false;stored=true;sClass=ssa_test.diskSF1T_in_LTO8G1T;cClass=-;hsm=enstore;accessLatency=NEARLINE;retentionPolicy=CUSTODIAL;path=/pnfs/fs/usr/ssa_test/CTA/small/test_1/50MB_114.data;uid=5744;gid=6209;links=0000F8917A00B90840FFAE29693B75473516 50MB_114.data;dcache://dcache/?store=&group=chimera&bfid=00006319F31D42B942939CDC96EE26806317:0000A5ACF71CD91B40C4AC47BD8C8742BFEB;enstore://enstore/?volume=common1:ssa_test.diskSF1T_in_LTO8G1T.cpio_odc&location_cookie=/volumes/aggwrite/cache/d31/806/00006319F31D42B942939CDC96EE26806317&size=52428800&file_family=diskSF1T_in_LTO8G1T&map_file=&pnfsid_file=00006319F31D42B942939CDC96EE26806317&pnfsid_map=&bfid=CDMS169766986000000&origdrive=common1:/volumes/aggwrite/cache:0&crc=786432001&original_name=/pnfs/fnal.gov/usr/ssa_test/CTA/small/test_1/50MB_114.data;;path=/pnfs/fnal.gov/usr/ssa_test/CTA/small/test_1/50MB_114.data;group=ssa_test;family=diskSF1T_in_LTO8G1T;bfid=CDMS169766986000000;volume=common1:ssa_test.diskSF1T_in_LTO8G1T.cpio_odc;location=/volumes/aggwrite/cache/d31/806/00006319F31D42B942939CDC96EE26806317; -uri=dcache://dcache/?store=&group=chimera&bfid=00006319F31D42B942939CDC96EE26806317:0000A5ACF71CD91B40C4AC47BD8C8742BFEB -pnfs=/pnfs/fs


if [ "$1" = "get" ]; then 
    pnfsid=$2
    location=$(get_location_cookie ${pnfsid})
    pool_file_path=$3
    pool_dir=`dirname ${pool_file_path}`

    bfid=`echo $5 | awk -F "=" '{ print $NF}'`
    IFS=: read -r child parent <<< ${bfid}

    package_path=`cat /pnfs/fnal.gov/".(pathof)(${parent})"`
    file_path=`cat /pnfs/fnal.gov/".(pathof)(${pnfsid})"`

    bn=`basename $file_path`
    dn=`dirname $file_path`
    location_cookie=`cat "${dn}/.(use)(4)($bn)" | sed -n '2p' | sed -e 's/^\///g'`

    export LD_PRELOAD=/usr/lib64/libpdcap.so.1
    (cd ${pool_dir} && tar --seek --record-size=512 --strip-components 5 --force-local -xf ${package_path} ${location_cookie})
    rc=$?
    unset LD_PRELOAD
    if [ $rc -eq 0 ]; then
	pnfsid_in_loc=`basename ${location_cookie}`
	if [ "${pnfsid_in_loc}" != "${pnfsid}" ]; then
            #
            # we have come across packaged files that have different PNFSID in their
            # name than their PNFSIDs. Handle those:
            #
            (cd  ${pool_dir} && mv ${pnfsid_in_loc} ${pnfsid})
	fi
	chmod 0644 ${pool_file_path}
	touch ${pool_file_path}
    else
	rm -f ${pool_file_path}
    fi
    exit $rc
else 
    exit 1
fi
exit 0

