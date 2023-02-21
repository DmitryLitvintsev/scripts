#!/bin/sh

set -ue

PNFS_ROOT="/pnfs/fnal.gov"

#
# returns file name for pnfs id
#

pathfinder() {
    id=$1
    fs_path=`head -n 1 "${PNFS_ROOT}/.(pathof)($id)" 2>/dev/null`
    echo ${fs_path}
}

#
# file size
#
get_size() {
    file_size=`stat -t $1 | awk '{ print $2}' 2>/dev/null`
    echo ${file_size}
}

#
# computes the wait on the basis of file size
#
fake_wait() {
    timeout=$((${si_size}/10000000))
    sleep $timeout
}

if [ $# -lt 3 ] ;then
    exit 4
else
    command=$1
    pnfsid=$2
    filepath=$3
    shift; shift; shift;
fi


file_name=`pathfinder ${pnfsid}`

# Return codes
# Return Code         Meaning                                Pool Behaviour
#                                                Into HSM                     From HSM
# 30 <= rc < 40       User defined               Deactivates request          Reports Problem to PoolManager
# 41                  No Space Left on device    Pool Retries                 Disables Pool,  Reports Problem to PoolManager
# 42                  Disk Read I/O Error        Pool Retries                 Disables Pool,  Reports Problem to PoolManager
# 43                  Disk Write I/O Error       Pool Retries                 Disables Pool,  Reports Problem to PoolManager
# All other                                      Pool Retries                 Reports Problem to PoolManager

if [ "$command" = "get" ] ; then
    file_size=`get_size ${file_name}`
    fake_wait
    dd bs=${file_size} count=1 if=/dev/zero of=${filepath}
elif [ "$command" = "put" ] ; then
    file_size=`get_size ${filepath}`
    bfid=$(dbus-uuidgen)
    # mimick read from disk :
    cat  ${filepath} > /dev/null
    #  fake wait here ...
    fake_wait
    # echo the hsm URI
    echo "fakehsm://fakehsm/?volume=VOO001&location_cookie=0000_000000000_0000001&size=${file_size}&original_name=${file_name}&pnfsid_file=${pnfsid}"

else
    exit 0
fi
