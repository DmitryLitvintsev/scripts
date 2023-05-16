#!/bin/bash 

set -e

function get_locality() { 
    dn=$(dirname $1)
    fn=$(basename $1)
    locality=$(cat ${dn}/".(get)(${fn})(locality)")
    echo ${locality}
}

function get_checksum() { 
    dn=$(dirname $1)
    fn=$(basename $1)
    checksum=$(cat ${dn}/".(get)(${fn})(checksums)" | grep ADLER | cut -d ":" -f2)
    echo ${checksum}
}

function calculate_checksum() { 
    checksum=$(xrdadler32 $1 | awk '{ print $1}')
    echo $checksum
}

if [ $# -eq 0 ]; then
    echo "Usage: `basename $0` destination dir" 1>&2
    exit 1 
fi

DESTINATION=$1


for f in `echo *`
do
    if [ -f $f ]; 
    then
	source_checksum=$(calculate_checksum $f)
	destination="${DESTINATION}/$f"
	if [ -e ${destination} ];then 
	    dcache_checksum=$(get_checksum ${destination})
	    if [ "${dcache_checksum}" = "${source_checksum}" ];then
		continue
	    else
		rm -f ${destination}
	    fi
	fi
	cp $f ${destination}
	dcache_checksum=$(get_checksum ${destination})
	if [ "${dcache_checksum}" = "${source_checksum}" ];then
	    echo "copied $f to ${destination}, SUCCESS"
	else
	    echo "copied $f to ${destination}, FAILURE" 1>&2
	fi
    fi
done
