#!/bin/bash 

set -e

if [ $# -eq 0 ]; then
    echo "Usage: `basename $0` list of full paths" 1>&2
    exit 1 
fi

while [ $1 ]; 
do 
    dn=$(dirname $1)
    fn=$(basename $1)
    locality=$(cat ${dn}/".(get)(${fn})(locality)")
    echo $1 ${locality}
    shift 
done
