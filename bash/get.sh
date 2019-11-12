#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Supply file name to copy." 1>&2
    exit 1
fi

fullpath=$1
fname=`basename ${fullpath}`

curl  -L --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` "https://fndca4a.fnal.gov:2880/${fullpath}" -o ${fname}