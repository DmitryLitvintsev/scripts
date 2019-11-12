#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Supply file name to bring online." 1>&2
    exit 1
fi

curl -L  --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u`  -X POST -H "Accept: application/json" -H "Content-Type: application/json" https://fndca3a.fnal.gov:3880/api/v1/namespace${1} --data '{"action" : "qos", "target" : "disk+tape"}'
