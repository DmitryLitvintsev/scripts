#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Supply file name to query." 1>&2
    exit 1
fi

curl  -k -X GET "https://fndca3a.fnal.gov:3880/api/v1/namespace${1}?locality=true"