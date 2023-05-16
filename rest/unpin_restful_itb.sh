#!/bin/bash

curl -L  --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u`  -X POST -H "Accept: application/json" -H "Content-Type: application/json" https://fndcaitb4.fnal.gov:3880/api/v1/namespace${1} --data '{"action" : "unpin" }'


