#!/bin/bash

# satge 
curl -L  --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X POST https://fndcadoor.fnal.gov:3880/api/v1/tape/stage -H "Content-Type: application/json" -d '{"files": [ {"diskLifetime":"P1D","path":"/pnfs/fnal.gov/usr/test/litvinse/atom/disk01/g2/dmitri/xist_ttt/xi_1.root"}, {"diskLifetime":"P1D","path": "/pnfs/fnal.gov/usr/test/litvinse/atom/disk01/g2/dmitri/xist_ttt/xi_2.root"}]}'

# check 

curl -L  --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET "https://fndcadoor.fnal.gov:3880/api/v1/tape/stage/50d56fa1-68c9-4e8c-aecf-f444563b4a6a"

curl -L  --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET https://fndcadoor.fnal.gov:3880/api/v1/tape/archiveinfo -H "Content-Type: application/json" -d '{"paths": ["/pnfs/fnal.gov/usr/dteam/ftstest/IBM1L9/ftstest2/1/test01000001", "/pnfs/fnal.gov/usr/dteam/ftstest/IBM1L9/ftstest2/1/test01000002"]}'


curl -f -L  --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X POST https://fndcadoor.fnal.gov:3880/api/v1/tape/release/50d56fa1-68c9-4e8c-aecf-f444563b4a6a -H "Content-Type: application/json" -d '{"paths": ["/pnfs/fnal.gov/usr/dteam/ftstest/IBM1L9/ftstest2/1/test01000001", "/pnfs/fnal.gov/usr/dteam/ftstest/IBM1L9/ftstest2/1/test01000002"]}'
