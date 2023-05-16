#!/bin/bash


while [ $1 ]; do
    echo $1
    curl  -L --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET  "https://fndcaitb4.fnal.gov:3880/api/v1/namespace/$1?locality=true&checksum=true"
    shift

 curl  -L --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET  "https://fndcaitb4.fnal.gov:3880/api/v1/namespace/pnfs/fnal.gov/usr/GM2/daq/run2/offline/gm2_5125A/runs_27000/27028/gm2offline_final_46206058_27028.00237.root?locality=true&checksum=true&locations=true&qos=true&qos=true"
    
}[litvinse@fnisd1 ~]$ curl   --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET  "https://fndca1.fnal.gov:3880/api/v1/namespace/pnfs/nal.gov/usr/GM2/daq/run2/offline/gm2_5125A/runs_27000/27028/gm2offline_final_46206058_27028.00237.root?locations=true"
{
  "fileMimeType" : "application/x-root",
  "labels" : [ ],
  "size" : 2146049001,
  "creationTime" : 1626393805040,
  "locations" : [ "rw-stkendca1915-1", "rw-stkendca58a-6" ],
  "fileType" : "REGULAR",
  "pnfsId" : "00006B183E2542EC41BE877635DF3428C697",
  "nlink" : 1,
  "mtime" : 1626393840595,
  "mode" : 420
}[litvinse@fnisd1 ~]$ 

[litvinse@fnisd1 ~]$ curl --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET "https://fndca1.fnal.gov:3880/api/v1/pools/rw-stkendca1915-1" -H "accept: application/json"
{
  "name" : "rw-stkendca1915-1",
  "links" : [ "readWrite-link" ],
  "groups" : [ "readWritePools" ]
}[litvinse@fnisd1 ~]$ curl --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET "https://fndca1.fnal.gov:3880/api/v1/pools/rw-stkendca58a-6" -H "accept: application/json"
{"errors":[{"message":"rrw-stkendca58a-6","status":"404"}]}[litvinse@fnisd1 ~]$ 
[litvinse@fnisd1 ~]$ 
[litvinse@fnisd1 ~]$ curl --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET "https://fndca1.fnal.gov:3880/api/v1/pools/rw-stkendca58a-6" -H "accept: application/json"
{
  "name" : "rw-stkendca58a-6",
  "links" : [ "readWrite-link" ],
  "groups" : [ "readWritePools" ]
}[litvinse@fnisd1 ~]$ 



done
#curl  -L --capath /etc/grid-security/certificates --cert /tmp/x509up_u`id -u` --cacert /tmp/x509up_u`id -u` --key  /tmp/x509up_u`id -u` -X GET "https://dmsdca06.fnal.gov:3880/api/v1/namespace/pnfs/fnal.gov/usr/test/litvinse/world_readable/xi036804.00bfbhd0?locality=true"

#[root@fndca2a ~]#  curl  -k -X GET "https://fndca3a.fnal.gov:3880/api/v1/namespace/pnfs/fnal.gov/usr/test/litvinse/world_readable/xi036804.00bfbhd0?locality=true"
