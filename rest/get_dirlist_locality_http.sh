#!/bin/sh

if [ ${1:-x} = "x" ]; then
    echo "please provide full file name" 1>&2;
    echo "usage: $0 <file_path>"
    exit 1
fi



WEBDAV_HOST=http://fndca4a.fnal.gov:8000
FILE_PATH=${1}


curl -s -L -X PROPFIND -H Depth:1 ${WEBDAV_HOST}${FILE_PATH} \
  --data '<?xml version="1.0" encoding="utf-8"?>
          <D:propfind xmlns:D="DAV:">
              <D:prop xmlns:R="http://www.dcache.org/2013/webdav"
                      xmlns:S="http://srm.lbl.gov/StorageResourceManager">
                  <R:Checksums/>
                  <S:AccessLatency/>
                  <S:RetentionPolicy/><S:FileLocality/>
              </D:prop>
          </D:propfind>' | xmllint -format -




