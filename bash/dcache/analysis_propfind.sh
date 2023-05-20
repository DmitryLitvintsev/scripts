#!/bin/bash 

function propfind()  { 
    zgrep 'PROPFIND' $1 | awk '{ print $NF}' | cut -d "=" -f2 | awk 'BEGIN {total=0; long=0} { if ( $1  >= 120000 ) { long += 1 }; total+=1} END {print total, long}'
}

echo "Date total timeouts"

for file in `ls -tr webdav-fndcadoor01Domain.access*`
do 
    stamp=`echo $file | cut -d "." -f3`
    counts=`propfind $file`
    echo "${stamp} ${counts}"
done

today=`date +"%Y-%m-%d"`
counts=`propfind webdav-fndcadoor01Domain.access`
echo "${today} ${counts}"





