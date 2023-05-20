#!/bin/bash 


function list()  { 
    zgrep 'ENC{LIST.*' $1 | grep -v Read | awk -F '=' '{print $NF}' | sed -e "s/{/ /g" | awk 'BEGIN {success=0; fail=0} { if ( $2 ==  226 ) {success += 1 } else { fail += 1 } } END  { print success+fail, fail }' 
}

echo "Date total fail"

for file in `ls -tr gridftp0-fndca4bDomain.access.*`
do 
    stamp=`echo $file | cut -d "." -f3`
    counts=`list $file`
    echo "${stamp} ${counts}"
done

today=`date +"%Y-%m-%d"`
counts=`list gridftp0-fndca4bDomain.access`
echo "${today} ${counts}"




