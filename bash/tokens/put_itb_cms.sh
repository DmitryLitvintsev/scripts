#!/bin/bash
set -e 

END_POINT="https://cmsdcadiskitb01.fnal.gov:2880"

for i in {1..10000}; do 


    htgettoken -a dwdvault.cern.ch -i cms
    httokendecode  | jq ".scope"


    spath=`httokendecode  | jq ".scope" | sed -e "s/\"//g"  | awk '{ print $1}' | cut -d ":" -f2`
    spath="${spath}/dcache/uscmsdisk/store/test/litvinse"
    TOKEN=`cat /run/user/8637/bt_u8637`

    echo "do curl"

    suffix=`date "+%s"`
    curl -s -f -k -L -H "Authorization: Bearer ${TOKEN}" -Tjunk ${END_POINT}${spath}/curl.${suffix}
    curl -s -f -k -L -H "Authorization: Bearer ${TOKEN}"  ${END_POINT}${spath}/curl.${suffix} -o curl.${suffix}
    echo "Done put/get"
    rm -f curl.${suffix}
    curl -f -k -L -H "Authorization: Bearer ${TOKEN}" -X DELETE ${END_POINT}${spath}/curl.${suffix}
    echo "Done delete"

    echo "do gfal"

    export BEARER_TOKEN=`cat /run/user/8637/bt_u8637`
    suffix=`date "+%s"`
    gfal-copy -K adler32 junk ${END_POINT}${spath}/gfal.${suffix}
    gfal-rm ${END_POINT}${spath}/gfal.${suffix}
    
    break
done

