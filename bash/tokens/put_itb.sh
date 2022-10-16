#!/bin/bash
set -e 

for i in {1..10000}; do 

    htgettoken -a fermicloud543.fnal.gov -i mu2e
    spath=`httokendecode  | jq ".scope" | sed -e "s/\"//g"  | awk '{ print $NF}' | cut -d ":" -f2`
    echo $spath
    TOKEN=`cat /run/user/8637/bt_u8637`

    echo "do curl"

    suffix=`date "+%s"`
    curl -f -k -L -H "Authorization: Bearer ${TOKEN}" -Tjunk https://fndcaitb4.fnal.gov:2880${spath}/curl.${suffix}
    curl -f -k -L -H "Authorization: Bearer ${TOKEN}"  https://fndcaitb4.fnal.gov:2880${spath}/curl.${suffix} -o curl.${suffix}
    rm -f curl.${suffix}
    curl -f -k -L -H "Authorization: Bearer ${TOKEN}" -X DELETE  https://fndcaitb4.fnal.gov:2880${spath}/curl.${suffix}
    #curl --capath /etc/grid-security/certificates -f -L -H "Authorization: Bearer ${TOKEN}" -X DELETE  https://fndcaitb4.fnal.gov:2880${spath}/curl.${suffix}

    echo "do gfal"

    export BEARER_TOKEN=`cat /run/user/8637/bt_u8637`
    suffix=`date "+%s"`
    gfal-copy -K adler32 junk  https://fndcaitb4.fnal.gov:2880${spath}/gfal.${suffix}
    gfal-rm  https://fndcaitb4.fnal.gov:2880${spath}/gfal.${suffix}

done

