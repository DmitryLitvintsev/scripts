#!/bin/bash

THREAD_COUNT_THRESHOLD=900

while [ 1 ];
do
    if [ -e /tmp/STOP ]; then
	exit 0
    fi
    threads=0
    t=`date  "+%Y-%m-%d %H:%M:%S"`
    dcache status 2>/dev/null  | sed -e "1,1d" | awk '{ print $1,$(NF-2)}' | while read name pid
    do
	tc=`ps -o thcount $pid | sed -e "1,1d"`
	echo "$t $name $tc"
	if [[ ${tc} -gt ${THREAD_COUNT_THRESHOLD} && ! -e /tmp/${name}.td ]]; then
	    netstat -apn
	    dcache dump threads ${name}
	    echo "Dumped threads for ${name}"
	    touch /tmp/${name}.td
	fi
    done
    total=`ps -eo thcount | tail -n +2 | awk '{ num_threads += $1 } END { print num_threads }'`
    echo "$t Total $total"
    sleep 30
done
