#!/bin/bash

nodes="dcaspark01 dcaspark02 dcaspark03 dcaspark04 dcaspark05 dcaspark06 dcaspark07 dcaspark08 dcaspark09 dcaspark10 dcaspark11 dcaspark12 dcaspark13 dcaspark14 dcaspark15 dcaspark16"

all="${nodes}"

command=${1}

ssh_loop() {
    pssh -l root --host "${all}"  -i  --inline-stdout -O ConnectTimeout=5 -O StrictHostKeyChecking=no -t 300 -p 50 "$@"
}

case $command in

    stop)
	ssh_loop "/opt/spark-3.3.0-bin-hadoop3/sbin/stop-worker.sh"
	;;

    start)
	ssh_loop "/opt/spark-3.3.0-bin-hadoop3/sbin/start-worker.sh spark://storagedev202:7077"
	;;
    copy)
	if [ $# -ne 2 ]; then
	    echo "Must specify RPM to deploy" 1>&2
	    exit 1
	fi
	scp_loop ${2}
	;;

    execute)
	shift
	ssh_loop "$@"
    ;;

esac
