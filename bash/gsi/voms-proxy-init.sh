#!/bin/bash

if [ $# -eq 0 ];
then
    echo "please provide one argument fermilab|sha2"
fi

case "$1" in
    "fermilab")
	voms-proxy-init -hours 120 -voms fermilab:/fermilab
	;;
    "sha2")
	voms-proxy-init -cert ${HOME}/.globus/256_usercert.pem -key ${HOME}/.globus/256_userkey.pem -hours 24 -voms sha2:/sha2
	;;
    "cms")
	voms-proxy-init -cert ${HOME}/.globus/usercert_cern.pem -key ${HOME}/.globus/userkey_cern.pem -hours 24 -rfc -voms cms
	;;
    "dteam")
	voms-proxy-init -cert ${HOME}/.globus/usercert_cern.pem -key ${HOME}/.globus/userkey_cern.pem -hours 24 -rfc -voms dteam
	;;
    *)
	;;
esac
