#!/bin/bash

DOOR="fndcaitb4.fnal.gov"
VAULT="htvaultprod.fnal.gov"
VO="dune"

USER_ID=$(id -u)
USER_NAME="litvinse"

ROOT_PATH="/pnfs/fnal.gov/usr"
WRITE_TOKEN_PATH="/${VO}/scratch/users/${USER_NAME}"
WRITE_GSI_PATH="/test/${USER_NAME}/scratch"
BEARER_TOKEN_FILE="${XDG_RUNTIME_DIR:-/tmp}/bt_u`id -u`"

function get_token() { 
    htgettoken -a ${VAULT} -i ${VO} 
}

function get_proxy() { 
    cat passwd | voms-proxy-init -hours 120 -voms fermilab:/fermilab -pwstdin  >/dev/null 2>&1
}



function token_transfer() {
    PROTOCOL=$1
    PORT=$2
    SUFFIX=`date "+%s%N"`
    xrdcp /etc/fstab ${PROTOCOL}://${DOOR}:${PORT}${ROOT_PATH}${WRITE_TOKEN_PATH}/junk_token.${SUFFIX}
    rc=$?
    echo "Done token write on ${PROTOCOL}, port=${PORT}, $rc"
    if [ $rc -eq 0 ]; then
	xrdcp -f ${PROTOCOL}://${DOOR}:${PORT}${ROOT_PATH}${WRITE_TOKEN_PATH}/junk_token.${SUFFIX} /dev/null
	echo "Done token read on ${PROTOCOL}, port=${PORT}, $rc"
    else
	echo "Skipped token read ${PROTOCOL}, port=${PORT}"
    fi
}

function gsi_transfer() {
    PROTOCOL=$1
    PORT=$2
    SUFFIX=`date "+%s%N"`
    xrdcp /etc/fstab ${PROTOCOL}://${DOOR}:${PORT}${ROOT_PATH}${WRITE_GSI_PATH}/junk_gsi.${SUFFIX}
    rc=$?
    echo "Done gsi write on ${PROTOCOL}, port=${PORT}, $rc"
    if [ $rc -eq 0 ]; then
	xrdcp -f ${PROTOCOL}://${DOOR}:${PORT}${ROOT_PATH}${WRITE_GSI_PATH}/junk_gsi.${SUFFIX} /dev/null
	echo "Done gsi read on ${PROTOCOL}, port=${PORT}, $rc"
    else
	echo "Skipped gsi read ${PROTOCOL}, port=${PORT}"
    fi
}

# make sure proxy destroyed 
voms-proxy-destroy 2>/dev/null

# token
get_token

token_transfer xroots 1094
token_transfer xroot 1094

# destroy token 
rm -f ${BEARER_TOKEN_FILE}

# gsi 

get_proxy
gsi_transfer xroots 1094
gsi_transfer xroot 1094

# make sure proxy destroyed 
voms-proxy-destroy 2>/dev/null

# token
get_token
token_transfer xroots 1096
token_transfer xroot 1096

# destroy token 
rm -f ${BEARER_TOKEN_FILE}

# gsi 

get_proxy
gsi_transfer xroots 1096
gsi_transfer xroot 1096

# make sure proxy destroyed                                                                                                                    
voms-proxy-destroy 2>/dev/null

exit 0

