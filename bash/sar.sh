#!/bin/bash 
##############################################################################
#
#  This script collects data from SAR files
#  for plotting.
#  Argument - a number (of days to collect statstics, 1 - for today)
#  No argument - collect all statistics for a month
#
###############################################################################

SAR_DIR=/var/log/sa/

if [ $# -eq 0 ]; 
then
    files=`ls  -1 -rt ${SAR_DIR} | grep -v sar`
else
    files=`ls  -1 -rt ${SAR_DIR} | grep -v sar | tail -n "${1}"`
fi

HOST=`hostname -s`

for name in load iowait network swap memory io; do 
    rm -f sar_${HOST}_${name}.data
    touch sar_${HOST}_${name}.data
done

export LC_TIME="POSIX"

i=`echo "${files}" | wc -w`
for f in $files; do
  i=$((i-1))
  day=`date --date="${i} days ago" +"%Y-%m-%d"`
  sar -f ${SAR_DIR}/$f -q | sed -e "/^$/d" | egrep  "^[0-9]+" |sed "1,1d" | sed -e "s/^/${day} /g"   >> sar_${HOST}_load.data
  sar -f ${SAR_DIR}/$f    | sed -e "/^$/d" | egrep  "^[0-9]+" |sed "1,1d" | sed -e "s/^/${day} /g"   >> sar_${HOST}_iowait.data
  sar -f ${SAR_DIR}/$f -n DEV | grep eth2  | egrep  "^[0-9]+" |sed "1,1d" | sed -e "s/^/${day} /g"   >> sar_${HOST}_network.data
  sar -f ${SAR_DIR}/$f -b | sed -e "/^$/d" | egrep  "^[0-9]+" | sed '1,1d'  | sed -e "s/^/${day} /g" >> sar_${HOST}_io.data
  sar -f ${SAR_DIR}/$f -r | sed -e "/^$/d" | egrep  "^[0-9]+" | sed '1,1d'  | sed -e "s/^/${day} /g" >> sar_${HOST}_memory.data
  sar -f ${SAR_DIR}/$f -S | sed -e "/^$/d" | egrep  "^[0-9]+" | sed '1,1d'  | sed -e "s/^/${day} /g" >> sar_${HOST}_swap.data
  sar -f ${SAR_DIR}/$f -v | sed -e "/^$/d" | egrep  "^[0-9]+" | sed '1,1d'  | sed -e "s/^/${day} /g" >> sar_${HOST}_fd.data
done


