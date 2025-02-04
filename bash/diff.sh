#!/bin/bash

# Time Arithmetic

TIME1="2023-12-18 12:07:13"
TIME2="2023-12-28 06:09:08"

TIME1="2024-02-13 16:21:08"
TIME2="2024-02-15 20:32:07"

# Convert the times to seconds from the Epoch
SEC1=`date +%s -d "${TIME1}"`
SEC2=`date +%s -d "${TIME2}"`

echo $SEC1

# Use expr to do the math, let's say TIME1 was the start and TIME2 was the finish
DIFFSEC=$((SEC2-SEC1))

echo Start ${TIME1}
echo Finish ${TIME2}

echo Took ${DIFFSEC} seconds.

# And use date to convert the seconds back to something more meaningful
echo Took `date +"%d %H:%M:%S" -ud @${DIFFSEC}`
