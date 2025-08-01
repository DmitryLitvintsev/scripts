#!/bin/bash

cd /var/log/cta

rm -f  /tmp/`hostname -s`.writes.csv
rm -f /tmp/`hostname -s`.reads.csv

# 406  2025-05-16 01:20:11  zgrep  'File successfully transfered to disk' /var/log/cta/cta-taped-T1.log-20250418-000001.gz | jq  -r '[.local_time,.epoch_time,.tapeVid,.mountId,.tapePool] |  @csv'
# 407  2025-05-16 01:20:40  zgrep  'File successfully transfered to disk' /var/log/cta/cta-taped-T1.log-20250418-000001.gz | jq  -r '[.local_time,.epoch_time,.tapeVid,.mountId,.tapePool] |  join(,)'
# 408  2025-05-16 01:20:45  zgrep  'File successfully transfered to disk' /var/log/cta/cta-taped-T1.log-20250418-000001.gz | jq  -r '[.local_time,.epoch_time,.tapeVid,.mountId,.tapePool] |  join(",")'
#    zgrep  'File successfully read from disk'     $file | jq  -r '"\(.local_time),\(.epoch_time),\(.tapeVid),\(.mountId),\(.tapePool)"' | cut -d '.' -f1-2 >> /tmp/`hostname -s`.writes.csv
#    zgrep  'File successfully transfered to disk' $file | jq  -r '"\(.local_time),\(.epoch_time),\(.tapeVid),\(.mountId),\(.tapePool)"' | cut -d '.' -f1-2 >> /tmp/`hostname -s`.reads.csv

for file in `ls cta-taped*`
do
    zgrep  'File successfully read from disk'     $file | jq  -r  '[.local_time,.epoch_time,.tapeVid,.mountId,.tapePool] |  join(",")' | cut -d '.' -f1-2 >> /tmp/`hostname -s`.writes.csv
    zgrep  'File successfully transfered to disk' $file | jq  -r  '[.local_time,.epoch_time,.tapeVid,.mountId,.tapePool] |  join(",")' | cut -d '.' -f1-2 >> /tmp/`hostname -s`.reads.csv
done
