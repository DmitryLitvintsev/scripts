#!/bin/bash

cat > insert.sql <<EOF
insert into 
archive_file
(ipnfsid, archive_file_id) 
VALUES
EOF

echo $1

cat $1 | awk -F ',' '{ print $1,$2}' | while read pnfsid arvhiveid; do
    echo -n "('${pnfsid}',${arvhiveid})," >> insert.sql
done
