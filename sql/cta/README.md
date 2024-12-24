```
        psql -F ',' -A -t c "select disk_file_id, archive_file_id from archive_file where disk_file_id ~ '[0-9A-F]{36}'" -o archive_file.csv
        archive_file.sh archive_file.csv
        psql -U enstore chimera -f archive_file.sql
        psql -U enstore chimera -f insert.sql
        psql -F ',' -t -A -U enstore chimera -f stored.sql -o stored.csv
        stored.sh stored.csv
```