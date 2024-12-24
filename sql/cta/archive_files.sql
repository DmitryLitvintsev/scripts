sql -F ',' -A -t  -c "select disk_file_id, archive_file_id from archive_file where disk_file_id ~ '[0-9A-F]{36}'" -o archive_file.csv
