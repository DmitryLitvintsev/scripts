Collecton of procedures to populate srm spacemanager database
=============================================================

The procedure consists of three steps:
   1. run SQL query to generate CSV file flat list of files in the tree
   1. run python script on the CSV file to prduce one huge populate SQL file
   1. run `psql ... -f <populate sql file>

Collect data from namespace tree
---------------------------------

Edit file `file_list.sql` and replace `000500000000000000214240` with pnfsid of the top level directory of the tree. Obtain pnfsid like so. Using PNFS mount

cat "/path/to/the/tree/top/directory/under/which/the/data/is/be/".(id)(stored)"

Then run this on chimera db

```
psql -t -F ',' -A  -U <user> chimera -f file_list.sql -o files.csv

```
replace `<user>` with actual postgresql role and if necessary db name


Generate populate SQL
---------------------

```
python generate_sql.py files.csv
```

The output will be in `populate.sql`

Run the populate SQL on spacemanager DB
---------------------------------------

```
psql -U <user> postgres spacemanager  -d populate.sql
```


replace `<user>` with actual postgresql role and if necessary db name
