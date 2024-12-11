Collecton of procedures to populate SRM spacemanager database
=============================================================

The procedure consists of three steps:
   1. run SQL query to generate CSV file flat list of files in the tree
   1. run python script on the CSV file to produce one huge populate SQL file
   1. run `psql ... -f <populate sql file>`

Collect data from namespace tree
---------------------------------

Edit file `file_list.sql` and replace `000500000000000000214240` with pnfsid of the top level directory of the tree. Obtain pnfsid like so. Using PNFS mount

```
cat /path/to/the/tree/top/directory/under/which/the/data/is/be/".(id)(stored)"
```
Above is for having `/path/to/the/tree/top/directory/under/which/the/data/is/be/stored` as the top of the directory tree under which the data is stoted imn the namespace 


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
psql -U <user> postgres spacemanager  -f populate.sql
```


replace `<user>` with actual postgresql role and if necessary db name

Furnish directory tag:
----------------------

You also have to do :

```
cd /path/to/the/tree/top/directory/under/which/the/data/is/be/stored
echo "5" > ".(tag)(WriteToken)"
```

Then the tag needs to propagate. For this :

```
psql -U <user> chimera
chimera# select f_push_tag(pnfsid2imumber('<pnfsid>'), 'WriteToken');
```

<pnfsid> in the above is pnfs id returned by this command:

```
cat /path/to/the/tree/top/directory/under/which/the/data/is/be/".(id)(stored)"
```

