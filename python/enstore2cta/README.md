enstore2cta - Enstore to CTA migration script
=============================================

Requirements
------------

Script works both with python2 and python3 and requires `psycopg2` module be installed (using `pip` or `yum install python-psycopg2`).


Invocation
----------

```
$ python enstore2cta.py
usage: enstore2cta.py [-h] [--label LABEL] [--all]

This script converts Enstore metadata to CTA metadata. It looks for YAML
configuration file pointed to by MIGRATION_CONFIG environment variable or, if
it is not defined, it looks for file enstore2cta.yaml in current directory.
Script will quit if configuration YAML is not found.

optional arguments:
  -h, --help     show this help message and exit
  --label LABEL  comma separated list of labels
  --all          do all labels
```

The script can work with individual label(s) passed as comma separated values to `--label` option. Or it can be invoked
with `--all` switch to migrate all labels. The migratoin is done by label.

Configuration
--------------

Script expects configuration file `enstore2cta.yaml` in current directory or pointed to by environment variable `MIGRATION_CONFIG`. The yaml file has to have "0600" permission bits and has to have the following parameters defned:

```
disk_instance_name: Fermilab       # CTA dsk instance name, needs to be defined in advance in CTA
tape_pool_name: ctasystest         # CTA tape pool name, needs to be defined in advance in CTA
cta_db: postgresql://user:password@host:port/db_name       # CTA db connection string, needs write access
enstore_db: postgresql://user:password@host:port/db_name   # Enstore DB connection string, needs r/o access
chimera_db: postgresql://user:password@host:port/db_name   # Chimera DB connection string, needs write access
# map from Enstore LMs to CTA logical library names
library_map:
  CD-LTO8F1: TS4500G1
  CD-LTO8F1T: TS4500G1
  CD-LTO8G1: TS4500G1
  CD-LTO8G1T: TS4500G1
  CD-LTO8G2: TS4500G1
  CD-LTO8G2T: TS4500G1
  CTA-TESTING: TS4500G1
  TFF1-LTO9: TS4500G1
  TFF1-LTO9T: TS4500G1
  TFF2-LTO9: TS4500G1
  TFF2-LTO9M: TS4500G1
  TFF2-LTO9T: TS4500G1
# Enstore to CTA media_type map
media_type_map:
  LTO8: LTO8
  M8: LTO7M
  LTO9: LTO9

```

The media type names and logical_library_name(s) must be defined in CTA


Limitation
----------

* Script uses single tape pool defined in yaml file.

* Counts of file copies on tape volumes are not updated. This can be done by a single update query after script has completed. Not relevant for sites utilizing multiple file copies.
