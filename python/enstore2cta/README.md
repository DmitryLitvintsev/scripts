enstore2cta - Enstore to CTA migration script
=============================================

Requirements
------------

Script works both with python2 and python3 and requires `psycopg2` module be installed (using `pip` or `yum install python-psycopg2`).


Invocation
----------

```
$ python enstore2cta.py
usage: enstore2cta.py [-h] [--label LABEL] [--all] [--skip_locations] [--add]
                      [--storage_class STORAGE_CLASS] [--vo VO]
                      [--cpu_count CPU_COUNT]

This script converts Enstore metadata to CTA metadata. It looks for YAML
configuration file pointed to by MIGRATION_CONFIG environment variable or, if
it is not defined, it looks for file enstore2cta.yaml in current directory.
Script will quit if configuration YAML is not found.

optional arguments:
  -h, --help            show this help message and exit
  --label LABEL         comma separated list of labels (default: None)
  --all                 do all labels (default: False)
  --skip_locations      skip filling chimera locations (good for testing)
                        (default: False)
  --add                 add volume(s) to existing system, do not create vos,
                        pools, archive_routes etc. These need to pre-exist in
                        CTA db (default: False)
  --storage_class STORAGE_CLASS
                        Add storage class corresponding to volume. Needed when
                        adding single volume to existing system using --add
                        option (default: None)
  --vo VO               vo corresponding to storage_class. Needed when adding
                        single volume to existing system using --add option
                        (default: None)
  --cpu_count CPU_COUNT
                        override cpu count - number of simulateously processed
                        labels (default: 8)
                        single volume to existing system using --add option

```

The script can work with individual label(s) passed as comma separated values to `--label` option. Or it can be invoked
with `--all` switch to migrate all labels. The migratoin is done by label.

Configuration
--------------

Script expects configuration file `enstore2cta.yaml` in current directory or pointed to by environment variable `MIGRATION_CONFIG`. The yaml file has to have "0600" permission bits and has to have the following parameters defned:

```
disk_instance_name: Fermilab       # CTA dsk instance name, needs to be defined in advance in CTA
cta_db: postgresql://user:password@host:port/db_name       # CTA db connection string, needs write access
enstore_db: postgresql://user:password@host:port/db_name   # Enstore DB connection string, needs r/o access
chimera_db: postgresql://user:password@host:port/db_name   # Chimera DB connection string, needs write access
# Enstore to CTA media_type map
media_type_map:
  LTO8: LTO8
  M8: LTO7M
  LTO9: LTO9

```
