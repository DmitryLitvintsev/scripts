enstore2cta - Enstore to CTA migration script
=============================================

Invocation
----------

```
$  python3 enstore2cta.py
{'disk_instance_name': 'Fermilab', 'tape_pool_name': 'ctasystest', 'logical_library_name': 'TS4500G1', 'cta_db': 'postgresql://user:password@host:port/db_name', 'enstore_db': 'postgresql://user:password@host:port/db_name', 'chimera_db': 'postgresql://user:password@host:port/db_name'}
usage: enstore2cta.py [-h] [--label LABEL] [--all]

optional arguments:
  -h, --help     show this help message and exit
  --label LABEL  comma separated list of labels
  --all          do all labels

```

The script can work with individual label(s) passed as comma separated values to `--label` option. Or it can be invoked
with `--all` switch to migrate all labels. The migratoin is done by label.

Configuration
--------------

Script expects configuration file `enstore2cta.yaml` in current directory or pointed to by environment variable `MIGRATION_CONFIG`. The script has to have "0600" permission bits and has to have the following parameters defned:

```
disk_instance_name: "Fermilab"   # CTA dsk instance name, needs to be defined in advance in CTA
tape_pool_name: "ctasystest"     # CTA tape pool name, needs to be defined in advance in CTA
logical_library_name: "TS4500G1" # CTA logical library name, needs to be defined in advance in CTA
cta_db: "postgresql://user:password@host:port/db_name"         # CTA db connection string, needs write access
enstore_db: "postgresql://user:password@host:port/db_name"     # Enstore DB connection string, needs r/o access
chimera_db: "postgresql://user:password@host:port/db_name"     # Chimera DB connection string, needs write access
```

Additionally the following media names need to be defined in CTA : `LTO7M', `LTO8`, `LTO9`.


Limitation
----------

At this moment all data will be associated with single `logical_library_name`. In the future mapping between
Enstor LMs and CTA logical libraries.

Script uses single tape pool defined in yaml file.

Counts of file copies on tape volumes are not updated. This can be done by a single update query after script has completed.
