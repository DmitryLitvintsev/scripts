#!/bin/env python
from __future__ import print_function
import argparse
import errno
import multiprocessing
import os
import re
import socket
import stat
import subprocess
import sys
import time
import uuid
import traceback
import psycopg2
import psycopg2.extras
import datetime
import getpass
import yaml

try:
    import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse


DISK_INSTANCE_NAME = "eosdev"
TAPE_POOL_NAME = "ctasystest"

CONFIG_FILE = os.getenv("MIGRATION_CONFIG")
if not CONFIG_FILE:
    CONFIG_FILE = "enstore2cta.yaml"

HOSTNAME = socket.getfqdn()

SELECT_STORAGE_CLASSES = """
select distinct storage_group||'.'||file_family||'@cta' as storage_class
from volume
  where active_files>0 and
        system_inhibit_0 = 'none' and
        library not like 'shelf%' and
        file_family not like '%_copy_1' and
        file_family not like '%-MIGRATION' and
        file_family not like '%-MIGRATION2'
"""

SELECT_VOS = """
select distinct storage_group from volume
  where active_files>0 and
        system_inhibit_0 = 'none' and
        library not like 'shelf%' and
        file_family not like '%_copy_1' and
        file_family not like '%-MIGRATION' and
        file_family not like '%-MIGRATION2'
"""

#
# pick up only "primary" volmes that do nopt have
# '_copy_1' suffix in file_family name
#

SELECT_ALL_ENSTORE_VOLUMES = """
select label from volume
  where media_type in ('LTO8', 'M8', 'LTO9') and
        system_inhibit_0 = 'none' and
        library not like 'shelf%' and
        file_family not like '%_copy_1' and
        file_family not like '%-MIGRATION' and
        file_family not like '%-MIGRATION2' and
        active_files > 0
        order by label asc
"""

SELECT_ENSTORE_FILES_FOR_VOLUME = """
select f.*, v.storage_group||'.'||v.file_family||'@cta' as storage_class
from file f inner join volume v
  on v.id = f.volume
  where
        v.media_type in ('LTO8', 'M8', 'LTO9') and
        v.system_inhibit_0 = 'none' and
        v.label = %s and
        v.active_files > 0 and
        f.deleted = 'n'
        order by f.location_cookie
"""

SELECT_ENSTORE_FILES_FOR_VOLUME_WITH_COPY = """
select f.*,
       v.storage_group||'.'||v.file_family||'@cta' as storage_class,
       f1.bfid as copy_bfid,
       f1.location_cookie as copy_location_cookie,
       f1.deleted as copy_deleted,
       v1.*
from file f
inner join volume v on v.id = f.volume
left outer join file_copies_map fcm on fcm.bfid = f.bfid
left outer join file f1 on f1.bfid = fcm.alt_bfid
left outer join volume v1 on v1.id = f1.volume
  where
        v.media_type in ('LTO8', 'M8', 'LTO9') and
        v.system_inhibit_0 = 'none' and
        v.label = %s and
        v.active_files > 0 and
        (f1.deleted is null or f1.deleted = 'n') and
        f.deleted = 'n'
        order by f.pnfs_id
"""

# Enstore to CTA media_type map.
# Entries in CTA are expected to exist.
media_type_map = {
    "LTO8" : "LTO8",
    "M8" : "LTO7M",
    "LTO9" : "LTO9"
}

printLock = multiprocessing.Lock()

def print_error(text):
    """
    Print text string to stderr prefixed with timestamp
    and ERROR keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    with printLock:
        sys.stderr.write(time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time()))+" ERROR : " + text + "\n")
        sys.stderr.flush()


def print_message(text):
    """
    Print text string to stdout prefixed with timestamp
    and INFO keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    with printLock:
        sys.stdout.write(time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time()))+" INFO : " + text + "\n")
        sys.stdout.flush()


def extract_file_number(location_cookie):
    return int(location_cookie.split("_")[2])

# create DB connection from URI
def create_connection(uri):
    result = urlparse.urlparse(uri)
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port)
    return connection


CRC_SWITCH = '2019-08-21 09:54:26'

def get_switch_epoch():
    """
    Timestamp when the change from 0 to 1 based adler checksum happened
    """
    time_format = '%Y-%m-%d %H:%M:%S'
    os.environ['TZ'] = 'America/Chicago'
    epoch = int(time.mktime(time.strptime(CRC_SWITCH, time_format)))
    return epoch

def convert_0_adler32_to_1_adler32(crc, filesize):
    BASE = 65521
    size = filesize % BASE
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) & 0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    new_adler = (s2 << 16) + s1
    return new_adler


ISERT_VO="""
insert into virtual_organization (
  virtual_organization_id,
  virtual_organization_name,
  read_max_drives,
  write_max_drives,
  max_file_size,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time,
  disk_instance_name
) values (
  (select nextval('virtual_organization_id_seq')),
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s)
"""

def insert_vos(enstore_db, cta_db, disk_instance_name=DISK_INSTANCE_NAME):
    vos = select(enstore_db,
                 SELECT_VOS)
    for row in vos:
        vo = row["storage_group"]
        res = insert(cta_db,
                     ISERT_VO,
                     (vo,
                      2, #FIXME read_max_drives
                      2, #FIXME write_max_drives
                      10*(1<<40), # 10 TB
                      "Imported from Enstore",
                      getpass.getuser(),
                      HOSTNAME,
                      int(time.time()),
                      getpass.getuser(),
                      HOSTNAME,
                      int(time.time()),
                      disk_instance_name))

INSERT_STORAGE_CLASS = """
insert into storage_class (
  storage_class_id,
  storage_class_name,
  nb_copies,
  virtual_organization_id,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time
) values (
  (select nextval('storage_class_id_seq')),
  %s,
  %s,
  (select virtual_organization_id from virtual_organization where virtual_organization_name = %s),
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""

def insert_storage_classes(enstore_db, cta_db):
    classes = select(enstore_db, SELECT_STORAGE_CLASSES)
    for row in classes:
        storage_class = row["storage_class"]
        vo = storage_class.split(".")[0]
        res = insert(cta_db,
                     INSERT_STORAGE_CLASS,
                     (storage_class,
                      1, #FIXME nb_copies
                      vo,
                      "Imported from Enstore",
                      getpass.getuser(),
                      HOSTNAME,
                      int(time.time()),
                      getpass.getuser(),
                      HOSTNAME,
                      int(time.time())))


INSERT_ARCHIVE_FILE="""
insert into archive_file (
  archive_file_id,
  disk_instance_name,
  disk_file_id,
  disk_file_uid,
  disk_file_gid,
  size_in_bytes,
  checksum_blob,
  checksum_adler32,
  storage_class_id,
  creation_time,
  reconciliation_time,
  is_deleted,
  collocation_hint
) values (
  (select nextval ('archive_file_id_seq')),
  (select disk_instance_name from disk_instance where disk_instance_name = %s),
  %s,
  %s,
  %s,
  %s,
  null,
  %s,
  (select storage_class_id from storage_class where storage_class_name = %s),
  %s,
  %s,
  %s,
  null
)
"""

INSERT_TAPE_FILE="""
insert into tape_file (
  vid,
  fseq,
  block_id,
  logical_size_in_bytes,
  copy_nb,
  creation_time,
  archive_file_id
) values (
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""

def insert_cta_file(connection, enstore_file, cta_label, config):
    file_create_time = int(enstore_file["bfid"][4:14])
    file_size = enstore_file["size"]
    file_crc = enstore_file["crc"]
    #
    # take care of "adler32 seeed 0" nonsense
    #
    if file_create_time < get_switch_epoch() and HOSTNAME.endswith(".fnal.gov"):
        file_crc =  convert_0_adler32_to_1_adler32(file_crc, file_size)

    cta_file = insert_returning(connection,
                                INSERT_ARCHIVE_FILE,(
                                    config.get("disk_instance_name"),
                                    enstore_file["pnfs_id"],
                                    enstore_file["uid"],
                                    enstore_file["gid"],
                                    file_size,
                                    file_crc,
                                    enstore_file["storage_class"],
                                    file_create_time,
                                    int(time.time()),
                                    '0'
                                ))
    archive_file_id = int(cta_file["archive_file_id"])
    res = insert(connection,
                 INSERT_TAPE_FILE, (
                     cta_label,
                     extract_file_number(enstore_file["location_cookie"]),
                     extract_file_number(enstore_file["location_cookie"]),
                     file_size,
                     1,
                     file_create_time,
                     archive_file_id))
    return archive_file_id

def insert_cta_tape_file_copy(connection,
                              archive_file_id,
                              enstore_file,
                              config):
    file_create_time = int(enstore_file["copy_bfid"][4:14])

    res = insert(connection,
                 INSERT_TAPE_FILE, (
                     enstore_file["label"][:6],
                     extract_file_number(enstore_file["copy_location_cookie"]),
                     extract_file_number(enstore_file["copy_location_cookie"]),
                     enstore_file["size"],
                     2, # copy number
                     file_create_time,
                     archive_file_id))

INSERT_CTA_TAPE = """
insert into tape (
   vid,  media_type_id, vendor, logical_library_id, tape_pool_id,
   encryption_key_name, data_in_bytes, last_fseq, nb_master_files,
   master_data_in_bytes, is_full, is_from_castor, dirty,
   nb_copy_nb_1, copy_nb_1_in_bytes,  nb_copy_nb_gt_1,
   copy_nb_gt_1_in_bytes, label_format, label_drive, label_time,
   last_read_drive, last_read_time, last_write_drive, last_write_time,
   read_mount_count, write_mount_count, user_comment,
   tape_state, state_reason, state_update_time, state_modified_by,
   creation_log_user_name, creation_log_host_name, creation_log_time,
   last_update_user_name, last_update_host_name, last_update_time,
   verification_status)
   values (%s,
           (select media_type_id from media_type where media_type_name = %s),
           'Unknown',
           (select logical_library_id from logical_library where logical_library_name = %s),
           (select tape_pool_id from tape_pool where tape_pool_name = %s),
           '',
           %s,
           %s,
           %s,
           %s,
           '0',
           '0',
           '0',
           %s,
           %s,
           0,
           0,
           '2',
           'Enstore',
           %s,
           '',
           %s,
           'Enstore',
           %s,
           %s,
           %s,
           %s,
           'ACTIVE',
           'Migrated from Enstore',
           %s,
           %s,
           %s,
           %s,
           %s,
           %s,
           %s,
           %s,
           ''
   )
"""

def insert_cta_tape(connection, enstore_volume, config):
    res = insert(connection,
                 INSERT_CTA_TAPE,(
                     enstore_volume["label"][:6],
                     config.get("media_type_map")[enstore_volume["media_type"]],
                     config.get("library_map")[enstore_volume["library"]],
                     config.get("tape_pool_name"), #FIXME
                     enstore_volume["active_bytes"],
                     extract_file_number(enstore_volume["eod_cookie"]) - 1,
                     enstore_volume["active_files"],
                     enstore_volume["active_bytes"],
                     enstore_volume["active_files"],
                     enstore_volume["active_bytes"],
                     int(time.mktime(enstore_volume["declared"].timetuple())),
                     int(time.mktime(enstore_volume["last_access"].timetuple())),
                     int(time.mktime(enstore_volume["last_access"].timetuple())),
                     min(enstore_volume["sum_rd_access"], enstore_volume["sum_mounts"]),
                     min(enstore_volume["sum_wr_access"], enstore_volume["sum_mounts"]),
                     ("Migrated from Enstore: %s" % (enstore_volume["comment"],))[:1000],
                     int(time.time()),
                     getpass.getuser(),
                     getpass.getuser(),
                     HOSTNAME,
                     int(time.time()),
                     getpass.getuser(),
                     HOSTNAME,
                     int(time.time())
                     ))
    return res


SELECT_CTA_LOCATION = """
select 'cta://cta/'||af.disk_file_id||'?archiveid='||af.archive_file_id as
location from archive_file af
        INNER JOIN tape_file tf on tf.archive_file_id = af.archive_file_id
  WHERE
      af.disk_file_id = %s
"""


def get_cta_location(connection, enstore_file):
    location  = select(connection,
                       SELECT_CTA_LOCATION,
                       (enstore_file["pnfs_id"],))
    if location:
        return location[0]["location"]
    else:
        return None


INSERT_CHIMERA_LOCATION="""
insert into t_locationinfo (inumber, itype, ipriority, ictime, iatime, istate, ilocation)
   values (
   (select inumber from t_inodes where ipnfsid = %s),
   0,
   10,
   now(),
   now(),
   1,
   %s)
"""


def insert_chimera_location(connection, enstore_file, location):
    res = insert(connection,
                 INSERT_CHIMERA_LOCATION,
                 (enstore_file["pnfs_id"],
                  location,))
    return res


class Worker(multiprocessing.Process):
    """
    Class that processed individual enstore volume
    """
    def __init__(self, queue, config):
        super(Worker, self).__init__()
        self.queue = queue
        self.config = config

    def run(self):
        try:
            # enstroe db
            enstore_db = create_connection(self.config.get("enstore_db"))
            # cta db
            cta_db =  create_connection(self.config.get("cta_db"))
            # chimera_db
            chimera_db = create_connection(self.config.get("chimera_db"))

            added_copy_volumes = set()
            for label in iter(self.queue.get, None):
                cta_label = label[:6]
                print_message("Doing label %s" % (label, ))
                enstore_volumes = select(enstore_db,
                                         "select * from volume where label=%s",
                                         (label,))
                if not enstore_volumes:
                    print_error("No such volume %s" % (label, ))
                    continue
                enstore_volume = enstore_volumes[0]
                try:
                    res = insert_cta_tape(cta_db, enstore_volume, self.config)
                except Exception as e:
                    print_error("%s already exist, skipping, %s " %
                                (enstore_volume["label"], str(e)))
                    continue
                files = select(enstore_db,
                               SELECT_ENSTORE_FILES_FOR_VOLUME_WITH_COPY,
                               (label, ))
                for f in files:
                    try:
                        archive_file_id = insert_cta_file(cta_db,
                                                          f,
                                                          cta_label,
                                                          self.config)
                        #
                        # do we have a copy
                        #
                        copy_label = f.get("label")
                        if copy_label:
                            if copy_label not in added_copy_volumes:
                                added_copy_volumes.add(copy_label)
                                try:
                                    res = insert_cta_tape(cta_db,
                                                          f,
                                                          self.config)
                                    print_message("%s added label containing "
                                                  "copies  %s" % (label,
                                                                  copy_label,))
                                except Exception as e:
                                    print_error("%s volume %s already exists, "
                                                "skipping %s" %
                                                (label, f["label"], str(e)))
                                    pass
                            try:
                                if f["copy_deleted"] == "n":
                                    insert_cta_tape_file_copy(cta_db,
                                                              archive_file_id,
                                                              f,
                                                              self.config)
                            except Exception as e:
                                print_error("%s Failed to insert tape_file, %s"
                                            " %s %s %s, skipping %s" %
                                            (label,
                                             f["label"],
                                             f["pnfs_id"],
                                             f["bfid"],
                                             f["copy_bfid"],
                                             str(e)))
                                pass

                        location = "cta://cta/%s?archiveid=%d" %\
                                   (f["pnfs_id"],
                                   archive_file_id,)
                        try:
                            res = insert_chimera_location(chimera_db, f, location)
                        except Exception as e:
                             print_error("%s %s failed to insert location %s, %s" %
                                         (label, f["pnfs_id"], location, str(e),))
                             pass

                    except Exception as e:
                        print_error("%s, multiple pnfsid, skipping %s, %s" %
                                    (enstore_volume["label"], f["pnfs_id"], str(e)))
                        continue
                print_message("%s Done, %d files" %(label, len(files),))
        except Exception as e:
            print_message("Exception %s" % (str(e)))
        finally:
            for i in (enstore_db, cta_db, chimera_db):
                if i:
                    try:
                        i.close()
                    except:
                        pass



def update(con, sql, pars):
    """
    Update database record

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    return insert(con, sql, pars)


def insert(con, sql, pars):
    """
    Insert database record

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    cursor = None
    try:
        cursor = con.cursor()
        res = cursor.execute(sql, pars)
        con.commit()
        return res
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass

def insert_returning(con, sql, pars):
    """
    Insert database record

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    cursor = None
    try:
        sql +=  "returning *"
        cursor = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql, pars)
        res = cursor.fetchone()
        con.commit()
        return res
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def select(con, sql, pars=None):
    """
    Select  database records

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    cursor = None
    try:
        cursor = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if pars:
            cursor.execute(sql, pars)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def main():
    """
    main function
    """
    parser = argparse.ArgumentParser(
        description="This script converts Enstore metadata to CTA metadata. "
        "It looks for YAML configuration file pointed to by MIGRATION_CONFIG "
        "environment variable or, if it is not defined, it looks for file enstore2cta.yaml "
        "in current directory. Script will quit if configuration YAML is not found. "
        )

    parser.add_argument(
        "--label",
        help="comma separated list of labels")

    parser.add_argument(
        "--all",
        help="do all labels",
        action="store_true")

    args = parser.parse_args()

    if args.label and args.all:
        parser.print_help(sys.stderr)
        sys.exit(1)

    if not args.label and not args.all:
        parser.print_help(sys.stderr)
        sys.exit(1)

    configuration = None
    try:
        mode = os.stat(CONFIG_FILE).st_mode
        if mode != 33152:
            print_error("Access to config file file %s is too permissive" %
                        (CONFIG_FILE,))
            sys.exit(1)
        with open(CONFIG_FILE, "r") as f:
            configuration = yaml.safe_load(f)
    except (OSError, IOError) as e:
        if e.errno == errno.ENOENT:
            print_error("Config file %s does not exist" % (CONFIG_FILE,))
        sys.exit(1)

    if not configuration:
        print_error("Failed to load configuration %s" % (CONFIG_FILE,))
        sys.exit(1)

    print (configuration)

    labels = None
    if args.label:
        labels = [i.upper() for i in args.label.strip().split(",")]

    if args.all:
        enstore_db = create_connection(configuration.get("enstore_db"))
        cursor = enstore_db.cursor()
        cursor.execute(SELECT_ALL_ENSTORE_VOLUMES)
        labels = [i[0] for i in cursor.fetchall()]
        cursor.close()
        enstore_db.close()

    if not labels:
         print_error("**** No labels found, quitting ***")
         sys.exit(1)

    enstore_db = create_connection(configuration.get("enstore_db"))
    cta_db = create_connection(configuration.get("cta_db"))
    insert_vos(enstore_db, cta_db, disk_instance_name=configuration.get("disk_instance_name"))
    insert_storage_classes(enstore_db, cta_db)
    enstore_db.close()
    cta_db.close()


    print_message("**** Start processing %d  labels ****" % (len(labels), ))
    t0 = time.time()

    queue = multiprocessing.Queue(10000)
    workers = []
    cpu_count = multiprocessing.cpu_count()

    for i in range(cpu_count):
        worker = Worker(queue, configuration)
        workers.append(worker)
        worker.start()

    for label in labels:
        queue.put(label)

    for i in range(cpu_count):
        queue.put(None)

    map(lambda x: x.join(), workers);


    print_message("**** FINISH ****")
    print_message("Took %d seconds" % (int(time.time()-t0+0.5),))

if __name__ == "__main__":
    main()
