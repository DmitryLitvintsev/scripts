#!/usr/bin/env python3

"""
Script to recover deleted file.

"""

from __future__ import annotations

import argparse
import errno
import logging
import multiprocessing
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from multiprocessing import Lock, Process, Queue
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, NoReturn
from urllib.parse import urlparse

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Global locks
print_lock = Lock()

# Constants
CONFIG_FILE = os.getenv("DCACHE_CONFIG", "dcache.yaml")

def get_pnfsid(path: str) -> str:
    """
    Get pnfsid for path

    Args:
        cmd: Command to execute.

    Returns:
        The command's return code.
    """
    pnfsid = None
    dn = os.path.dirname(path)
    fn = os.path.basename(path)
    with open("%s/.(id)(%s)" % (dn, fn,), "r") as fh:
        pnfsid = fh.readlines()[0].strip()
    return pnfsid


def execute_command(cmd: str) -> int:
    """Execute a shell command and return its exit code.

    Args:
        cmd: Command to execute.

    Returns:
        The command's return code.
    """
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True
        )
        process.communicate()
        return process.returncode
    except subprocess.SubprocessError as exc:
        logger.error("Command execution failed: %s", exc)
        return 1


def create_connection(uri: str) -> psycopg2.extensions.connection:
    """Create a database connection from a URI.

    Args:
        uri: Database connection URI

    Returns:
        Database connection object

    Raises:
        psycopg2.Error: If connection fails
    """
    result = urlparse(uri)
    return psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )


def update(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> Any:
    """Update database records.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        Query result

    Raises:
        psycopg2.Error: If query fails
    """
    return insert(conn, sql, params)


def insert(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> Any:
    """Insert database records.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        Query result

    Raises:
        psycopg2.Error: If query fails
    """
    cursor = None
    try:
        cursor = conn.cursor()
        if params:
            result = cursor.execute(sql, params)
        else:
            result = cursor.execute(sql)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def insert_returning(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> Dict[str, Any]:
    """Insert database record and return inserted row.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        Dictionary containing inserted row

    Raises:
        psycopg2.Error: If query fails
    """
    cursor = None
    try:
        sql += " returning *"
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        result = cursor.fetchone()
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def select(
    conn: psycopg2.extensions.connection,
    sql: str,
    params: Optional[tuple] = None
) -> List[Dict[str, Any]]:
    """Select database records.

    Args:
        conn: Database connection
        sql: SQL statement
        params: Query parameters

    Returns:
        List of dictionaries containing selected rows

    Raises:
        psycopg2.Error: If query fails
    """
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass

class Worker(Process):
    """Worker process to query CTA DB and process results."""

    def __init__(
        self,
        queue: Queue,
        config: Dict[str, Any]
    ) -> None:
        """Initialize the worker.

        Args:
            queue: Task queue
            config: Configuration dictionary
        """
        super().__init__()
        self.queue = queue
        self.config = config

    def run(self) -> None:
        """Process tasks from the queue."""
        cta_db = chimera_db = None
        try:
            cta_db = create_connection(self.config["cta_db"])
            chimera_db = create_connection(self.config["chimera_db"])
            for file_name in iter(self.queue.get, None):
                self._process_file(file_name, cta_db, chimera_db)

        except Exception as exc:
            logger.error("Worker failed: %s", exc)
        finally:
            for conn in (cta_db, chimera_db):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _process_file(
        self,
        file_name: str,
        cta_db: psycopg2.extensions.connection,
        chimera_db: psycopg2.extensions.connection
    ) -> None:
        """Process a single file.

        Args:
            file_name: full file name
            cta_db: CTA database connection
            chimera_db: Chimera database connection
            pnfsid: PNFS ID to process
            storage_class: Storage class
        """

        rows = select(
            chimera_db,
            "select * "
            "from t_deleted_paths where iname = %s order by ideltime desc limit 1",
            (file_name, )
        )

        if not rows:
            logger.error(f"{file_name} does not exist in t_deletd_paths in cbimera, skipping")
            return

        chimera_file_info = rows[0]
        pnfsid = chimera_file_info.get("ipnfsid")

        rows = select(
            cta_db,
            "select sc.storage_class_name, frl.* "
            "from file_recycle_log frl inner join storage_class sc "
            "on frl.storage_class_id = sc.storage_class_id  where disk_file_id_when_deleted = %s ",
            (pnfsid, )
        )

        if not rows:
            logger.error(f"{file_name} {pnfsid} does not exist in cta DB, skipping")
            return

        file_info = rows[0]
        adler32_csum = hex(int(file_info.get("checksum_adler32"))).lstrip("0x").zfill(8)
        storage_class = file_info.get("storage_class_name")
        parts =  storage_class.split("@")
        (vo, file_family), hsm = parts[0].split("."),parts[1]
        archive_file_id = file_info.get("archive_file_id")


        res = insert(
            cta_db,
            "insert into archive_file ( "
            "  archive_file_id, "
            "  disk_instance_name, "
            "  disk_file_id, "
            "  disk_file_uid, "
            "  disk_file_gid, "
            "  size_in_bytes, "
            "  checksum_blob, "
            "  checksum_adler32, "
            "  storage_class_id, "
            "  creation_time, "
            "  reconciliation_time "
            " ) values ( "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s) ",
            (file_info.get("archive_file_id"),
             file_info.get("disk_instance_name"),
             file_info.get("disk_file_id_when_deleted"),
             file_info.get("disk_file_uid"),
             file_info.get("disk_file_gid"),
             file_info.get("size_in_bytes"),
             file_info.get("checksum_blob"),
             file_info.get("checksum_adler32"),
             file_info.get("storage_class_id"),
             file_info.get("archive_file_creation_time"),
             file_info.get("reconciliation_time")))


        res = insert(
            cta_db,
            "insert into tape_file( "
            "  vid, "
            "  fseq, "
            "  block_id, "
            "  logical_size_in_bytes, "
            "  copy_nb, "
            "  creation_time, "
            "  archive_file_id "
            "  ) values ( "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s, "
            "%s ) ",
            (file_info.get("vid"),
             file_info.get("fseq"),
             file_info.get("block_id"),
             file_info.get("size_in_bytes"),
             file_info.get("copy_nb"),
             file_info.get("tape_file_creation_time"),
             file_info.get("archive_file_id")))


        res = insert(
            cta_db,
            "update tape set dirty = '1' where vid = %s",
            (file_info.get("vid"),))


        Path(file_name).touch()
        new_pnfsid = get_pnfsid(file_name)

        res = update(
            chimera_db,
            "update t_inodes set "
            " ipnfsid = %s, "
            " imode = %s, "
            " iuid = %s, "
            " igid = %s, "
            " icrtime = %s, "
            " ictime = %s, "
            " iatime = %s, "
            " imtime = %s, "
            " iaccess_latency = %s, "
            " iretention_policy = 0, "
            " isize = %s "
            "where ipnfsid = %s",
            (pnfsid,
             chimera_file_info.get("imode"),
             chimera_file_info.get("iuid"),
             chimera_file_info.get("igid"),
             chimera_file_info.get("icrtime"),
             chimera_file_info.get("icrtime"),
             chimera_file_info.get("icrtime"),
             chimera_file_info.get("icrtime"),
             chimera_file_info.get("iaccess_latency"),
             chimera_file_info.get("isize"),
             new_pnfsid,))

        logger.info(f"Doing checksum {pnfsid} {adler32_csum}")
        res = insert(
            chimera_db,
            "update t_inodes_checksum "
            " set isum = %s "
            "where "
            "inumber = (select inumber from t_inodes where ipnfsid = %s)",
            (adler32_csum, pnfsid))

        try:
            res = insert(
                chimera_db,
                "insert into t_storageinfo ("
                " inumber, ihsmname, istoragegroup, istoragesubgroup "
                ") values ( "
                "(select inumber from t_inodes where ipnfsid = %s), %s, %s, %s)",
                (pnfsid, hsm, vo, file_family))
            logger.info(f"Inserted into t_storageinfo {pnfsid} {hsm} {vo} {file_family}")
        except:
            res = insert(
                chimera_db,
                "update t_storageinfo "
                "set ihsmname = %s, "
                "istoragegroup = %s, "
                "istoragesubgroup = %s "
                "where inumber = (select inumber from t_inodes where ipnfsid = %s) ",
                (hsm, vo, file_family, pnfsid))
            logger.info(f"Updated into t_storageinfo {pnfsid} {hsm} {vo} {file_family}")


        res = insert(
            chimera_db,
            "insert into t_locationinfo ("
            " inumber, itype, ipriority, ictime, iatime, istate, ilocation "
            ") values ("
            "(select inumber from t_inodes where ipnfsid = %s), "
            "0, 10, %s, %s, 1, %s)",
            (pnfsid,
             chimera_file_info.get("icrtime"),
             chimera_file_info.get("icrtime"),
             f"cta://cta/{pnfsid}?archiveid={archive_file_id}"))

        logger.info(f"Inserted into t_locationinfo {pnfsid} cta://cta/{pnfsid}?archiveid={archive_file_id}")

        rows = select(
            chimera_db,
            "select ilocation from t_locationinfo "
            "where inumber = (select inumber from t_inodes where ipnfsid = %s) "
            " and itype = 1",
            (pnfsid,))

        if rows:
            locations = [row["ilocation"] for row in rows]
            logger.info(f"TODO: delete {new_pnfsid} on locations {locations}")

        res = insert(
            chimera_db,
            "delete from t_locationinfo "
            "where inumber = (select inumber from t_inodes where ipnfsid = %s) "
            " and itype = 1",
            (pnfsid,))

        res = insert(
            chimera_db,
            "delete from t_deleted_paths "
            "where ipnfsid = %s ",
            (pnfsid,))

        res = insert(
            cta_db,
            "delete from file_recycle_log "
            "where "
            " archive_file_id = %s "
            " and copy_nb = %s "
            " and disk_file_id_when_deleted = %s ",
            (archive_file_id, file_info.get("copy_nb"), pnfsid))

#        res = insert(
#            chimera_db,
#            "update t_locationinfo "
#            " set ilocation = %s "
#            "where "
#            "inumber = (select inumber from t_inodes where ipnfsid = %s)",
#            (f"cta://cta/{pnfsid}?archiveid={archive_file_id}", pnfsid))
#

#        res = insert(
#            chimera_db,
#            "update nsert into t_storageinfo "
#            "(inumber, ihsmname, istoragegroup, istoragesubgroup) "
#            "values "
#            "((select inumber from t_inodes where ipnfsid = %s), "
#            "%s, %s, %s)",
#            (pnfsid, hsm, vo, file_family))


# delete from t_locationinfo where inumber = 12680326071 and itype = 1 ;


def main() -> None:
    """Main function to process files that are already on tape."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Recover removed files"
    )

    parser.add_argument(
        "--cpu-count",
        type=int,
        default=multiprocessing.cpu_count(),
        help="Number of worker processes to spawn"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    parser.add_argument(
        "-f", "--file",
        nargs='+',
        help='file paths to recover',
        required=False,
        metavar="FILE")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if not args.file:
        parser.print_help(sys.stderr)
        sys.exit(1)

    # Load configuration
    config = {}
    try:
        config_path = Path(CONFIG_FILE)
        #if config_path.stat().st_mode != 0o600:
        if config_path.stat().st_mode != 33152 :
            logger.error(
                "Config file %s permissions too permissive, should be 0600",
                CONFIG_FILE
            )
            sys.exit(1)

        config = yaml.safe_load(config_path.read_text())
        if not config:
            logger.error("Failed to load configuration from %s", CONFIG_FILE)
            sys.exit(1)

    except (OSError, IOError) as exc:
        if isinstance(exc, FileNotFoundError):
            logger.error("Config file %s does not exist", CONFIG_FILE)
        else:
            logger.error("Error reading config file: %s", exc)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.error("Error parsing config file: %s", exc)
        sys.exit(1)




    logger.info("Starting recovery processing")

    queue: Queue = Queue(maxsize=10)
    workers = [
        Worker(queue, config)
        for _ in range(args.cpu_count)
    ]

    for worker in workers:
        worker.start()

    # Process files
    for i, item in enumerate(args.file, 1):
        if i % 100 == 0:
            logger.info("Queued %d files for processing", i)
        queue.put(item)

    # Signal workers to finish
    for _ in range(args.cpu_count):
        queue.put(None)

    # Wait for workers
    for worker in workers:
        worker.join()

    logger.info("Processing completed successfully")


if __name__ == "__main__":
    main()
