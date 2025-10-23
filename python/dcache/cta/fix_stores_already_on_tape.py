#!/usr/bin/env python3

"""
Script to fix stores that are already on tape.

This script handles files that are already stored on tape but need their
database entries updated to reflect this status.
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

import paramiko
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
kinit_lock = Lock()

# Constants
UUID_STR = str(uuid.uuid4())
CONFIG_FILE = os.getenv("DCACHE_CONFIG", "dcache.yaml")
HOSTNAME = socket.getfqdn()
SSH_HOST = "fndca"
SSH_PORT = 24223
SSH_USER = "enstore"
KRB5CCNAME = f"/tmp/krb5cc_root.migration-{UUID_STR}"

# Environment setup
os.environ["KRB5CCNAME"] = KRB5CCNAME

# Compiled regex pattern
PNFSID_PATTERN = re.compile(r"[A-F0-9]{36}")


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


def kinit() -> None:
    """Create Kerberos ticket for admin shell access."""
    cmd = f"/usr/bin/kinit -k host/{HOSTNAME}"
    if execute_command(cmd) != 0:
        logger.error("Failed to initialize Kerberos ticket")
        sys.exit(1)


def get_shell(
    host: str = SSH_HOST,
    port: int = SSH_PORT,
    user: str = SSH_USER
) -> paramiko.SSHClient:
    """Create and return an admin shell connection.

    Args:
        host: SSH host to connect to
        port: SSH port number
        user: SSH username

    Returns:
        Connected SSH client

    Raises:
        paramiko.SSHException: If connection fails
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=host,
        port=port,
        username=user,
        gss_auth=True,
        gss_kex=True
    )
    return ssh


def execute_admin_command(ssh: paramiko.SSHClient, cmd: str) -> List[str]:
    """Execute a command on the admin shell.

    Args:
        ssh: Connected SSH client
        cmd: Command to execute

    Returns:
        List of output lines with whitespace stripped
    """
    stdin, stdout, stderr = ssh.exec_command(cmd)
    return [
        line.strip().replace(r"\r", "\n")
        for line in stdout.readlines()
        if line.strip().replace(r"\r", "\n")
    ]


def is_cached(ssh: paramiko.SSHClient, pnfsid: str) -> bool:
    """Check if a file is cached.

    Args:
        ssh: Connected SSH client
        pnfsid: PNFS ID to check

    Returns:
        True if file is cached, False otherwise
    """
    result = execute_admin_command(ssh, f"\\sn cacheinfoof {pnfsid}")
    return bool(result)


def get_locations(ssh: paramiko.SSHClient, pnfsid: str) -> List[str]:
    """Get storage locations for a PNFS ID.

    Args:
        ssh: Connected SSH client
        pnfsid: PNFS ID to check

    Returns:
        List of storage locations
    """
    result = execute_admin_command(ssh, f"\\sn cacheinfoof {pnfsid}")
    if result:
        return result[0].split()
    return []


def mark_precious(ssh: paramiko.SSHClient, pnfsid: str) -> bool:
    """Mark a file as precious on all locations.

    Args:
        ssh: Connected SSH client
        pnfsid: PNFS ID to mark

    Returns:
        True if operation succeeded
    """
    execute_admin_command(ssh, f"\\sl {pnfsid} rep set precious {pnfsid}")
    return True


def mark_precious_on_location(
    ssh: paramiko.SSHClient,
    pool: str,
    pnfsid: str
) -> bool:
    """Mark a file as precious on a specific pool.

    Args:
        ssh: Connected SSH client
        pool: Pool name
        pnfsid: PNFS ID to mark

    Returns:
        True if operation succeeded
    """
    result = execute_admin_command(
        ssh,
        f"\\s {pool} rep set precious {pnfsid}"
    )
    logger.info("Marked precious %s %s %s", pnfsid, pool, result)
    return True


def clear_file_cache_location(
    ssh: paramiko.SSHClient,
    pool: str,
    pnfsid: str
) -> bool:
    """Clear a file's cache location.

    Args:
        ssh: Connected SSH client
        pool: Pool name
        pnfsid: PNFS ID to clear

    Returns:
        True if operation succeeded
    """
    result = execute_admin_command(
        ssh,
        f"\\sn clear file cache location {pnfsid} {pool}"
    )
    logger.info("Cleared file cache location %s %s %s", pnfsid, pool, result)
    return True


def get_precious_fraction(ssh: paramiko.SSHClient, pool: str) -> float:
    """Get the fraction of precious data on a pool.

    Args:
        ssh: Connected SSH client
        pool: Pool name

    Returns:
        Percentage of precious data (0-100)
    """
    result = execute_admin_command(ssh, f"\\s {pool} info -a")
    for line in result:
        if "Precious" in line:
            return float(re.sub(r"[\[-\]]", "", line.split()[-1]))
    return 0.0


def get_active_pools_in_pool_group(
    ssh: paramiko.SSHClient,
    pgroup: str
) -> List[str]:
    """Get list of active pools in a pool group.

    Args:
        ssh: Connected SSH client
        pgroup: Pool group name

    Returns:
        List of active pool names
    """
    result = execute_admin_command(
        ssh,
        f"\\s PoolManager psu ls pgroup -a {pgroup}"
    )
    pools = []
    has_pool_list = False

    for line in result:
        if not line.strip():
            continue
        if "nested groups" in line:
            break
        if line.strip().startswith("poolList :"):
            has_pool_list = True
            continue
        if has_pool_list:
            parts = line.split()
            if "mode=disabled" in parts[1]:
                continue
            pools.append(parts[0].strip())

    return pools


def get_path(pnfsid: str) -> Optional[str]:
    """Get filesystem path for a PNFS ID.

    Args:
        pnfsid: PNFS ID to look up

    Returns:
        Path if found, None otherwise
    """
    try:
        path = Path(f"/pnfs/fnal.gov/usr/.(pathof)({pnfsid})")
        return path.read_text().strip()
    except (IOError, OSError) as exc:
        logger.error("Failed to get path for %s: %s", pnfsid, exc)
        return None


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


class KinitWorker(Process):
    """Worker process to maintain Kerberos tickets."""

    def __init__(self) -> None:
        """Initialize the worker."""
        super().__init__()
        self.stop = False

    def run(self) -> None:
        """Periodically refresh Kerberos tickets."""
        while not self.stop:
            with kinit_lock:
                kinit()
                time.sleep(14400)  # 4 hours


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
        cta_db = chimera_db = ssh = None
        try:
            cta_db = create_connection(self.config["cta_db"])
            chimera_db = create_connection(self.config["chimera_db"])
            ssh = get_shell(
                self.config["admin"].get("host", SSH_HOST),
                self.config["admin"].get("port", SSH_PORT),
                self.config["admin"].get("user", SSH_USER)
            )

            for pnfsid, storage_class in iter(self.queue.get, None):
                self._process_file(ssh, cta_db, chimera_db, pnfsid, storage_class)

        except Exception as exc:
            logger.error("Worker failed: %s", exc)
        finally:
            for conn in (ssh, cta_db, chimera_db):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _process_file(
        self,
        ssh: paramiko.SSHClient,
        cta_db: psycopg2.extensions.connection,
        chimera_db: psycopg2.extensions.connection,
        pnfsid: str,
        storage_class: str
    ) -> None:
        """Process a single file.

        Args:
            ssh: SSH connection
            cta_db: CTA database connection
            chimera_db: Chimera database connection
            pnfsid: PNFS ID to process
            storage_class: Storage class
        """

        # select only entries that are 12 hours old
        # to avoid messing with files that are stored in CTA
        # but are still in 'st ls' queue waiting for CTA notification
        # plus some multiple copy files may take really long time to complete
        rows = select(
            cta_db,
            "select disk_instance_name, "
            "'cta://cta/'||disk_file_id||'?archiveid='||archive_file_id as location "
            "from archive_file where disk_file_id = %s and creation_time < %s",
            (pnfsid, int(time.time()) - 12 * 3600)
        )

        if not rows:
            return

        disk_instance_name = rows[0]["disk_instance_name"]
        location = rows[0]["location"]
        storage_group, file_family = storage_class.split(".")

        logger.info(
            "%s %s %s %s %s",
            pnfsid,
            disk_instance_name,
            location,
            storage_group,
            file_family
            )

        result = select(
            chimera_db,
            "select count(*) from t_locationinfo "
            "where itype = 0 and inumber = "
            "(select inumber from t_inodes where ipnfsid = %s)",
            (pnfsid,)
        )

        if result[0]["count"] != 0:
            logger.error(
                "File has location in chimera %s %s",
                pnfsid,
                storage_class
            )
            return

        logger.info(
            "%s %s %s %s %s",
            pnfsid,
            disk_instance_name,
            location,
            storage_group,
            file_family
        )

        insert(
            chimera_db,
            "insert into t_storageinfo "
            "(inumber, ihsmname, istoragegroup, istoragesubgroup) "
            "values (pnfsid2inumber(%s), 'cta', %s, %s)",
            (pnfsid, storage_group, file_family)
        )

        insert(
            chimera_db,
            "insert into t_locationinfo "
            "(inumber, itype, ipriority, ictime, iatime, istate, ilocation) "
            "values (pnfsid2inumber(%s), 0, 10, now(), now(), 1, %s)",
            (pnfsid, location)
        )

        execute_admin_command(
            ssh,
            f"\\sl {pnfsid} rep set cached {pnfsid}"
        )
        execute_admin_command(
            ssh,
            f"\\sl {pnfsid} st kill {pnfsid}"
        )


def main() -> None:
    """Main function to process files that are already on tape."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Process files that are already on tape and update their status"
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

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Load configuration
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

    logger.info("Starting store processing")

    # Initialize Kerberos
    try:
        kinit()
    except Exception as exc:
        logger.error("Failed to initialize Kerberos: %s", exc)
        sys.exit(1)

    # Start Kerberos ticket refresh worker
    kinit_worker = KinitWorker()
    kinit_worker.start()

    try:
        # Get initial SSH connection
        ssh = get_shell(
            config["admin"].get("host", SSH_HOST),
            config["admin"].get("port", SSH_PORT),
            config["admin"].get("user", SSH_USER)
        )

        # Get all stores
        all_stores = execute_admin_command(ssh, "\\s r*,w* st ls")
        pnfsids = []

        # Parse store information
        for line in all_stores:
            if not line:
                continue
            parts = line.strip().split()
            if len(parts) < 2:
                continue
            pnfsid = parts[-2]
            if PNFSID_PATTERN.match(pnfsid):
                storage_class = parts[-1].strip("'")
                pnfsids.append((pnfsid, storage_class))

        logger.info("Found %d stores to process", len(pnfsids))
        ssh.close()

        # Set up worker processes
        queue: Queue = Queue(maxsize=10000)
        workers = [
            Worker(queue, config)
            for _ in range(args.cpu_count)
        ]

        for worker in workers:
            worker.start()

        # Process files
        for i, item in enumerate(pnfsids, 1):
            if i % 1000 == 0:
                logger.info("Queued %d files for processing", i)
            queue.put(item)

        # Signal workers to finish
        for _ in range(args.cpu_count):
            queue.put(None)

        # Wait for workers
        for worker in workers:
            worker.join()

    except Exception as exc:
        logger.error("Processing failed: %s", exc)
        sys.exit(1)
    finally:
        # Clean up
        kinit_worker.stop = True
        kinit_worker.terminate()
        kinit_worker.join(timeout=1)
        try:
            os.unlink(KRB5CCNAME)
        except OSError:
            pass

    logger.info("Processing completed successfully")


if __name__ == "__main__":
    main()
