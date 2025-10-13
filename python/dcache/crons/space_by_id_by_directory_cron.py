#!/usr/bin/env python3

"""
Script to collect user space by id by directory using PostgreSQL and pnfs filesystem.

This script performs the following tasks:
1. Collects space usage information from PostgreSQL database
2. Processes directory information using multiprocessing
3. Exports results to JSON and transfers to remote host
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import socket
import subprocess
import sys
import uuid

from multiprocessing import Lock, Manager, Process, Queue
from pathlib import Path
from typing import Any, Dict, List, Tuple, NoReturn
from urllib.parse import urlparse

import paramiko
import psycopg2
import psycopg2.extensions
import psycopg2.extras

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Constants
UUID_STR = str(uuid.uuid4())
KRB5CCNAME = f"/tmp/krb5cc_root.{UUID_STR}"
os.environ["KRB5CCNAME"] = KRB5CCNAME

HOSTNAME = socket.gethostname()
SSH_HOST = "remotehost"
SSH_PORT = 22
SSH_USER = "root"

# Type aliases
SpaceInfo = Tuple[int, int, int, int]  # uid, gid, count, total
UserSpaceDict = Dict[str, Dict[str, List[SpaceInfo]]]


def execute_command(cmd: str) -> int:
    """Execute a shell command and return its return code.

    Args:
        cmd: The command to execute.

    Returns:
        The return code of the command.
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
        logger.error("Failed to execute command: %s", exc)
        return 1


def kinit() -> None:
    """Create kerberos ticket for admin shell access."""
    cmd = f"/usr/bin/kinit -k host/{HOSTNAME}"
    if execute_command(cmd) != 0:
        logger.error("Failed to initialize Kerberos ticket")
        sys.exit(1)


class RemoteCopier:
    """Handle remote file copying operations via SSH."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize SSH connection with Kerberos authentication.

        Args:
            **kwargs: Connection parameters for paramiko.SSHClient.
        """
        kinit()
        self.shell = paramiko.SSHClient()
        self.shell.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.shell.connect(
                **kwargs,
                gss_auth=True,
                gss_kex=True,
            )
            self.client = self.shell.open_sftp()
        except paramiko.SSHException as exc:
            logger.error("SSH connection failed: %s", exc)
            sys.exit(1)

    def put(self, local_file_path: str, remote_file_path: str) -> None:
        """Upload a file to the remote server.

        Args:
            local_file_path: Path to the local file.
            remote_file_path: Path on the remote server.
        """
        try:
            self.client.put(local_file_path, remote_file_path)
        except (IOError, paramiko.SFTPError) as exc:
            logger.error("Failed to upload file: %s", exc)
            raise

    def close(self) -> None:
        """Close the SFTP and SSH connections."""
        for conn in (self.client, self.shell):
            try:
                conn.close()
            except Exception as exc:
                logger.warning("Error closing connection: %s", exc)

# query below excludes /pnfs/fs/usr/icarus/persistent/calibration
# and /pnfs/fs/usr/uboone/persistent/PublicAccess
SPACE_QUERY = (
    "WITH RECURSIVE paths(number, path, TYPE, fsize, uid, gid) AS ("
    "  VALUES (pnfsid2inumber(%s), '', 16384, 0::BIGINT, 0, 0)"
    "  UNION"
    "  SELECT d.ichild, path || '/' || d.iname, i.itype, i.isize,"
    "         i.iuid, i.igid"
    "  FROM t_dirs d, t_inodes i, paths p"
    "  WHERE p.TYPE = 16384"
    "    AND d.iparent = p.number"
    "    AND d.ichild not in (5597689247, 7801204967)"
    "    AND d.iname != '.'"
    "    AND d.iname != '..'"
    "    AND i.inumber = d.ichild)"
    " SELECT SUM(p.fsize::bigint) AS total, count(*), uid, gid"
    " FROM paths p"
    " WHERE p.TYPE = 32768"
    " GROUP BY uid, gid"
    " ORDER BY total DESC"
)


def create_connection(uri: str) -> psycopg2.extensions.connection:
    """Create a PostgreSQL database connection.

    Args:
        uri: The database URI.

    Returns:
        A database connection object.

    Raises:
        psycopg2.Error: If connection fails.
    """
    try:
        result = urlparse(uri)
        connection = psycopg2.connect(
            database=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
        )
        return connection
    except psycopg2.Error as exc:
        logger.error("Database connection failed: %s", exc)
        raise


class Worker(Process):
    """Worker process for parallel directory processing."""

    def __init__(
        self,
        task_queue: Queue,
        space_summary: Manager.dict,
        lock: Lock,
        print_lock: Lock,
        db_uri: str,
    ) -> None:
        """Initialize the worker process.

        Args:
            task_queue: Queue containing directory tasks.
            space_summary: Shared dictionary for results.
            lock: Lock for shared resource access.
            print_lock: Lock for print operations.
            db_uri: Database connection URI.
        """
        super().__init__()
        self.task_queue = task_queue
        self.lock = lock
        self.space_summary = space_summary
        self.print_lock = print_lock
        self.db_uri = db_uri

    def run(self) -> None:
        """Process directory tasks and collect space usage information."""
        db = None
        try:
            db = create_connection(self.db_uri)
            user_space: UserSpaceDict = {}

            for data in iter(self.task_queue.get, None):
                direntry, dirs = data
                if direntry not in user_space:
                    user_space[direntry] = {}

                for d, pnfsid in dirs:
                    if d not in user_space[direntry]:
                        user_space[direntry][d] = []

                    logger.info("Processing directory: %s %s", direntry, d)

                    try:
                        with db.cursor() as cursor:
                            cursor.execute(SPACE_QUERY, (pnfsid,))
                            for row in cursor.fetchall():
                                user_space[direntry][d].append(
                                    (int(row[2]), int(row[3]), int(row[1]), int(row[0]))
                                )
                    except psycopg2.Error as exc:
                        logger.error(
                            "Database error processing %s/%s: %s",
                            direntry, d, exc
                        )
                        continue
                    except Exception as exc:
                        logger.error(
                            "Unexpected error processing %s/%s: %s",
                            direntry, d, exc
                        )
                        continue

            if user_space:
                with self.lock:
                    self.space_summary.update(user_space)

        except Exception as exc:
            logger.error("Worker process failed: %s", exc)
        finally:
            try:
                if db:
                    db.close()
            except Exception:
                pass


def exit_with_error(message: str, code: int = 1) -> NoReturn:
    """Exit the program with an error message.

    Args:
        message: Error message to log.
        code: Exit code to use.
    """
    logger.error(message)
    sys.exit(code)


def main() -> None:
    """Process directory space usage and upload results."""
    top_dir = Path("/pnfs/fs/usr")
    if not top_dir.exists():
        exit_with_error(f"{top_dir} does not exist or is not mounted")

    try:
        cpu_count = multiprocessing.cpu_count()
        manager = Manager()
        space_summary = manager.dict()
        task_queue = Queue(maxsize=10000)
        lock = Lock()
        print_lock = Lock()

        db_uri = os.environ.get(
            "CHIMERA_DB_URI",
            "postgresql://enstore_reader:password@localhost:5432/chimera"
        )

        workers = [
            Worker(task_queue, space_summary, lock, print_lock, db_uri)
            for _ in range(cpu_count)
        ]

        for worker in workers:
            worker.start()

        # Process directories
        for direntry in os.listdir(top_dir):
            de = top_dir / direntry
            if de.is_symlink() or de.is_file():
                continue

            dirs = []
            for d in os.listdir(de):
                if d not in ("persistent", "resilient"):
                    continue

                id_path = de / f".(id)({d})"
                try:
                    pnfsid = id_path.read_text(encoding="utf-8").strip()
                    if pnfsid:
                        dirs.append((d, pnfsid))
                except IOError as exc:
                    logger.error("Error reading %s: %s", id_path, exc)

            if dirs:
                task_queue.put((direntry, dirs))

        # Signal workers to finish
        for _ in range(cpu_count):
            task_queue.put(None)

        # Wait for workers
        for worker in workers:
            worker.join()

        # Save and transfer results
        out_path = Path("/tmp/user_space_by_id_by_directory.json")
        try:
            out_path.write_text(
                json.dumps(dict(space_summary), indent=4, sort_keys=True),
                encoding="utf-8"
            )
        except IOError as exc:
            exit_with_error(f"Failed to write output file: {exc}")

        try:
            copier = RemoteCopier(
                hostname=SSH_HOST,
                username=SSH_USER,
                port=SSH_PORT
            )
            copier.put(str(out_path), str(out_path))
            copier.close()
        except Exception as exc:
            exit_with_error(f"Failed to transfer file: {exc}")
        finally:
            try:
                os.unlink(KRB5CCNAME)
            except OSError:
                pass

    except Exception as exc:
        exit_with_error(f"Unexpected error: {exc}")


if __name__ == "__main__":
    main()
