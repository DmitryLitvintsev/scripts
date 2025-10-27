#!/usr/bin/env python3

"""
This script scans srmspacefile table and for each pnfsid
checks if it is present in chimera t_inodes table. If the
record is not found it is removed from srmspacefile table.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import multiprocessing
import os
import sys
from multiprocessing import Lock, Process, Queue
from pathlib import Path
from typing import List, Optional, Tuple, Any
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection, cursor
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

CONFIG_FILE = os.getenv("DCACHE_CONFIG", "dcache.yaml")

# SQL Queries
DELETE_SPACEFILE_ENTRY = """
DELETE FROM srmspacefile WHERE pnfsid = %s
"""

SPACEMANAGER_SCAN_QUERY = """
SELECT pnfsid FROM srmspacefile
"""

CHIMERA_CHECK_QUERY = """
SELECT * FROM t_inodes WHERE ipnfsid = %s
"""

# Global lock for printing
print_lock = Lock()


def log_error(text: str) -> None:
    """Log error message with timestamp."""
    with print_lock:
        logger.error(text)


def log_info(text: str) -> None:
    """Log info message with timestamp."""
    with print_lock:
        logger.info(text)

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

class Worker(Process):
    """Worker process to handle database operations."""

    def __init__(
        self,
        queue: Queue,
        config: Dict[str, Any]
    ) -> None:
        """Initialize worker process with database connections.

        Args:
            queue: Task queue
        config: Configuration dictionary
        """
        super().__init__()
        self.queue = queue
        self.config = config

    def run(self) -> None:
        """Process tasks from the queue."""
        chimera_db = spacemanager_db = chimera_cursor = spacemanager_cursor = None
        try:
            chimera_db = create_connection(self.config["chimera_db"])
            chimera_cursor = chimera_db.cursor()
            spacemanager_db =  create_connection(self.config["spacemanager_db"])
            spacemanager_cursor =  spacemanager_db.cursor()

            for pnfsid in iter(self.queue.get, None):
                # Check if pnfsid exists in chimera
                chimera_cursor.execute(CHIMERA_CHECK_QUERY, (pnfsid,))
                res = chimera_cursor.fetchall()

                if not res:
                    log_info(f"Removing {pnfsid} from srmspacefile")
                    try:
                        spacemanager_cursor.execute(
                            DELETE_SPACEFILE_ENTRY,
                            (pnfsid,)
                            )
                        spacemanager_db.commit()
                    except Exception as exc:
                        log_error(
                            f"Failed removing {pnfsid} from srmspacefile: {exc}"
                        )
                        spacemanager_db.rollback()
        finally:
            for cursor in (chimera_cursor, spacemanager_cursor):
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
            for conn in (chimera_db, spacemanager_db):
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

def main() -> None:
    """Main function to process space files."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
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
    config = None
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

    # Create database connection
    connection = cursor = None
    try:
        connection =  create_connection(config["spacemanager_db"])

        cursor = connection.cursor(
            'cursor_for_spacemanager_scan',
            cursor_factory=psycopg2.extras.DictCursor
        )

        # Create queue and workers
        queue: Queue = Queue(maxsize=10000)
        processes: List[Worker] = []

        # Start workers
        total = 0
        for _ in range(args.cpu_count):
            worker = Worker(
                queue,
                config
            )
            processes.append(worker)
            worker.start()

        try:
            cursor.execute(SPACEMANAGER_SCAN_QUERY)
            while True:
                res = cursor.fetchmany(10000)
                if not res:
                    break
                total += len(res)
                for r in res:
                    pnfsid = r[0]
                    queue.put(pnfsid)
                log_info(f"Processed {total}")

        except Exception as exc:
            log_error(f"Error: {exc}")
            raise
        finally:
            # Stop workers
            for _ in range(args.cpu_count):
                queue.put(None)

            # Wait for workers to finish
            for process in processes:
                process.join()

    except Exception as exc:
        log_error(f"Fatal error: {exc}")
        sys.exit(1)
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if connection:
            try:
                connection.close()
            except Exception:
                pass

    log_info("Finished processing")


if __name__ == "__main__":
    main()
