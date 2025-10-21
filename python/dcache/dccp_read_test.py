#!/usr/bin/env python

import argparse
import logging
import multiprocessing
import os
import subprocess
import sys
import time

STOP="/tmp/STOP"

from multiprocessing import Process, Queue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


def execute_command(cmd: str):
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


class Worker(Process):
    """Worker process to run a dccp on a loop"""
    def __init__(self, uri, queue):
        super().__init__()
        self.uri = uri
        self.queue = queue

    def run(self):
        for pnfsid in iter(self.queue.get, None):
            if os.path.exists(STOP):
                break
            rc = execute_command(f"dccp {self.uri}/{pnfsid} /dev/null")
            if rc != 0:
                logger.error("dccp failed, quitting")
                break

def main():
    """
    main function
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Run multiple dccps in a loop"
        )

    parser.add_argument(
        "--cpu_count",
        action="store",
        type=int,
        default=multiprocessing.cpu_count(),
        help="size of the process pool")

    parser.add_argument(
        "--url",
        action ="store",
        type=str,
        default="pnfs://stkendca53a:22125",
        help="dcap url")

    parser.add_argument(
        "--pnfsid",
        action="store",
        type=str,
        required=True,
        default=None,
        metavar="PNFSID",
        help="pnfsid of a file")

    parser.add_argument(
        "--count",
        action="store",
        type=int,
        default=100,
        help="how many total dccps to run")

    args = parser.parse_args()

    cpu_count = args.cpu_count
    url = args.url
    pnfsid = args.pnfsid
    dccp_count = args.count

    if not pnfsid:
        parser.print_help(sys.stderr)
        sys.exit(1)

    queue = Queue(maxsize=10000)

    workers = [
            Worker(url, queue)
            for _ in range(cpu_count)
    ]

    for worker in workers:
        worker.start()

    for _ in range(dccp_count):
        queue.put(pnfsid)

    for _ in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()


if __name__ == "__main__":
    main()
