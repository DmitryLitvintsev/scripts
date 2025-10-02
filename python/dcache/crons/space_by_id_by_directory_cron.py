#!/usr/bin/env python3

"""
Script to collect user space by id by directory using PostgreSQL and pnfs
filesystem. Converted to Python 3 and multiprocessing, PEP8 compliant.
"""
import os
import sys
import time
import pickle
import multiprocessing
from multiprocessing import Process, Lock, Manager, Queue
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
import subprocess

SPACE_QUERY = (
    'WITH RECURSIVE paths(number, path, TYPE, fsize, uid, gid) AS ('
    '  VALUES (pnfsid2inumber(%s), \'\', 16384, 0::BIGINT, 0, 0)'
    '  UNION'
    '  SELECT d.ichild, path || \'/\' || d.iname, i.itype, i.isize,'
    '         i.iuid, i.igid'
    '  FROM t_dirs d, t_inodes i, paths p'
    '  WHERE p.TYPE = 16384'
    '    AND d.iparent = p.number'
    '    AND d.iname != \'.\''
    '    AND d.iname != \'..\''
    '    AND i.inumber = d.ichild)'
    ' SELECT SUM(p.fsize::bigint) AS total, count(*), uid, gid'
    ' FROM paths p'
    ' WHERE p.TYPE = 32768'
    ' GROUP BY uid, gid'
    ' ORDER BY total DESC'
)

def create_connection(uri):
    result = urlparse(uri)
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port)
    return connection

class Worker(Process):
    def __init__(self, task_queue, user_space,
                 lock, print_lock, db_uri):
        super().__init__()
        self.task_queue = task_queue
        self.user_space = user_space
        self.lock = lock
        self.print_lock = print_lock
        self.db_uri = db_uri

    def run(self):
        db = create_connection(self.db_uri)
        while True:
            data = self.task_queue.get()
            if data is None:
                break
            direntry, d, pnfsid = data
            cursor = None
            with self.print_lock:
                sys.stderr.write(
                    f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} "
                    f": doing {direntry}\n"
                )
                sys.stderr.flush()
            try:
                cursor = db.cursor()
                cursor.execute(SPACE_QUERY, (pnfsid,))
                res = cursor.fetchall()
                for row in res:
                    with self.lock:
                        self.user_space[direntry][d].append(
                            (row[2], row[3], row[1], row[0])
                        )
            except Exception as exc:
                with self.print_lock:
                    sys.stderr.write(
                        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} "
                        f": {type(exc)} {exc}\n"
                    )
                    sys.stderr.flush()
                # Do not raise, just log and continue
            finally:
                if cursor:
                    cursor.close()
        db.close()

def main():
    top_dir = "/pnfs/fs/usr"
    if not os.path.exists(top_dir):
        sys.stderr.write(
            f"{top_dir} does not exist, or not mounted\n"
        )
        sys.stderr.flush()
        sys.exit(1)

    cpu_count = multiprocessing.cpu_count()
    manager = Manager()
    user_space = manager.dict()
    task_queue = Queue(maxsize=10000)
    lock = Lock()
    print_lock = Lock()

    # Use environment variable for DB URI if available
    db_uri = os.environ.get(
        "CHIMERA_DB_URI",
        "postgresql://enstore_reader:password@localhost:5432/chimera"
    )

    workers = [
        Worker(task_queue, user_space, lock, print_lock, db_uri)
        for _ in range(cpu_count)
    ]
    for worker in workers:
        worker.start()

    direntries = os.listdir(top_dir)
    for direntry in direntries:
        de = os.path.abspath(os.path.join(top_dir, direntry))
        if os.path.islink(de) or os.path.isfile(de):
            continue
        for d in os.listdir(de):
            if d != "persistent":
                continue
            with lock:
                if direntry not in user_space:
                    user_space[direntry] = manager.dict()
                if d not in user_space[direntry]:
                    user_space[direntry][d] = manager.list()
            id_path = os.path.join(de, f".(id)({d})")
            pnfsid = None
            try:
                with open(id_path, "r", encoding="utf-8") as f:
                    pnfsid = f.readline().strip()
            except Exception as exc:
                with print_lock:
                    sys.stderr.write(
                        f"Error reading {id_path}: {exc}\n"
                    )
                    sys.stderr.flush()
                continue
            if not pnfsid:
                continue
            task_queue.put((direntry, d, pnfsid))

    for _ in range(cpu_count):
        task_queue.put(None)

    for worker in workers:
        worker.join()

    out_path = "/tmp/user_space_by_id_by_directory.data"
    # The file is opened in binary mode, which is correct for pickle.dump
    # type: ignore[assignment]
    with open(out_path, "wb") as f:  # type: ignore
        pickle.dump(dict(user_space), f)
    enrcp_cmd = [
        os.environ.get("ENSTORE_DIR", "") + "/sbin/enrcp",
        out_path,
        "fndca:/tmp"
    ]
    try:
        subprocess.run(enrcp_cmd, check=True)
    except Exception as exc:
        sys.stderr.write(f"Error running enrcp: {exc}\n")
        sys.stderr.flush()

if __name__ == "__main__":
    main()
