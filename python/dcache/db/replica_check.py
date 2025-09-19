#!/usr/bin/env python

import  psycopg2
import sys
import time
try:
    import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse


__QUERY="""
SELECT CASE
         WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
         ELSE EXTRACT (EPOCH
                       FROM age(now(),pg_last_xact_replay_timestamp()))
       END AS log_delay
"""


def print_error(text):
    """
    Print text string to stderr prefixed with timestamp
    and ERROR keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    sys.stderr.write(time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(time.time()))+" ERROR : " + text + "\n")
    sys.stderr.flush()


def print_message(text):
    """
    Print text string to stderr prefixed with timestamp
    and ERROR keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    sys.stdout.write(time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(time.time()))+" ERROR : " + text + "\n")
    sys.stderr.flush()



def create_connection(uri):
    result = urlparse.urlparse(uri)
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port)
    return connection


def main():
    db = cursor = None
    try:
        db = create_connection("postgresql://enstore:password@localhost:5432/chimera")
        cursor = db.cursor()
        cursor.execute(__QUERY)
        result = cursor.fetchall()
        delay = 0
        if result:
            delay = int(result[0][0])
        if delay > 600:
            print_error(f"Replica is behind {delay} seconds")
            return 1
        return 0
    except Exception as e:
        print_error(f"Failed to query DB {e}")
        return 1
    finally:
        for i in (cursor, db):
            if i:
                try:
                    i.close()
                except:
                    pass


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
