import json
from kafka import KafkaConsumer
from urllib.parse import urlparse, parse_qs
import logging

"""
msgType:
  remove
  request
  restore
  store
  transfer

"""

_STORAGE_GROUP = "mu2e"
_LOGGER = logging.getLogger()

def main():

    consumer = KafkaConsumer("ingest.dcache.billing",
                             bootstrap_servers="lskafka:9092",
                             value_deserializer=lambda m: json.loads(m.decode("ascii")))
    for msg in consumer:
        message = msg.value
        msgType = message["msgType"]

        pnfsid = message.get("pnfsid")
        pool = message.get("cellName")
        storage_info = message.get("storageInfo")
        if storage_info:
            if not storage_info.startswith(_STORAGE_GROUP):
                continue


        if msgType == "restore":
            if 'locations' not in message:
                continue
            transfer_time = message.get("transferTime")
            file_size = message.get("fileSize")
            storage_info = message.get("storageInfo")
            speed = file_size / float ( 1 << 20 ) / transfer_time * 1000.
            locations = message.get("locations")
            loc = locations[0]
            url = urlparse(loc)
            qs = parse_qs(url.query)
            volume = qs.get("volume")[0]
            path = qs.get("original_name")[0]
            _LOGGER.info (f"File {path} restored from {volume} to pool {pool} speed {speed:.2f} MiB/s")
        elif msgType == "store":
            transfer_time = message.get("transferTime")
            file_size = message.get("fileSize")
            speed = file_size / float ( 1 << 20 ) / transfer_time * 1000.
            path = message.get("billingPath")
            _LOGGER.info (f"File {path} stored from pool {pool}, speed {speed:.2f} MiB/s")
        elif msgType == "remove":
            status = message.get("status")

            if status.get("msg") == "sweeper making space for new data" and \
                   status.get("code") == 0:
                _LOGGER.info(f"{pnfsid} went OFFLINE")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s - %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S%z")

    main()
