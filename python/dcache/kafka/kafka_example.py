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

_STORAGE_GROUP = "cms"
_LOGGER = logging.getLogger()

def main():

    #consumer = KafkaConsumer("ingest.dcache.billing",
    consumer = KafkaConsumer("ingest.dcache.billing.cms-tape",
    #consumer = KafkaConsumer("ingest.dcache.billing.cms-disk",
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
        elif msgType == "transfer":
#            {'date': '2026-05-20T14:38:48-05:00', 'msgType': 'transfer', 'cellName': 'w-cmsstor368-disk-disk2', 'session': 'pool:w-cmsstor368-disk-disk2@w-cmsstor368-disk-disk2Domain:1779305928000-9853', 'subject': ['GidPrincipal[5063,primary]', 'GroupNamePrincipal[uscms6769]', 'FQANPrincipal[/cms]', 'EmailAddressPrincipal[alan.malta@cern.ch]', 'FQANPrincipal[/cms/uscms]', 'GidPrincipal[1999]', '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=amaltaro/CN=718748/CN=Alan Malta Rodrigues', 'FQANPrincipal[/cms/Role=production,primary]', 'UserNamePrincipal[cmsprod]', 'Origin[2620:6a:0:8421:f0:0:188:119]', 'GroupNamePrincipal[cmsprod,primary]', 'UidPrincipal[9811]', 'GidPrincipal[9114]', 'GidPrincipal[9247]'], 'initiator': 'door:XrootdLFNs-cmsdcadisk01new@xrootdLFNs-cmsdcadisk01newDomain:AAZSRO1WWeg:1779305910458000', 'transferPath': '/dcache/uscmsdisk/store/mc/HINPbPbWinter25GS/Hydjet_Quenched_MinBias_TuneCELLO_5p36TeV_pythia8/GEN-SIM/151X_mcRun3_2025_realistic_HI_v4-v2/2560002/e4056925-364d-4104-80e4-d0779175e3dd.root', 'meanReadBandwidth': 3976993086.4393344, 'readIdle': 'PT0.076390418S', 'queuingTime': 0, 'cellDomain': 'w-cmsstor368-disk-disk2Domain', 'isP2p': False, 'transferTime': 16880, 'version': '1.0', 'storageInfo': 'cms.cms11@cta', 'transferSize': 28055934, 'localEndpoint': '[2620:6a:0:8420:f0:0:205:89]:15227', 'protocolInfo': {'protocol': 'Xrootd', 'port': 58528, 'host': '2620:6a:0:8421:f0:0:188:119', 'transferTag': '', 'versionMajor': 5, 'versionMinor': 0}, 'cellType': 'pool', 'readActive': 'PT0.002415457S', 'fileSize': 9444622945, 'pnfsid': '00005393924E7BA049D3BABA151655F5ECFA', 'billingPath': '/dcache/uscmsdisk/store/mc/HINPbPbWinter25GS/Hydjet_Quenched_MinBias_TuneCELLO_5p36TeV_pythia8/GEN-SIM/151X_mcRun3_2025_realistic_HI_v4-v2/2560002/e4056925-364d-4104-80e4-d0779175e3dd.root', 'isWrite': 'read', 'status': {'msg': '', 'code': 0}}

            direction = message.get("isWrite")
            direction_string = "to" if direction == "write" else "from"
            transfer_time = message.get("transferTime")
            transfer_size = message.get("transferSize")
            protocol_name = message.get("protocolInfo").get("protocol")
            is_p2p = message.get("isP2p")

            file_size = message.get("fileSize")
            speed = transfer_size / float ( 1 << 20 ) / transfer_time * 1000.
            path = message.get("billingPath")
            _LOGGER.info (f"File {path} {direction} {direction_string} pool {pool}, speed {speed:.2f} MiB/s")
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
