#!/usr/bin/python3

import subprocess
import sys
#                                   total                     precious           sticky                  cached
# GM2.resilient@enstore     240044919811      1292           0         0  237212669097      1285  2832250714         7


def execute_command(cmd):
    """
    Executes shell command

    :param cmd: command string
    :type cmd: str
    :return: shell command return code
    :rtype: int
    """
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    output, errors = p.communicate()
    rc = p.returncode
    #    if rc != 0:
    #       print_error(errors)
    return (rc, output.decode("utf-8").strip().split("\n"))


_CMD1 = "ssh -p 24223 enstore@fndca3b \"\s rw* rep ls -s \" | awk '{ print $1,$6,$7}'"
#_CMD1 = "ssh -p 24223 enstore@fndca3b \"\s rw-stkendca19a-1 rep ls -s \" | awk '{ print $1,$6,$7}'"

if __name__ == "__main__":
    rc, data = execute_command(_CMD1)
    if rc:
        sys.exit(1)
    summary = {}
    for line in data:
        parts = line.split()
        if len(parts)<2 : continue
        sc = parts[0].split("@")[0].strip()
        if sc.endswith("resilient") :
            continue
        sc = sc.split(".")[0]
        bytes = int(parts[1])
        count = int(parts[2])
        if count == 0 : continue
        if sc not in summary:
            summary[sc] = { 'volume' : 0,
                            'count' : 0 }
        summary[sc]['volume'] += bytes
        summary[sc]['count'] += count

    items = list(summary.items())
    items.sort(key=lambda x: x[1]['volume'],reverse=True)
    w1 = max([len(x) for x in summary.keys()]) + 1
    w2 = max([len(str(x['volume']/1024/1024/1024)) for x in summary.values()]) + 1
    w3 = max([len(str(x['count'])) for x in summary.values()]) + 1

    sg = "sg"
    gb = "volume [GiB]"
    cnt = "count"

    bytes  = sum(x['volume'] for x in summary.values())
    counts = sum(x['count'] for x in summary.values())
    t = "total"



    print(f"{sg:^{w1}} {gb:^{w2}} {cnt:^{w3}}")
    for item in items:
        print(f"{item[0]:<{w1}} {item[1]['volume']/1024/1024/1024:>{w2}.1f} {item[1]['count']:>{w3}}")
    print(f"{t:<{w1}} {bytes/1024/1024/1024:>{w2}.1f} {counts:>{w3}}")
