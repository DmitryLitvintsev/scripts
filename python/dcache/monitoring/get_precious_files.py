#!/usr/bin/python3

import argparse
import subprocess
import sys

#                                   total                     precious           sticky                  cached
# GM2.resilient@enstore     240044919811      1292           0         0  237212669097      1285  2832250714         7
#[d0.prd@enstore, [20757518100770, 59523, 528281210903, 2714, 0, 0, 20229236889867, 56809]]



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
    #return (rc, output.decode("utf-8").strip().replace(r"\r","\n").split("\n"))

#_CMD1 = "ssh -p 24223 enstore@fndca3b. \"\s rw*,w* rep ls -s=b \" | sed -e '1d' | awk '{ print $1,$4,$5}'"

#_CMD1 = "ssh -p 24223 enstore@fndcaitb3. \"\s rw-stkendca34a-* rep ls -s=b -noheader  \" "
_CMD1 = "ssh -p 24223 enstore@fndca3b \"\s rw*,w* rep ls -s=b  -noheader \" "


if __name__ == "__main__":


    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--pgroup",
        help="Pool grooup name")

    args = parser.parse_args()

    if args.pgroup:
        rc, data = execute_command(f"ssh -p 24223 enstore@fndca3b \"\s PoolManager psu ls pgroup -a {args.pgroup}\"")
        pools=""
        if data:
            if not data[0]:
                sys.stderr.write(f"No such pgroup {args.pgroup}\n")
                sys.stderr.flush()
                sys.exit(1)
            for i in data:
                if i.strip().find("stken") == -1:
                    continue
                pools += i.strip().split()[0].strip() + ","
            pools = pools[:-1]
            _CMD1 = f"ssh -p 24223 enstore@fndca3b  \"\s {pools}  rep ls -s=b -noheader \" "


    rc, data = execute_command(_CMD1)
    if rc:
        sys.exit(1)

    summary = {}
    for line in data:
        parts = line.split()
        if len(parts) < 4: continue
        sc = parts[0].split("@")[0].strip()
        if sc.endswith("resilient"): continue
        bytes = int(parts[3])
        count = int(parts[4])
        if count == 0 : continue
        if sc not in summary:
            summary[sc] = { 'volume' : 0,
                            'count' : 0 }
        summary[sc]['volume'] += bytes
        summary[sc]['count'] += count

    if summary:
        items = list(summary.items())
        items.sort(key=lambda x: x[1]['volume'],reverse=True)
        w1 = max([len(x) for x in summary.keys()]) + 1
        w2 = max([len(str(x['volume']/1024/1024/1024)) for x in summary.values()]) + 1
        w3 = max([len(str(x['count'])) for x in summary.values()]) + 1

        bytes  = sum(x['volume'] for x in summary.values())
        counts = sum(x['count'] for x in summary.values())

        sg = "sg"
        gb = "volume [GiB]"
        cnt = "count"
        t = "total"

        print(f"{sg:^{w1}} {gb:^{w2}} {cnt:^{w3}}")
        for item in items:
            print(f"{item[0]:<{w1}} {item[1]['volume']/1024/1024/1024:>{w2}.1f} {item[1]['count']:>{w3}}")
        print(f"{t:<{w1}} {bytes/1024/1024/1024:>{w2}.1f} {counts:>{w3}}")
    else:
        print("No precious files found")
