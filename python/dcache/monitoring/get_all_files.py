#!/usr/bin/python3

import argparse
import subprocess
import sys

#                                   total                     precious           sticky                  cached
# GM2.resilient@enstore     240044919811      1292           0         0  237212669097      1285  2832250714         7
#[d0.prd@enstore, [20757518100770, 59523, 528281210903, 2714, 0, 0, 20229236889867, 56809]]

#Storage class           Total: size,   files;  Precious: size, files;  Sticky: size,   files;  others: size, files
#lbne.persistent@enstore       49GiB    23703               0B      0          49GiB    23703             0B      0
#dune.persistent@enstore       12TiB  1042726               0B      0          12TiB  1042726             0B      0



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

#_CMD1 = "ssh -p 24223 enstore@fndca3b.fnal.gov \"\s rw*,w* rep ls -s=b \" | sed -e '1d' | awk '{ print $1,$4,$5}'"

#_CMD1 = "ssh -p 24223 enstore@fndcaitb3.fnal.gov \"\s rw-stkendca34a-* rep ls -s=b -noheader  \" "
_CMD1 = "ssh -p 24223 enstore@fndca3b.fnal.gov \"\s rw*,w* rep ls -s=b  -noheader \" "


if __name__ == "__main__":


    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--pgroup",
        help="Pool grooup name")

    args = parser.parse_args()

    if args.pgroup:
        rc, data = execute_command(f"ssh -p 24223 enstore@fndca3b.fnal.gov \"\s PoolManager psu ls pgroup -a {args.pgroup}\"")
        if rc != 0:
            print("Failed to connect to admin", file=sys.stderr)
            sys.exit(1)
        pools=""
        if data:
            if not data[0]:
                sys.stderr.write(f"No such pgroup {args.pgroup}\n")
                sys.stderr.flush()
                sys.exit(1)
            for i in data:
                if i.strip().find("stken") == -1 and i.strip().find("pub") == -1:
                    continue
                pools += i.strip().split()[0].strip() + ","
            pools = pools[:-1]
            _CMD1 = f"ssh -p 24223 enstore@fndca3b.fnal.gov  \"\s {pools}  rep ls -s=b -noheader \" "


    rc, data = execute_command(_CMD1)
    if rc:
        sys.exit(1)

    summary = {}
    for line in data:
        parts = line.split()
        if len(parts) < 4: continue
        sc = parts[0].split("@")[0].strip()
        total_bytes = total_count =  0
        precious_bytes = precious_count =  0
        sticky_bytes = sticky_count =  0
        other_bytes = other_count =  0
        try:
            total_bytes = int(parts[1])
            total_count = int(parts[2])
            precious_bytes = int(parts[3])
            precious_count = int(parts[4])
            sticky_bytes = int(parts[5])
            sticky_count = int(parts[6])
            other_bytes = int(parts[7])
            other_count = int(parts[8])
        except Exception:
            continue
        if total_count == 0 : continue
        if sc not in summary:
            summary[sc] = {
                'total_volume' : 0,
                'total_count' : 0,
                'precious_volume' : 0,
                'precious_count' : 0,
                'sticky_volume' : 0,
                'sticky_count' : 0,
                'other_volume' : 0,
                'other_count' : 0
                }
        summary[sc]['total_volume'] += total_bytes
        summary[sc]['total_count'] += total_count
        summary[sc]['precious_volume'] += precious_bytes
        summary[sc]['precious_count'] += precious_count
        summary[sc]['sticky_volume'] += sticky_bytes
        summary[sc]['sticky_count'] += sticky_count
        summary[sc]['other_volume'] += other_bytes
        summary[sc]['other_count'] += other_count

    if summary:
        items = list(summary.items())
        items.sort(key=lambda x: x[1]['total_volume'],reverse=True)

        w1 = max([len(x) for x in summary.keys()]) + 1
        w2 = max([len(str(x['total_volume']/1024/1024/1024)) for x in summary.values()]) + 1
        w3 = max([len(str(x['total_count'])) for x in summary.values()]) + 1

        p_w2 = max([len(str(x['precious_volume']/1024/1024/1024)) for x in summary.values()]) + 1
        p_w3 = max([len(str(x['precious_count'])) for x in summary.values()]) + 1

        s_w2 = max([len(str(x['sticky_volume']/1024/1024/1024)) for x in summary.values()]) + 1
        s_w3 = max([len(str(x['sticky_count'])) for x in summary.values()]) + 1

        o_w2 = max([len(str(x['other_volume']/1024/1024/1024)) for x in summary.values()]) + 1
        o_w3 = max([len(str(x['other_count'])) for x in summary.values()]) + 1

        total_bytes  = sum(x['total_volume'] for x in summary.values())
        total_counts = sum(x['total_count'] for x in summary.values())

        precous_bytes  = sum(x['precious_volume'] for x in summary.values())
        precious_counts = sum(x['precious_count'] for x in summary.values())

        sticky_bytes  = sum(x['sticky_volume'] for x in summary.values())
        sticky_counts = sum(x['sticky_count'] for x in summary.values())

        other_bytes  = sum(x['other_volume'] for x in summary.values())
        other_counts = sum(x['other_count'] for x in summary.values())

        sg = "sg"
        gb = "volume [GiB]"
        cnt = "count"
        t = "total"
        p = "precious"
        s = "sticky"
        o = "other"
        w = w1+p_w2

        print(f"{' ':^{w}} {t:^{w2}} {p:>{p_w2}} {s:>{s_w2}} {o:>{o_w2}}")
        print(f"{sg:^{w1}} {gb:^{w2}} {cnt:^{w3}} {gb:^{w2}} {cnt:^{w3}} {gb:^{w2}} {cnt:^{w3}} {gb:^{w2}} {cnt:^{w3}}")
        for item in items:
            print(f"{item[0]:<{w1}} {item[1]['total_volume']/1024/1024/1024:>{w2}.1f} {item[1]['total_count']:>{w3}} "
                  f"{item[1]['precious_volume']/1024/1024/1024:>{w2}.1f} {item[1]['precious_count']:>{w3}} "
                  f"{item[1]['sticky_volume']/1024/1024/1024:>{w2}.1f} {item[1]['sticky_count']:>{w3}} "
                  f"{item[1]['other_volume']/1024/1024/1024:>{w2}.1f} {item[1]['other_count']:>{w3}}"
                  )
        print(f"{t:<{w1}} {total_bytes/1024/1024/1024:>{w2}.1f} {total_counts:>{w3}} "
              f"{precious_bytes/1024/1024/1024:>{w2}.1f} {precious_counts:>{w3}} "
              f"{sticky_bytes/1024/1024/1024:>{w2}.1f} {sticky_counts:>{w3}} "
              f"{other_bytes/1024/1024/1024:>{w2}.1f} {other_counts:>{w3}} "
              )
    else:
        print("No precious files found")
