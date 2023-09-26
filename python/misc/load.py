import os
import sys
import time


if __name__ == "__main__":
    fn = sys.argv[1]
    data = []
    with open(fn, "r") as f:
        for line in f:
            if not line:
                continue
            parts = line.strip().split(",")
            start = int(parts[0])
            end = int(parts[1])
            data.append((start, 1))
            data.append((end, -1))
    data.sort(key=lambda x: x[0])

    load = 0
    max_load = 0
    with open("/tmp/load.data", "w") as f:
        for i in data[:-4]:
            t = i[0]
            f.write("%s %s\n" %(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(i[0])), load,))
            load += i[1]
            if load > max_load : max_load = load
            f.write("%s %s\n" %(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(i[0])), load,))

    with open("/tmp/load.cmd", "w") as f:
        f.write("set terminal postscript color solid\n"
                "set output 'load.ps'\n"
                "set title 'Load vs time'\n"
                "set boxwidth 0.75 absolute\n"
                "set style fill solid 1.00 border -1\n"
                "set xtics border in scale 1,0.5 nomirror\n"
                "set mxtics 2\n"
                "set ylabel 'load'\n"
                "set xlabel 'date'\n"
                "set xdata time\n"
                "set grid\n"
                "set timefmt '%Y-%m-%d %H:%M:%S'\n"
                "set format x '%H:%M'\n")
        f.write("set yrange [0: %f] \n" %( (max_load + 0.1*max_load, )))
        f.write("plot '/tmp/load.data' using 1:3 with lines lw 2 t ''\n")
    os.system("gnuplot /tmp/load.cmd ; convert -rotate 90 -flatten load.ps load.jpg")
