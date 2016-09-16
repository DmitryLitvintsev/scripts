#!/bin/bash

cat > gnuplot.cmd <<EOF
set terminal postscript color solid
set output "${1}_network.ps"
set title "network on ${1} on `date +"%m/%d/%y"`"
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by 90
set mxtics 2
set ylabel "[MiB/s]"
set xlabel "time (hour)"
set xdata time
set grid
set timefmt "%Y-%m-%d %H:%M:%S"
set format x '%m-%d %H'
plot "sar_${1}_network.data" using 1:(\$6/1024.) with lines lw 2  t 'RX', "sar_${1}_network.data" using 1:(\$7/1024.) with lines lw 2  t 'TX'
EOF

gnuplot  gnuplot.cmd

cat > gnuplot.cmd <<EOF
set terminal postscript color solid
set output "${1}_io_network.ps"
set title "I/O and I/O wait on ${1} on `date +"%m/%d/%y"`"
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by 90
set mxtics 2
set ylabel "[MiB/s]"
set y2label "I/O wait [%]"
set xlabel "time (hour)"
set xdata time
set grid
set y2tics border
set timefmt "%Y-%m-%d %H:%M:%S"
set format x '%m-%d %H'
plot "sar_${1}_io.data" using 1:(\$6*512/1024./1024.) with lines lw 2 lc 2 t 'read I/O','' using 1:(\$7*512./1024./1024.) with lines lw 2 lc 1 t 'write I/O', "sar_${1}_network.data" using 1:(\$6*2./1024.) with lines lw 2  t 'RX', "sar_${1}_network.data" using 1:(\$7*2./1024.) with lines lw 2  t 'TX', "sar_${1}_iowait.data" using 1:7  axes x1y2 with lines lw 2 t 'iowait'
EOF

gnuplot  gnuplot.cmd

cat > gnuplot.cmd <<EOF
set terminal postscript color solid
set output "${1}_io_memory.ps"
set title "IO and memory on ${1} on `date +"%m/%d/%y"`"
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by 90
set mxtics 2
set ylabel "[MiB/s]"
set y2label "memory/swap usage [\%]"
set xlabel "time (hour)"
set xdata time
set grid
set y2tics border
set timefmt "%Y-%m-%d %H:%M:%S"
set format x '%m-%d %H'
plot "sar_${1}_io.data" using 1:(\$6*512/1024./1024.) with lines lw 2 lc 2 t 'read I/O', '' using 1:(\$7*512./1024./1024.) with lines lw 2 lc 1 t 'write I/O', "sar_${1}_memory.data" using 1:5 axes x1y2 with lines lw 2 t 'memory usage %', "sar_${1}_swap.data" using 1:5 axes x1y2 with lines lw 2 t 'swap usage %'
EOF

gnuplot  gnuplot.cmd

cat > gnuplot.cmd <<EOF
set terminal postscript color solid
set output "${1}_load_memory.ps"
set title "Load and memory on ${1} on `date +"%m/%d/%y"`"
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by 90
set mxtics 2
set ylabel "load average"
set y2label "memory/swap usage [\%]"
set xlabel "time (hour)"
set xdata time
set grid
set y2tics border
set timefmt "%Y-%m-%d %H:%M:%S"
set format x '%m-%d %H'
plot "sar_${1}_load.data" using 1:5  with lines lw 2 t 'ldavg-1', "sar_${1}_memory.data" using 1:5 axes x1y2 with lines lw 2 t 'memory usage %', "sar_${1}_swap.data" using 1:5 axes x1y2 with lines lw 2 t 'swap usage %'
EOF

gnuplot  gnuplot.cmd


cat > gnuplot.cmd <<EOF
set terminal postscript color solid
set output "${1}_io_load.ps"
set title "I/O and load on ${1} on `date +"%m/%d/%y"`"
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by 90
set mxtics 2
set ylabel "[MiB/s]"
set y2label "load"
set xlabel "time (hour)"
set xdata time
set grid
set y2tics border
set timefmt "%Y-%m-%d %H:%M:%S"
set format x '%m-%d %H'
plot "sar_${1}_io.data" using 1:(\$6*512/1024./1024.) with lines lw 2 lc 2 t 'read I/O',\
'' using 1:(\$7*512./1024./1024.) with lines lw 2 lc 1 t 'write I/O',\
"sar_${1}_load.data" using 1:5 axes x1y2 with lines lw 2 t 'ldavg-1'
EOF

cat > gnuplot.cmd <<EOF
set terminal postscript color solid
set output "${1}_fd_memory.ps"
set title "Fd and memory on ${1} on `date +"%m/%d/%y"`"
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by 90
set mxtics 2
set ylabel "[count]"
set y2label "memory/swap usage [\%]"
set xlabel "time (hour)"
set xdata time
set grid
set y2tics border
set timefmt "%Y-%m-%d %H:%M:%S"
set format x '%m-%d %H'
plot "sar_${1}_fd.data" using 1:4 with lines lw 2 lc 1 t '# fd', "sar_${1}_memory.data" using 1:5 axes x1y2 with lines lw 2 t 'memory usage %', "sar_${1}_swap.data" using 1:5 axes x1y2 with lines lw 2 t 'swap usage %'
EOF

gnuplot  gnuplot.cmd


