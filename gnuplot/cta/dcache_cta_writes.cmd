# Example of stacked time series plot

set terminal pdf color solid
set output 'dcache_cta_writes.pdf'
set title 'dCache CTA writes, cta dev'

FILE = 'dcache_cta_writes.data'

#set key invert reverse right outside

set yrange [0:420]

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -90
set ylabel "Rate [MiB/s]"
set xlabel "hour"
set grid
set format x  "%H:%M"
set timefmt "%b %d %H:%M:%S"
set xdata time
set xrange ["Mar 16 16:20:00" : "Mar 16 16:32:00"]

a0 =  300
f(x) = a0
fit f(x) FILE using 1:($5/$4/1024./1024.) via a0


plot FILE using 1:($5/$4/1024./1024.) with points pointtype 7 ps 0.3 lc 7 t '', f(x)  lw 3 lc 2 t sprintf("avg rate = %d MiB/s", int(a0+0.5))
