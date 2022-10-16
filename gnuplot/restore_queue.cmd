# Example of gnuplot script that taks CSV timeseries data
# and performs range fits

set terminal pdf color solid
set output 'restore_queue.pdf'
set title 'Enstore restore queue vs time'
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by -90
set mxtics 2
set ylabel "Count"
set xlabel "day"
set grid
set xdata time
set format x  "%m-%d %H"
set timefmt "%Y-%m-%d %H:%M:%S"

set key top right
set datafile separator ","


FILE = 'data/restoreQueue.csv'

a1              = -0.259833 
a0              = 4.33119e+08
f(x) = a1*x+a0
fit [strptime("%Y-%m-%d %H-%M-%s", "2022-10-11 12:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 02:00:00")] f(x) FILE using 1:2 via a1,a0


dx = strptime("%Y-%m-%d %H-%M-%s", "2022-10-11 12:00:00") - strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 02:00:00")
dy = f(strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 02:00:00")) - f(strptime("%Y-%m-%d %H-%M-%s", "2022-10-11 12:00:00"))
s1 = int(dy / dx *3600.)

b1              = -0.259833 
b0              = 4.33119e+08
g(x) = b1*x+b0
fit [strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 20:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2022-10-15 12:00:00")] g(x) FILE using 1:2 via b1,b0

dx = strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 20:00:00") - strptime("%Y-%m-%d %H-%M-%s", "2022-10-15 12:00:00")
dy = g(strptime("%Y-%m-%d %H-%M-%s", "2022-10-15 12:00:00")) - g(strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 20:00:00"))
s2 = int(dy / dx * 3600.)

plot FILE using 1:2 with lines lw 2 t 'restore queue', \
     [strptime("%Y-%m-%d %H-%M-%s", "2022-10-11 18:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 00:00:00")] f(x) t sprintf("%d files/hour", s1) lw 4, \
     [strptime("%Y-%m-%d %H-%M-%s", "2022-10-14 20:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2022-10-15 12:00:00")] g(x) t sprintf("%d files/hour", s2)  lw 4
     
