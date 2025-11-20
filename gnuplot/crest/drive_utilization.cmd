set terminal pdf color solid
set output 'drive_utilization.pdf'
set title 'Number active drives vs time, volume vs time on public'
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by -45
set mxtics 2
set ylabel '# of drives in use'
set xlabel 'date'
set xdata time
set y2label 'volume [TiB/day]'
set y2tics border
set datafile separator ','
set key autotitle columnhead
#set key invert reverse right outside
set grid
set timefmt '%Y-%m-%d %H:%M:%S'
set format x '%Y-%m-%d'
#set y2range [0:]
#set yrange [0: 226.000000]
FILE1='data/drive_utilization.csv'
FILE2='data/transferred_for_drive_utilization.csv'

a0 =  50
f(x) = a0
fit f(x) FILE1 using 1:2 via a0

b0 = 300
g(x) = b0

fit g(x) FILE2 using 2:1 via b0

c0 = 1000
h(x) = c0

fit h(x) FILE1 using  1:($2*360.*0.953674*3600.*24./1024./1024.) via c0

set yrange [0:130]

plot FILE1 using 1:2 with lines lw 2 lc 1 t 'active drives', FILE2 using 2:1 axes x1y2 with lines lw 2 lc 2 t 'actual volume', FILE1 using 1:($2*360.*3600.*24./1024./1024.) axes x1y2 with lines lw 2 lc 3 t 'theor. volume', f(x)  lw 4 lc 1 t sprintf("avg drives = %d", int(a0)), g(x)  axes x1y2 lw 4 lc 2 t sprintf("avg vol = %d", int(b0)),  h(x) axes x1y2 lw 4 lc 3 t sprintf("theor. vol. = %d", int(c0))
