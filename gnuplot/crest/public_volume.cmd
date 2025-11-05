set terminal pdf color solid
set output 'public_volume.pdf'
set title 'Tape volume. Public'
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by -45
set mxtics 2
set ylabel 'Tape Volume [PB/year]'
set xlabel 'date'
set xdata time
set datafile separator ','
set key autotitle columnhead
set grid
set timefmt '%Y-%m-%d %H:%M:%S'
set format x '%Y'

FILE = 'data/lisa.csv'

a1              = 8.09753e-07  
a0              = -1153.67     

f(x) = a1*x+a0
fit [strptime("%Y-%m-%d %H-%M-%s", "2023-12-01 00:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2032-12-01 00:00:00")] f(x) FILE using 1:($2-$3+43) via a1,a0

dx = strptime("%Y-%m-%d %H-%M-%s", "2032-12-01 00:00:00") - strptime("%Y-%m-%d %H-%M-%s", "2023-12-01 00:00:00")
dy = f(strptime("%Y-%m-%d %H-%M-%s", "2032-12-01 00:00:00")) - f(strptime("%Y-%m-%d %H-%M-%s", "2023-12-01 00:00:00"))

s1 = int(dy / dx *3600.*24*365)

set key invert reverse right outside
plot 'data/historic_volumes.csv' using 3:($2/1000/1000/1000/1000/1000)  with lines lw 2 lc 3 t 'vol on tape', 'data/lisa.csv' using 1:($2-$3+43)  with points lw 2 lc 5 t 'projection' , [strptime("%Y-%m-%d %H-%M-%s", "2023-12-01 00:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2032-12-01 00:00:00")] f(x) t sprintf("%d PB/year", s1) lw 4 lc 2 

