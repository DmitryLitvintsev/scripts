set terminal pdf color solid
set output 'cms_rate.pdf'
set title 'Daily data volume (reads and writes) vs time.  CMS'
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by -45
set mxtics 2
set ylabel 'Daily volume TB'
set xlabel 'date'
set xdata time
set datafile separator ','
set key autotitle columnhead
set grid
set timefmt '%Y-%m-%d %H:%M:%S'
set format x '%Y'
set xrange [: '2033-01-01 00:00:00']
set yrange [: 1600]
set key invert reverse right outside

AVG='data/transferred_cms.csv'
MAX='data/maxima.csv'

a1              = 2.09383e-06
a0              = -2939.33

f(x) = a1*x+a0
fit [strptime("%Y-%m-%d %H-%M-%s", "2014-12-01 00:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2021-12-01 00:00:00")] f(x) AVG using 2:($1/365) via a1,a0
dx = strptime("%Y-%m-%d %H-%M-%s", "2021-12-01 00:00:00") - strptime("%Y-%m-%d %H-%M-%s", "2014-12-01 00:00:00")
dy = f(strptime("%Y-%m-%d %H-%M-%s", "2021-12-01 00:00:00")) - f(strptime("%Y-%m-%d %H-%M-%s", "2014-12-01 00:00:00"))

s1 = int(dy / dx *3600.*24*365)

b1              = 2.09383e-06
b0              = -2939.33


g(x) = b1*x+b0
fit [strptime("%Y-%m-%d %H-%M-%s", "2014-12-01 00:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2021-12-01 00:00:00")] g(x) MAX using 1:($2/1024./1024./1024./1024.) via b1,b0
dx1 = strptime("%Y-%m-%d %H-%M-%s", "2021-12-01 00:00:00") - strptime("%Y-%m-%d %H-%M-%s", "2014-12-01 00:00:00")
dy1 = g(strptime("%Y-%m-%d %H-%M-%s", "2021-12-01 00:00:00")) - g(strptime("%Y-%m-%d %H-%M-%s", "2014-12-01 00:00:00"))

s2 = int(dy1 / dx1 *3600.*24*365)


plot AVG using 2:($1/365.) with yerrorbars lw 2 t 'avg daily volume', MAX using  1:($2/1024./1024./1024./1024.) with yerrorbars lw 2 t 'max daily volume', [strptime("%Y-%m-%d %H-%M-%s", "2014-12-01 00:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2032-12-01 00:00:00")] f(x) t sprintf("%d TiB/year", s1) lw 4 lc 1, [strptime("%Y-%m-%d %H-%M-\
%s", "2014-12-01 00:00:00"):strptime("%Y-%m-%d %H-%M-%s", "2032-12-01 00:00:00")] g(x) t sprintf("%d TiB/year", s2) lw 4 lc 2
