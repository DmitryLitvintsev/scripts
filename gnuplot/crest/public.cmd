set terminal pdf color solid
set output 'public.pdf'
set title 'HTTP PROPFIND requests on CMS dCache daily'
set title 'Rates and volumes vs time'
set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror rotate by -90
set mxtics 2
set ylabel 'Daily volume TB'
set xlabel 'date'
set xdata time
set y2label 'Data Volume [PB]'
set y2tics border
set datafile separator ','
set key autotitle columnhead
set grid
set timefmt '%Y-%m-%d %H:%M:%S'
set format x '%Y'
plot 'data/transferred_public.csv' using 2:($1/365.) with lines lw 2 t 'avg daily volume', 'data/maxima.csv' using  1:($3/1024./1024./1024./1024.) with lines lw 2 t 'max daily volume', 'data/historic_volumes.csv' using 3:($2/1000/1000/1000/1000/1000) axes x1y2 with lines lw 2 lc 3 t 'vol on tape', 'data/lisa.csv' using 1:($2-$3) axes x1y2 with points lw 2 lc 5 t 'projection'
