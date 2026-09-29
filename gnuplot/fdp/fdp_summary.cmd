# Example of stacked time series plot

set terminal postscript color solid
set output 'fdp_fy_2025_summary.ps'
set title 'Writes and reads FDP, FY 2025'

FILE = 'fdp_fy_2025.csv'

set key invert reverse left top

set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -45
set ylabel "TB / month"
set xlabel "date"
set grid
set datafile separator ","
set format x  "%Y-%b"
set xdata time
set timefmt "%Y-%m-%d %H:%M:%S"
#set yrange [0:200]

box_width = 14*24*3600*0.5
set boxwidth box_width
offset = 0.5 * box_width

#
plot FILE using (timecolumn(1) - offset):($4/1000000000000) with boxes t 'writes',\
 '' using (timecolumn(1) + offset):($8/1000000000000) with boxes t 'reads', \
 '' using 1:($5/1000000000000) with lines lc 1 lw 5 t 'aggregate writes', \
 '' using 1:($9/1000000000000) with lines lc 2 lw 5 t 'aggregate reads'
