# Example of stacked time series plot

set terminal postscript color solid
set output 'fdp_fy_2025.ps'
set title 'Writes and reads FDP, FY 2025'

FILE = 'fdp_fy_2025.csv'

#set key invert reverse right outside
set key invert reverse left top
set style data histogram
set style histogram rowstacked

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -45
set ylabel "TB / month"
set xlabel "date"
set grid
set datafile separator ","
set format x  "%Y-%m"
#set xdata time
#set timefmt "%Y-%m-%d %H:%M:%S"
set yrange [0:200]
#
plot FILE using ($4/1000000000000):xtic(1):xticlabel(strftime("%Y-%b", strptime("%Y-%m-%d %H:%M:%S", strcol(1)))) t 'writes',\
 '' using ($8/1000000000000) t 'reads'
