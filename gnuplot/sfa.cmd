# Example of stacked time series plot

set terminal pdf color solid
set output 'sfa.pdf'
set title 'Daily SFA activity'

FILE = 'data/enstore_sfa_week.data'
FILE1 = 'data/dcache_sfa_week.data'

set key invert reverse right outside
set style data histogram
set style histogram rowstacked

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -90
set ylabel "TiB / day"
set xlabel "date"
set grid
#set datafile separator ","
set format x  "%Y-%m-%d"

plot FILE using 6:xtic(1) t 'restore', \
FILE using 4  t 'store', \
FILE using 8 t 'dcache read'
