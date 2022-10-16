# Example of stacked time series plot

set terminal pdf color solid
set output 'enstore_writes.pdf'
set title 'Daily write volumes by major players to Enstore'

FILE = 'data/enstore_writes.csv'

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
set datafile separator ","
set format x  "%Y-%m-%d"

plot FILE using 3:xtic(1):xticlabel((int($0) % 5)==0? strftime("%m/%d %H", strptime("%Y-%m-%d %H:%M:%S", strcol(1))):"") t 'gm2', \
'' using 7 t 'nova', \
'' using 6 t 'icarus', \
'' using 8 t 'lqcd', \
'' using 9 t 'dune', \
'' using 2 t 'minerva', \
'' using 4 t 'uboone', \
'' using 5 t 'mu2e'
