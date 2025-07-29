# Example of stacked time series plot

#set terminal pdf color solid
#set output 'migration.pdf'

set terminal postscript color solid
set output 'stores_and_restores.ps'
set title 'Stores / Restors on public system, weekly volume'

DATA = 'data/stores_restores.csv'


set key invert reverse right outside
set style data histogram
set style histogram rowstacked

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -90
set ylabel "TiB / week"
set xlabel "date (Y-m-d)"
set grid
set format x  "%Y-%m"
set datafile separator ","

#plot CDF using 4:xtic(1):xtic(1):xticlabel((int($0) % 5)==0? strftime("%Y-%m", strptime("%Y-%m-%d", strcol(1))):"")   t 'CDF', \
#D0 using 4  t 'D0', \
#CMS using ($4/1024./1024./1024./1024.)  t 'CMS', \
#PUBLIC using ($4/1024./1024./1024./1024.)  t 'Public'
#

plot DATA using 8:xtic(1):xtic(1):xticlabel((int($0) % 5)==0? strftime("%Y-%m-%d", strptime("%Y-%m-%d %H-%M-%S", strcol(1))):"")  t 'writes', \
     DATA using 4  t 'reads'
