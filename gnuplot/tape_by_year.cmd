# Example of stacked time series plot

set terminal pdf color solid
set output 'tape_by_year.pdf'
set title 'yealy writes to Enstore'

FILE = 'data/yearly_writes_cms.data'

set key invert reverse right outside
set style data histogram
set style histogram rowstacked

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -45
set ylabel "PiB / year"
set xlabel "year"
set grid
#set format x  "%Y-%m"

set datafile separator ","


plot FILE using ($2/1024):xtic(1):xtic(1):xticlabel((int($0) % 2)==0? strftime("%Y", strptime("%Y-%m-%d", strcol(1))):"")   t 'CMS', '' using ($3/1024)  t 'other'
