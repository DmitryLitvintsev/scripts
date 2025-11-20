# Example of stacked time series plot

set terminal pdf color solid
set output 'migration_new.pdf'
set title 'Media Migration using dCache, monthly volume'

D0 = 'data/d0_writes_monthly_new.data'
CDF = 'data/cdf_writes_monthly_new.data'
CMS = 'data/cms_writes_monthly_new.data'
PUBLIC = 'data/public_writes_monthly_new.data'

set key invert reverse right outside
set style data histogram
set style histogram rowstacked

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -90
set ylabel "TiB / month"
set xlabel "date (Y-M)"
set grid
set format x  "%Y-%m"

set label  "Total: 64 PiB" at 40,6000

#xtic(1):xticlabel((int($0) % 5)==0? strftime("%m/%d %H", strptime("%Y-%m-%d %H:%M:%S", strcol(1))):"") 

plot CDF using 4:xtic(1):xtic(1):xticlabel((int($0) % 5)==0? strftime("%Y-%m", strptime("%Y-%m-%d", strcol(1))):"")   t 'CDF', \
D0 using 4  t 'D0', \
CMS using ($4/1024./1024./1024./1024.)  t 'CMS', \
PUBLIC using ($4/1024./1024./1024./1024.)  t 'Public'
