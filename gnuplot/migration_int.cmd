# Example of stacked time series plot

set terminal pdf color solid
set output 'migration_int.pdf'
set title 'Migration of T10K media, monthly cumulative volume'

D0 = 'data/d0_writes_monthly_int.data'
CDF = 'data/cdf_writes_monthly_int.data'
CMS = 'data/cms_writes_monthly_int.data'
PUBLIC = 'data/public_writes_monthly_int.data'

set key invert reverse right outside
set style data histogram
set style histogram rowstacked

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2

set xtics border in scale 1,0.5 nomirror rotate by -90
set ylabel "TiB / month"
set xlabel "date (Y-m)"
set grid
set format x  "%Y-%m"

#xtic(1):xticlabel((int($0) % 5)==0? strftime("%m/%d %H", strptime("%Y-%m-%d %H:%M:%S", strcol(1))):"") 

plot CDF using 4:xtic(1):xtic(1):xticlabel((int($0) % 5)==0? strftime("%Y-%m", strptime("%Y-%m-%d", strcol(1))):"")   t 'CDF', \
D0 using 4  t 'D0', \
CMS using ($4/1024./1024./1024./1024.)  t 'CMS', \
PUBLIC using ($4/1024./1024./1024./1024.)  t 'Public'
