# Example of stacked time series plot

set terminal pdf color solid
set output 'listing_public_ftp.pdf'
set title 'FTP LIST requests on Public dCache daily. One door. Multiply by 11'

FILE = 'data/list_public.data'

set key invert reverse right outside
set style data histogram
set style histogram rowstacked

set boxwidth 0.75 absolute
set style fill solid 1.00 border -1
set xtics border in scale 1,0.5 nomirror
set mxtics 2
#set log y

set xtics border in scale 1,0.5 nomirror rotate by -90
set ylabel "Requests / day"
set xlabel "date (Y-m-d)"
set grid
set format x  "%Y-%m-%d"

#xtic(1):xticlabel((int($0) % 5)==0? strftime("%m/%d %H", strptime("%Y-%m-%d %H:%M:%S", strcol(1))):"") 


plot FILE using 3:xtic(1):xtic(1):xticlabel((int($0) % 5)==0? strftime("%Y-%m-%d", strptime("%Y-%m-%d", strcol(1))):"")   t 'fail', \
FILE using ($2-$3 ) t 'success'
