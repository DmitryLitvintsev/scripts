---
--- this query counts link, dirs and files in chimera
---
select date_trunc('day',imtime) as day,
       sum(case when itype = 40960 then 1 else 0 end) as links, 
       sum(case when itype = 16384 then 1 else 0 end) as dirs, 
       sum(case when itype = 32768 then 1 else 0 end) as files 
from 
     t_inodes 
group by day 
order by day asc;
