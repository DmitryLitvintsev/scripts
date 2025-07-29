with foo as (select date_trunc('week', to_timestamp(extract (epoch from datestamp ) + queuedtime / 1000 + connectiontime/1000))
 as day,
    sum(case when action = 'restore' then 1 else 0 end) as restores,
    sum(case when action = 'restore' then fullsize else 0 end)/1024./1024./1024./1024.  as restore_volume,
    sum(case when action = 'store' then 1 else 0 end) as stores,
    sum(case when action = 'store' then fullsize else 0 end)/1024./1024./1024./1024.  as store_volume
    from storageinfo where  errorcode=0 and datestamp > current_date - interval '6 months' group by day)
 select foo.day as week,
 foo.restores as daily_restores, sum(foo.restores) over (order by foo.day) as total_restores,
 to_char(foo.restore_volume, '9999D9') as daily_restore_volume,
 sum(foo.restore_volume) over (order by foo.day) as total_restore_volume,
 foo.stores as daily_stores, sum(foo.stores) over (order by foo.day) as total_stores,
 to_char(foo.store_volume, '9999D9') as daily_store_volume, sum(foo.store_volume) over (order by foo.day) as total_store_volume
 from foo order by foo.day asc;
