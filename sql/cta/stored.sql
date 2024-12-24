select af.archive_file_id, ti.ipnfsid, ti.inumber  from archive_file af inner join t_inodes ti on ti.ipnfsid = af.ipnfsid
where ti.inumber not in (select inumber from t_locationinfo where itype = 0);
