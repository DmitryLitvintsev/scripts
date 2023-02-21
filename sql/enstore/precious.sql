SELECT count(*),
       v.storage_group,
       v.file_family
FROM FILE f
INNER JOIN volume v ON v.id = f.volume
INNER JOIN file_migrate fm ON f.bfid = fm.src_bfid
WHERE fm.dst_bfid IS NULL
GROUP BY v.storage_group,
         v.file_family
