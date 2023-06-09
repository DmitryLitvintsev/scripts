--
-- for each volume find the volume that have been migrated to it
--

SELECT  json_build_object('dst', v1.label,
                          'src', array_to_json(array_agg(distinct v.label)))
FROM FILE f
INNER JOIN file_migrate fm ON f.bfid = fm.src_bfid
INNER JOIN volume v ON v.id = f.volume
INNER JOIN FILE f1 ON f1.bfid = fm.dst_bfid
INNER JOIN volume v1 ON f1.volume = v1.id
WHERE f1.deleted = 'n'
GROUP BY v1.label;
