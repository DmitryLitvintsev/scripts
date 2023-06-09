--
-- for each migrated volume find out to what volumes it has been migrated
--

SELECT  json_build_object('src', v.label,
                          'dst', array_to_json(array_agg(distinct v1.label)))
FROM FILE f
INNER JOIN file_migrate fm ON f.bfid = fm.src_bfid
INNER JOIN volume v ON v.id = f.volume
INNER JOIN FILE f1 ON f1.bfid = fm.dst_bfid
INNER JOIN volume v1 ON f1.volume = v1.id
WHERE f1.deleted = 'n'
GROUP BY v.label;
