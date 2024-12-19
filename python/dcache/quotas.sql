WITH RECURSIVE files(ino, type, fsize, uid, gid)
AS ( VALUES (pnfsid2inumber('00008212BDAEB7854A5E80A49FFA4167EAD9'), 16384, 0::BIGINT, 0::BIGINT, 0::BIGINT)
UNION SELECT i.inumber, i.itype, i.isize, i.iuid, i.igid
FROM t_dirs d, t_inodes i, files f
WHERE f.type=16384 AND
    d.iparent=f.ino AND
    d.iname != '.' AND
    d.iname != '..' AND
    i.inumber=d.ichild )
SELECT count(*), sum(f.fsize), f.uid, f.gid
FROM files f where f.type = 32768 group by f.uid, f.gid order by f.uid, f.gid;
