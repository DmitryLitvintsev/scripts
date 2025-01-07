WITH RECURSIVE files(ino, pnfsid, type, fsize, creation_time)
AS ( VALUES (pnfsid2inumber('000500000000000000214240'), '000500000000000000214240', 16384, 0::BIGINT, 0::BIGINT)
UNION SELECT i.inumber, i.ipnfsid, i.itype, i.isize, cast( extract( epoch from i.icrtime) as BIGINT)*1000
FROM t_dirs d, t_inodes i, files f
WHERE f.type=16384 AND 
    d.iparent=f.ino AND
    d.iname != '.' AND
    d.iname != '..' AND
    i.inumber=d.ichild ) 
SELECT '/xenon.biggrid.nl', '*', 5, f.fsize::bigint, f.creation_time, f.pnfsid, 2
FROM files f where f.type = 32768

