---
--- This query finds duplicated location_cookies in file table
---

SELECT count(*),
       v.label,
       f.location_cookie
FROM file f
INNER JOIN volume v ON v.id=f.volume
WHERE f.deleted='n'
  AND v.media_type NOT IN ('null',
                           'disk')
  AND v.system_inhibit_0 != 'DELETED'
GROUP BY f.location_cookie,
         v.label
HAVING count(*)>1;
