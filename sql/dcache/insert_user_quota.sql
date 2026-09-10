INSERT into t_user_quota
(iuid, ireplica_used, icustodial_used, ioutput_used)
SELECT ti.iuid, SUM(CASE WHEN ti.iretention_policy = 2 THEN isize ELSE 0 END) AS replica, 
                SUM(CASE WHEN ti.iretention_policy = 1 THEN isize ELSE 0 END) AS output,
                SUM(CASE WHEN ti.iretention_policy = 0 THEN isize ELSE 0 END) AS custodial 
FROM t_inodes ti WHERE ti.iuid NOT IN  (SELECT iuid FROM t_user_quota) AND ti.itype=32768 GROUP BY ti.iuid;
