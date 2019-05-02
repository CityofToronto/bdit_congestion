SELECT analysis_id, street, start_crossstreet, end_crossstreet, mth, avg(tt_here) AS tt_here, avg(tt_bt) AS tt_bt
FROM here_analysis.bt_here_monthly_comparison
INNER JOIN bluetooth.segments USING (analysis_id)
WHERE group_id = 1 AND time_bin >= '16:00' AND time_bin < '19:00'
GROUP BY analysis_id, street, start_crossstreet, end_crossstreet, mth
ORDER BY analysis_id, mth