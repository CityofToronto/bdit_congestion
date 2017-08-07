SELECT DISTINCT B.group_id, B.street, B.direction, A.dt
FROM here_analysis.corridor_link_agg A
INNER JOIN here_analysis.corridors B USING (corridor_id)