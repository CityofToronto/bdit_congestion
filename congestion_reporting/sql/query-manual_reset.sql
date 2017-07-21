truncate here_analysis.corridors;
truncate here_analysis.corridor_links;
alter sequence here_analysis.corridor_links_corridor_link_id_seq restart;
update here_analysis.corridor_load SET processed = 'F';
select populate_corridors_manual();
refresh materialized view here_analysis.corridor_links_geom;