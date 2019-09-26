CREATE TABLE here_analysis.analysis_periods (
period_id smallint,
group_id smallint,
time_bin time without time zone
);


-- AM Peak Period (8-9 a.m.)
INSERT INTO here_analysis.analysis_periods VALUES (1,1,'08:00');
INSERT INTO here_analysis.analysis_periods VALUES (1,1,'08:30');
-- PM Peak Period (5-6 p.m.)
INSERT INTO here_analysis.analysis_periods VALUES (2,1,'17:00');
INSERT INTO here_analysis.analysis_periods VALUES (2,1,'17:30');
-- Friday/Saturday Night (10 p.m. - 1 a.m.)
INSERT INTO here_analysis.analysis_periods VALUES (3,5,'22:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,5,'22:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,5,'23:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,5,'23:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'00:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'00:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'22:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'22:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'23:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'23:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,7,'00:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,7,'00:30');
-- AM Peak Period (7-10 a.m.)
INSERT INTO here_analysis.analysis_periods VALUES (4,1,'07:00');
INSERT INTO here_analysis.analysis_periods VALUES (4,1,'07:30');
INSERT INTO here_analysis.analysis_periods VALUES (4,1,'08:00');
INSERT INTO here_analysis.analysis_periods VALUES (4,1,'08:30');
INSERT INTO here_analysis.analysis_periods VALUES (4,1,'09:00');
INSERT INTO here_analysis.analysis_periods VALUES (4,1,'09:30');
-- PM Peak Period (4-7 p.m.)
INSERT INTO here_analysis.analysis_periods VALUES (5,1,'16:00');
INSERT INTO here_analysis.analysis_periods VALUES (5,1,'16:30');
INSERT INTO here_analysis.analysis_periods VALUES (5,1,'17:00');
INSERT INTO here_analysis.analysis_periods VALUES (5,1,'17:30');
INSERT INTO here_analysis.analysis_periods VALUES (5,1,'18:00');
INSERT INTO here_analysis.analysis_periods VALUES (5,1,'18:30');