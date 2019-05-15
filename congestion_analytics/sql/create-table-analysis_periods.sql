CREATE TABLE here_analysis.analysis_periods (
period_id smallint,
group_id smallint,
time_bin time without time zone
);


INSERT INTO here_analysis.analysis_periods VALUES (1,1,'08:00');
INSERT INTO here_analysis.analysis_periods VALUES (1,1,'08:30');
INSERT INTO here_analysis.analysis_periods VALUES (2,1,'17:00');
INSERT INTO here_analysis.analysis_periods VALUES (2,1,'17:30');
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