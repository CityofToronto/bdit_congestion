CREATE TABLE here_analysis.analysis_periods (
period_id smallint,
group_id smallint,
time_bin time without time zone
);

INSERT INTO here_analysis.analysis_periods VALUES (1,1,'08:00');
INSERT INTO here_analysis.analysis_periods VALUES (1,1,'08:30');
INSERT INTO here_analysis.analysis_periods VALUES (2,1,'17:00');
INSERT INTO here_analysis.analysis_periods VALUES (2,1,'17:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'01:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'01:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,6,'02:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,7,'01:00');
INSERT INTO here_analysis.analysis_periods VALUES (3,7,'01:30');
INSERT INTO here_analysis.analysis_periods VALUES (3,7,'02:00');
