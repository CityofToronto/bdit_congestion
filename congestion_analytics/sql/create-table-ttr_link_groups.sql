CREATE TABLE here_analysis.ttr_link_groups (
	ttr_link_group_id smallint,
	ttr_range numrange,
	description text
);

INSERT INTO here_analysis.ttr_link_groups VALUES (1,'[0,0.5)','< 0.5');
INSERT INTO here_analysis.ttr_link_groups VALUES (2,'[0.5,0.6)','0.5 - 0.6');
INSERT INTO here_analysis.ttr_link_groups VALUES (3,'[0.6,0.7)','0.6 - 0.7');
INSERT INTO here_analysis.ttr_link_groups VALUES (4,'[0.7,0.8)','0.7 - 0.8');
INSERT INTO here_analysis.ttr_link_groups VALUES (5,'[0.8,0.9)','0.8 - 0.9');
INSERT INTO here_analysis.ttr_link_groups VALUES (6,'[0.9,0.97)','0.9 - 0.97');
INSERT INTO here_analysis.ttr_link_groups VALUES (7,'[0.97,1.03)','0.97 - 1.03');
INSERT INTO here_analysis.ttr_link_groups VALUES (8,'[1.03,1.10)','1.03 - 1.10');
INSERT INTO here_analysis.ttr_link_groups VALUES (9,'[1.10,1.20)','1.10 - 1.20');
INSERT INTO here_analysis.ttr_link_groups VALUES (10,'[1.20,1.30)','1.20 - 1.30');
INSERT INTO here_analysis.ttr_link_groups VALUES (11,'[1.30,1.40)','1.30 - 1.40');
INSERT INTO here_analysis.ttr_link_groups VALUES (12,'[1.40,1.50)','1.40 - 1.50');
INSERT INTO here_analysis.ttr_link_groups VALUES (13,'[1.50,1.75)','1.50 - 1.75');
INSERT INTO here_analysis.ttr_link_groups VALUES (14,'[1.75,2.00)','1.75 - 2.00');
INSERT INTO here_analysis.ttr_link_groups VALUES (15,'[2.00,100.00)','> 2.00');