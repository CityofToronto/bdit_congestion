CREATE TABLE here_analysis.agg_30min_hc
(
  link_dir text NOT NULL,
  num_bins smallint NOT NULL,
  datetime_bin timestamp without time zone NOT NULL,
  spd_avg numeric(8,3) NOT NULL
);

CREATE TABLE here_analysis.agg_30min_hc_201401 (
	CHECK ( datetime_bin >= '2014-01-01' AND datetime_bin < '2014-02-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201402 (
	CHECK ( datetime_bin >= '2014-02-01' AND datetime_bin < '2014-03-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201403 (
	CHECK ( datetime_bin >= '2014-03-01' AND datetime_bin < '2014-04-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201404 (
	CHECK ( datetime_bin >= '2014-04-01' AND datetime_bin < '2014-05-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201405 (
	CHECK ( datetime_bin >= '2014-05-01' AND datetime_bin < '2014-06-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201406 (
	CHECK ( datetime_bin >= '2014-06-01' AND datetime_bin < '2014-07-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201407 (
	CHECK ( datetime_bin >= '2014-07-01' AND datetime_bin < '2014-08-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201408 (
	CHECK ( datetime_bin >= '2014-08-01' AND datetime_bin < '2014-09-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201409 (
	CHECK ( datetime_bin >= '2014-09-01' AND datetime_bin < '2014-10-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201410 (
	CHECK ( datetime_bin >= '2014-10-01' AND datetime_bin < '2014-11-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201411 (
	CHECK ( datetime_bin >= '2014-11-01' AND datetime_bin < '2014-12-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201412 (
	CHECK ( datetime_bin >= '2014-12-01' AND datetime_bin < '2015-01-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201501 (
	CHECK ( datetime_bin >= '2015-01-01' AND datetime_bin < '2015-02-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201502 (
	CHECK ( datetime_bin >= '2015-02-01' AND datetime_bin < '2015-03-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201503 (
	CHECK ( datetime_bin >= '2015-03-01' AND datetime_bin < '2015-04-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201504 (
	CHECK ( datetime_bin >= '2015-04-01' AND datetime_bin < '2015-05-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201505 (
	CHECK ( datetime_bin >= '2015-05-01' AND datetime_bin < '2015-06-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201506 (
	CHECK ( datetime_bin >= '2015-06-01' AND datetime_bin < '2015-07-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201507 (
	CHECK ( datetime_bin >= '2015-07-01' AND datetime_bin < '2015-08-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201508 (
	CHECK ( datetime_bin >= '2015-08-01' AND datetime_bin < '2015-09-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201509 (
	CHECK ( datetime_bin >= '2015-09-01' AND datetime_bin < '2015-10-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201510 (
	CHECK ( datetime_bin >= '2015-10-01' AND datetime_bin < '2015-11-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201511 (
	CHECK ( datetime_bin >= '2015-11-01' AND datetime_bin < '2015-12-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201512 (
	CHECK ( datetime_bin >= '2015-12-01' AND datetime_bin < '2016-01-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201601 (
	CHECK ( datetime_bin >= '2016-01-01' AND datetime_bin < '2016-02-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201602 (
	CHECK ( datetime_bin >= '2016-02-01' AND datetime_bin < '2016-03-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201603 (
	CHECK ( datetime_bin >= '2016-03-01' AND datetime_bin < '2016-04-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201604 (
	CHECK ( datetime_bin >= '2016-04-01' AND datetime_bin < '2016-05-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201605 (
	CHECK ( datetime_bin >= '2016-05-01' AND datetime_bin < '2016-06-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201606 (
	CHECK ( datetime_bin >= '2016-06-01' AND datetime_bin < '2016-07-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201607 (
	CHECK ( datetime_bin >= '2016-07-01' AND datetime_bin < '2016-08-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201608 (
	CHECK ( datetime_bin >= '2016-08-01' AND datetime_bin < '2016-09-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201609 (
	CHECK ( datetime_bin >= '2016-09-01' AND datetime_bin < '2016-10-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201610 (
	CHECK ( datetime_bin >= '2016-10-01' AND datetime_bin < '2016-11-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201611 (
	CHECK ( datetime_bin >= '2016-11-01' AND datetime_bin < '2016-12-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201612 (
	CHECK ( datetime_bin >= '2016-12-01' AND datetime_bin < '2017-01-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201701 (
	CHECK ( datetime_bin >= '2017-01-01' AND datetime_bin < '2017-02-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201702 (
	CHECK ( datetime_bin >= '2017-02-01' AND datetime_bin < '2017-03-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201703 (
	CHECK ( datetime_bin >= '2017-03-01' AND datetime_bin < '2017-04-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201704 (
	CHECK ( datetime_bin >= '2017-04-01' AND datetime_bin < '2017-05-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201705 (
	CHECK ( datetime_bin >= '2017-05-01' AND datetime_bin < '2017-06-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201706 (
	CHECK ( datetime_bin >= '2017-06-01' AND datetime_bin < '2017-07-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201707 (
	CHECK ( datetime_bin >= '2017-07-01' AND datetime_bin < '2017-08-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201708 (
	CHECK ( datetime_bin >= '2017-08-01' AND datetime_bin < '2017-09-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201709 (
	CHECK ( datetime_bin >= '2017-09-01' AND datetime_bin < '2017-10-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201710 (
	CHECK ( datetime_bin >= '2017-10-01' AND datetime_bin < '2017-11-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201711 (
	CHECK ( datetime_bin >= '2017-11-01' AND datetime_bin < '2017-12-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201712 (
	CHECK ( datetime_bin >= '2017-12-01' AND datetime_bin < '2018-01-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201801 (
	CHECK ( datetime_bin >= '2018-01-01' AND datetime_bin < '2018-02-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201802 (
	CHECK ( datetime_bin >= '2018-02-01' AND datetime_bin < '2018-03-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201803 (
	CHECK ( datetime_bin >= '2018-03-01' AND datetime_bin < '2018-04-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201804 (
	CHECK ( datetime_bin >= '2018-04-01' AND datetime_bin < '2018-05-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201805 (
	CHECK ( datetime_bin >= '2018-05-01' AND datetime_bin < '2018-06-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201806 (
	CHECK ( datetime_bin >= '2018-06-01' AND datetime_bin < '2018-07-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201807 (
	CHECK ( datetime_bin >= '2018-07-01' AND datetime_bin < '2018-08-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201808 (
	CHECK ( datetime_bin >= '2018-08-01' AND datetime_bin < '2018-09-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201809 (
	CHECK ( datetime_bin >= '2018-09-01' AND datetime_bin < '2018-10-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201810 (
	CHECK ( datetime_bin >= '2018-10-01' AND datetime_bin < '2018-11-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201811 (
	CHECK ( datetime_bin >= '2018-11-01' AND datetime_bin < '2018-12-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201812 (
	CHECK ( datetime_bin >= '2018-12-01' AND datetime_bin < '2019-01-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201901 (
	CHECK ( datetime_bin >= '2019-01-01' AND datetime_bin < '2019-02-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201902 (
	CHECK ( datetime_bin >= '2019-02-01' AND datetime_bin < '2019-03-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201903 (
	CHECK ( datetime_bin >= '2019-03-01' AND datetime_bin < '2019-04-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201904 (
	CHECK ( datetime_bin >= '2019-04-01' AND datetime_bin < '2019-05-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201905 (
	CHECK ( datetime_bin >= '2019-05-01' AND datetime_bin < '2019-06-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201906 (
	CHECK ( datetime_bin >= '2019-06-01' AND datetime_bin < '2019-07-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201907 (
	CHECK ( datetime_bin >= '2019-07-01' AND datetime_bin < '2019-08-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201908 (
	CHECK ( datetime_bin >= '2019-08-01' AND datetime_bin < '2019-09-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201909 (
	CHECK ( datetime_bin >= '2019-09-01' AND datetime_bin < '2019-10-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201910 (
	CHECK ( datetime_bin >= '2019-10-01' AND datetime_bin < '2019-11-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201911 (
	CHECK ( datetime_bin >= '2019-11-01' AND datetime_bin < '2019-12-01' )
) INHERITS (here_analysis.agg_30min_hc);
CREATE TABLE here_analysis.agg_30min_hc_201912 (
	CHECK ( datetime_bin >= '2019-12-01' AND datetime_bin < '2020-01-01' )
) INHERITS (here_analysis.agg_30min_hc);