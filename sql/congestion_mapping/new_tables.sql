CREATE TABLE rdumas.congestion_metrics (
    tmc char(9) not null,
    hh smallint not null,
    agg_id smallint not null,
    agg_period date not null,
    tti real,
    bti real,
    UNIQUE(tmc, hh, agg_id, agg_period)
);

CREATE TABLE rdumas.aggregation_levels (
    agg_id smallserial,
    agg_level varchar(9) not null
);
    