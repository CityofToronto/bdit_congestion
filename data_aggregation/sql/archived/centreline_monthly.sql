-- Parent table structure

CREATE TABLE IF NOT EXISTS congestion.centreline_monthly
(
    uid integer NOT NULL,
    mth date NOT NULL,
    hr integer NOT NULL,
    day_type text,
    avg_tt numeric NOT NULL,
    median_tt numeric NOT NULL,
    pct_85_tt numeric NOT NULL,
    pct_95_tt numeric NOT NULL,
    min_tt numeric NOT NULL,
    max_tt numeric NOT NULL,
    std_dev numeric NOT NULL,
    num_bins integer NOT NULL,
    py_pct_10_tt numeric NOT NULL,
    py_pct_25_tt numeric NOT NULL

) PARTITION BY RANGE (mth);

ALTER TABLE congestion.centreline_monthly OWNER TO congestion_admins;
GRANT SELECT ON TABLE congestion.centreline_monthly TO bdit_humans;

COMMENT ON TABLE congestion.centreline_monthly
IS 'Table that contains centreline sets equivalent of network segments hourly travel time and travel time index for each monthly. Partition yearly using mth column. New monthly data automatically aggregated through an airflow process.';
