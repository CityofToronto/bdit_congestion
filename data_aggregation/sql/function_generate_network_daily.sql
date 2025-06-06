CREATE OR REPLACE FUNCTION congestion.generate_network_daily(_dt date)
RETURNS void
LANGUAGE 'sql'
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

WITH speed_links AS (
    SELECT
        segment_id,
        link_dir,
        links.length AS link_length,
        dt,
        extract(hour from tod)::int AS hr,
        harmean(mean) AS spd_avg,
        COUNT(tx)::int AS num_bin
    FROM here.ta
    INNER JOIN congestion.network_links_22_2 links USING (link_dir)
    WHERE
        dt >= _dt
        AND dt < _dt + interval '1 day'
    GROUP BY
        segment_id,
        link_dir,
        dt,
        hr,
        links.length
),

/*
Produces estimates of the average travel time for each 1 hour bin for each
individual segment (segment_id), where at least 80% of the segment (by distance)
has observations at the link (link_dir) level
*/
tt_hr AS (
    SELECT
        segment_id, 
        dt,
        hr,
        -- Adjusted to segment's length
        SUM(link_length / spd_avg  * 3.6 ) * total_length / SUM(link_length) AS tt,
        -- Not adjusted to segment's length, only summing up link_dir tt
        SUM(link_length / spd_avg  * 3.6 ) AS unadjusted_tt,\
        -- Sum of link_dir with data's length
        SUM(link_length) AS length_w_data,
        -- Adjusted tt valid to use if this value is True
        CASE
            WHEN SUM(link_length) >= 0.8 * total_length THEN True 
            ELSE False 
        END AS is_valid,
        sum(num_bin) AS num_bin
    FROM speed_links
    INNER JOIN congestion.network_segments USING (segment_id)
    GROUP BY
        segment_id,
        dt,
        hr,
        total_length
    ORDER BY
        segment_id,
        dt,
        hr
)

/*
Final Output: Inserts an estimate of the segment aggregation into
congestion.network_segments_daily
*/
INSERT INTO congestion.network_segments_daily
SELECT
    segment_id,
    dt,
    hr,
    round(tt::numeric, 2) AS tt,
    round(unadjusted_tt::numeric, 2) AS unadjusted_tt,
    length_w_data,
    is_valid,
    num_bin
FROM tt_hr

$BODY$;

ALTER FUNCTION congestion.generate_network_daily(date) OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_network_daily(date) TO congestion_admins;
GRANT EXECUTE ON FUNCTION congestion.generate_network_daily(date) TO congestion_bot;
REVOKE EXECUTE ON FUNCTION congestion.generate_network_daily(date) TO bdit_humans;

COMMENT ON FUNCTION congestion.generate_network_daily(date)
    IS 'Function that aggregate network segments hourly travel time for each day. Runs everyday through an airflow process.';