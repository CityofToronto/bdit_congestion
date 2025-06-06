CREATE OR REPLACE FUNCTION congestion.generate_network_daily_spd(
    _dt date
)
RETURNS void
LANGUAGE plpgsql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
DECLARE 
    network_table TEXT;
    street_version TEXT;  -- Declare street_version
BEGIN
    -- Add some checks before running
    IF (SELECT COUNT(1) FROM here.ta_path WHERE dt = _dt) = 0 THEN
        RAISE EXCEPTION 'No Data in ta_path'; 
    END IF;
    
    IF _dt < '2017-09-01' THEN 
        RAISE EXCEPTION 'Date out of bound'; 
    END IF;

    SELECT sv.street_version
    INTO street_version
    FROM here.street_valid_range_path sv
    WHERE _dt <@ sv.valid_range;

    IF street_version IS NULL THEN
        RAISE EXCEPTION 'street version does not exist';
    END IF;

    -- Now construct network_table using street_version
    network_table := 'network_links_' || street_version;

    EXECUTE format($$
        WITH speed_links AS (
            SELECT
                links.segment_id,
                links.link_dir,
                links.length AS link_length,
                ta_path.dt,
                extract(hour from ta_path.tod)::int AS hr,
                harmean(ta_path.mean) AS spd_avg,
                COUNT(ta_path.tx)::int AS num_bin
            FROM here.ta_path
            INNER JOIN congestion.%1$I AS links USING (link_dir)
            WHERE
                ta_path.dt >= %2$L::date
                AND ta_path.dt < %2$L::date + INTERNAL '1 day'
            GROUP BY
                links.segment_id,
                links.link_dir,
                dt,
                hr,
                links.length
        ),

        /*
        tt_hr: Produces estimates of the average travel time for each 1 hour bin for each individual segment (segment_id), 
                where at least 80 percent of the segment (by distance) has observations at the link (link_dir) level
        */
        tt_hr AS (
            SELECT
                speed_links.segment_id, 
                speed_links.dt,
                speed_links.hr,
                SUM(speed_links.link_length)/SUM(speed_links.link_length / speed_links.spd_avg  * 3.6 ) AS spd,
                SUM(speed_links.link_length) AS length_w_data, -- Sum of link_dir with data's length  
                get_length.total_length,
                CASE WHEN SUM(speed_links.link_length) >= 0.8 * get_length.total_length 
                    THEN True
                    ELSE False
                END AS is_valid, -- Adjusted tt valid to use if this value is True
                sum(speed_links.num_bin) AS num_bin
            FROM speed_links
            -- using length of the most updated version
            INNER JOIN (
                SELECT segment_id, sum(length) AS get_length.total_length
                FROM congestion.%1$I
                GROUP BY segment_id
            ) AS get_length USING (segment_id)
            GROUP BY
                speed_links.segment_id,
                speed_links.dt,
                speed_links.hr,
                get_length.total_length
        )

        /*
        Final Output: Inserts a speed estimate of the segment aggregation into congestion.network_segments_daily
        */
        INSERT INTO congestion.network_segments_daily_spd
        SELECT
            tt_hr.segment_id,
            tt_hr.dt,
            tt_hr.hr,
            round(tt_hr.spd, 2) as spd,
            tt_hr.length_w_data,
            tt_hr.total_length,
            tt_hr.is_valid,
            tt_hr.num_bin
        FROM tt_hr
        -- only for valid segments during aggregation date
        LEFT JOIN congestion.network_segments_retired retired USING (segment_id)
        WHERE
            retired.segment_id IS NULL
            OR ( 
                retired.segment_id IS NOT NULL
                AND %2$L >= valid_from
                AND %2$L < valid_to
            );
        $$ , network_table, _dt);

END;
$BODY$;

ALTER FUNCTION congestion.generate_network_daily_spd(date)
OWNER TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_network_daily_spd(date) TO congestion_admins;

GRANT EXECUTE ON FUNCTION congestion.generate_network_daily_spd(date) TO congestion_bot;

COMMENT ON FUNCTION congestion.generate_network_daily_spd(date)
IS 'Function that aggregate network segments hourly speed for each day.';
