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
    FROM here.street_valid_range sv
    WHERE _dt <@ sv.valid_range;

    IF street_version IS NULL THEN
        RAISE EXCEPTION 'street version does not exist';
    END IF;

    -- Now construct network_table using street_version
    network_table := 'network_links_' || street_version;

    EXECUTE format($$
        WITH speed_links AS (
            SELECT
                segment_id,
                link_dir,
                links.length AS link_length,
                dt,
                extract(hour from tod)::int AS hr,
                harmean(mean) AS spd_avg,
                COUNT(tx)::int as num_bin
            FROM here.ta_path
            INNER JOIN congestion.%1$I links USING (link_dir)
            WHERE
                dt >= %2$L
                AND dt < %2$L
            GROUP BY
                segment_id,
                link_dir,
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
                segment_id, 
                dt,
                hr,
                avg(SUM(link_length / spd_avg  * 3.6 )/SUM(link_length)) AS spd,
                SUM(link_length) AS length_w_data, -- Sum of link_dir with data's length  
                total_length,
                CASE WHEN SUM(link_length) >= 0.8 * total_length 
                    THEN True
                    ELSE False
                END AS is_valid, -- Adjusted tt valid to use if this value is True
                sum(num_bin) AS num_bin
            FROM speed_links
            -- using length of the most updated version
            INNER JOIN (
                SELECT segment_id, sum(length) AS total_length
                FROM congestion.%1$I
                GROUP BY segment_id
            ) AS a USING (segment_id)
            GROUP BY
                segment_id,
                dt,
                hr,
                total_length
        )

        /*
        Final Output: Inserts a speed estimate of the segment aggregation into congestion.network_segments_daily
        */
        INSERT INTO congestion.network_segments_daily_spd
        SELECT
            tt_hr.segment_id,
            tt_hr.dt,
            tt_hr.hr,
            round(tt_hr.spd_avg, 2) as spd,
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
                %2$L >= valid_from
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
