CREATE OR REPLACE FUNCTION congestion.update_street_name(
	new_ver text,
    centreline_ver text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $$
DECLARE
    new_table text := 'temp_congestion_centreline_' || new_ver;
    centreline_table text := 'centreline_' || centreline_ver;
BEGIN
	EXECUTE format('with all_ints as (
		select distinct intersection_id, linear_name_full, feature_code
		from ( select from_intersection_id AS intersection_id, linear_name_full, feature_code
				from gis_core.%I
				union
				select to_intersection_id, linear_name_full, feature_code
				from gis_core.%I
				)b)
		
		, get_road_name as (
		select segment_id, from_int, to_int,
		array_agg(distinct linear_name_full)::text[] centreline_list
		FROM (	SELECT segment_id, from_int, to_int, unnest(centreline_ids) as centreline_id
				FROM congestion.%I
				) temp
		left join gis_core.%I using (centreline_id)
		group by segment_id, from_int, to_int)
		
		, get_intersection_name as (
		select 
			segment_id, from_int,
			array_agg(a.linear_name_full order by a.feature_code)::text[] as from_name,
			array_agg(b.linear_name_full order by b.feature_code)::text[] as to_name,
			to_int, centreline_list
		from get_road_name
		left join all_ints a on from_int = a.intersection_id
		left join all_ints b on to_int = b.intersection_id			
		group by segment_id, from_int, to_int, centreline_list)
		
		, final_output AS (
		SELECT 
			segment_id, 
			from_int,
			case when (array_diff(from_name, centreline_list))[1] is null then array_to_string(from_name, ''/'')
				else array_to_string(array[(array_diff(from_name, centreline_list))[1]], ''/'') end as from_name_final,
			to_int,
			case when (array_diff(to_name, centreline_list))[1] is null then array_to_string(to_name, ''/'')
				else array_to_string(array[(array_diff(to_name, centreline_list))[1]], ''/'') end as to_name_final,
				array_to_string(centreline_list, ''/'') AS centreline_name
		from get_intersection_name)

		UPDATE congestion.%I a
		set from_int_desc = from_name_final, 
		to_int_desc = to_name_final,
		streetname = centreline_name
		from final_output
		WHERE a.segment_id = final_output.segment_id;
		'
       ,centreline_table, centreline_table, new_table, centreline_table, new_table
    );

END;
$$;

ALTER FUNCTION congestion.update_street_name(text, text)
    OWNER TO congestion_admins;

COMMENT ON FUNCTION  congestion.update_street_name(text, text)
IS 'Functions to update street name and intersection names';

