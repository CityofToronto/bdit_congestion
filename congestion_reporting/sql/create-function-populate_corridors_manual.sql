CREATE OR REPLACE FUNCTION populate_corridors_manual() RETURNS void as $$

DECLARE
	corridor_rec RECORD;
	curr_corr_id INT;
	curr_geom geometry(MultiLineString, 32190);
	corr_length NUMERIC;
	curr_street VARCHAR;
BEGIN
	FOR corridor_rec IN SELECT corridor_load_id FROM here_analysis.corridor_load WHERE processed = FALSE ORDER BY corridor_load_id LOOP

		curr_corr_id := (SELECT coalesce(MAX(corridor_id),0) + 1 FROM here_analysis.corridors);
		curr_geom := (SELECT geom FROM here_analysis.corridor_load WHERE corridor_load_id = corridor_rec.corridor_load_id);
		curr_street := (SELECT CASE WHEN street_alt IS NULL THEN street ELSE CONCAT(street,' / ',street_alt) END FROM here_analysis.corridor_load WHERE corridor_load_id = corridor_rec.corridor_load_id);
		-- insert record into here_analysis.corridors
		INSERT INTO here_analysis.corridors(corridor_id, corridor_name, street, direction, intersection_start, intersection_end, corridor_load_id)
		SELECT 	curr_corr_id,
			CONCAT(curr_street,' ',A.direction,' - ',A.intersection_start,' to ',A.intersection_end) as corridor_name,
			curr_street,
			A.direction,
			A.intersection_start,
			A.intersection_end,
			A.corridor_load_id
		FROM here_analysis.corridor_load A WHERE corridor_load_id = corridor_rec.corridor_load_id;

		-- insert records into here_analysis.corridor_links
		INSERT INTO here_analysis.corridor_links(corridor_id, link_dir, seq, distance_km)
		SELECT	curr_corr_id as corridor_id,
			CONCAT(a.link_id,
				CASE WHEN ST_LineLocatePoint(st_linemerge(curr_geom),ST_StartPoint(ST_Transform(a.geom,32190))) <
					ST_LineLocatePoint(st_linemerge(curr_geom),ST_EndPoint(ST_Transform(a.geom,32190))) THEN 'F'
				ELSE 'T'
				END) AS link_dir,
			ROW_NUMBER() OVER (ORDER BY line_dist(ST_Transform(a.geom,32190), curr_geom)) AS seq,
			ST_Length(ST_Transform(a.geom,32190))/1000.0 AS distance_km
		FROM here_gis.streets_16_1 a
		INNER JOIN here_gis.streets_att_16_1 b USING (link_id)
		WHERE 	b.paved::text = 'Y' AND b.poiaccess::text = 'N' AND b.ar_auto::text = 'Y' AND b.ar_traff::text = 'Y'
			AND angle_diff(ST_Transform(a.geom,32190), curr_geom) < 10
			AND ST_DWithin(ST_Centroid(ST_Transform(a.geom,32190)), curr_geom, 15)
			AND (dist_frac(ST_Transform(a.geom,32190),curr_geom) < 0.65 OR ST_Length(ST_Transform(a.geom,32190)) <= 5)
			AND 	CASE WHEN ST_LineLocatePoint(st_linemerge(curr_geom),ST_StartPoint(ST_Transform(a.geom,32190))) <
					ST_LineLocatePoint(st_linemerge(curr_geom),ST_EndPoint(ST_Transform(a.geom,32190))) THEN b.dir_travel IN ('B','F')
				ELSE b.dir_travel IN ('B','T')
				END
		ORDER BY line_dist(ST_Transform(a.geom,32190), curr_geom) asc;

		corr_length := (SELECT SUM(distance_km) FROM here_analysis.corridor_links WHERE corridor_id = curr_corr_id);

		-- update length_km field in here_analysis.corridors
		UPDATE here_analysis.corridors
		SET length_km = corr_length
		WHERE corridor_id = curr_corr_id;
	END LOOP;

	FOR corridor_rec IN SELECT corridor_load_id FROM here_analysis.corridor_load WHERE processed = FALSE AND counter_direction = TRUE ORDER BY corridor_load_id LOOP

		curr_corr_id := (SELECT coalesce(MAX(corridor_id),0) + 1 FROM here_analysis.corridors);
		curr_geom := (SELECT ST_Reverse(geom) FROM here_analysis.corridor_load WHERE corridor_load_id = corridor_rec.corridor_load_id);
		curr_street := (SELECT CASE WHEN street_alt IS NULL THEN street ELSE CONCAT(street_alt,' / ',street) END FROM here_analysis.corridor_load WHERE corridor_load_id = corridor_rec.corridor_load_id);

		-- insert record into here_analysis.corridors
		INSERT INTO here_analysis.corridors(corridor_id, corridor_name, street, direction,  intersection_start, intersection_end, corridor_load_id)
		SELECT 	curr_corr_id,
			CONCAT(curr_street,' ', CASE 	WHEN A.direction = 'EB' THEN 'WB'
							WHEN A.direction = 'WB' THEN 'EB'
							WHEN A.direction = 'NB' THEN 'SB'
							WHEN A.direction = 'SB' THEN 'NB' END
				,' - ',A.intersection_end,' to ',A.intersection_start) as corridor_name,
			curr_street,
			CASE 	WHEN A.direction = 'EB' THEN 'WB'
							WHEN A.direction = 'WB' THEN 'EB'
							WHEN A.direction = 'NB' THEN 'SB'
							WHEN A.direction = 'SB' THEN 'NB' END AS direction,
			A.intersection_start,
			A.intersection_end,
			A.corridor_load_id
		FROM here_analysis.corridor_load A WHERE corridor_load_id = corridor_rec.corridor_load_id;

		-- insert records into here_analysis.corridor_links
		INSERT INTO here_analysis.corridor_links(corridor_id, link_dir, seq, distance_km)
		SELECT	curr_corr_id as corridor_id,
			CONCAT(A.link_id,
				CASE WHEN ST_LineLocatePoint(st_linemerge(curr_geom),ST_StartPoint(ST_Transform(a.geom,32190))) <
					ST_LineLocatePoint(st_linemerge(curr_geom),ST_EndPoint(ST_Transform(a.geom,32190))) THEN 'F'
				ELSE 'T'
				END) AS link_dir,
			ROW_NUMBER() OVER (ORDER BY line_dist(ST_Transform(a.geom,32190), curr_geom)) AS seq,
			ST_Length(ST_Transform(a.geom,32190))/1000.0 AS distance_km
		FROM here_gis.streets_16_1 a
		INNER JOIN here_gis.streets_att_16_1 b USING (link_id)
		WHERE 	b.paved::text = 'Y' AND b.poiaccess::text = 'N' AND b.ar_auto::text = 'Y' AND b.ar_traff::text = 'Y'
			AND angle_diff(ST_Transform(a.geom,32190), curr_geom) < 10
			AND ST_DWithin(ST_Centroid(ST_Transform(a.geom,32190)), curr_geom, 15)
			AND (dist_frac(ST_Transform(a.geom,32190),curr_geom) < 0.65 OR ST_Length(ST_Transform(a.geom,32190)) <= 5)
			AND 	CASE WHEN ST_LineLocatePoint(st_linemerge(curr_geom),ST_StartPoint(ST_Transform(a.geom,32190))) <
					ST_LineLocatePoint(st_linemerge(curr_geom),ST_EndPoint(ST_Transform(a.geom,32190))) THEN b.dir_travel IN ('B','F')
				ELSE b.dir_travel IN ('B','T')
				END
		ORDER BY line_dist(ST_Transform(a.geom,32190), curr_geom) asc;
		corr_length := (SELECT SUM(distance_km) FROM here_analysis.corridor_links WHERE corridor_id = curr_corr_id);

		-- update length_km field in here_analysis.corridors
		UPDATE here_analysis.corridors
		SET length_km = corr_length
		WHERE corridor_id = curr_corr_id;

		UPDATE here_analysis.corridor_load
		SET processed = TRUE
		WHERE corridor_load_id = corridor_rec.corridor_load_id;
		
	END LOOP;

	UPDATE here_analysis.corridors A
	SET num_links = (SELECT COUNT(*) FROM here_analysis.corridor_links B WHERE B.corridor_id = A.corridor_id);

	UPDATE here_analysis.corridor_links A
	SET tot_distance_km = (SELECT SUM(distance_km) FROM here_analysis.corridor_links B WHERE B.corridor_id = A.corridor_id AND A.seq >= B.seq);
	
END;

$$ LANGUAGE plpgsql;