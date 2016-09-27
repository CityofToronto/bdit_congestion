SELECT gid, tmc, roadname, geom
INTO rdumas.arterial_corridors
FROM gis.inrix_tmc_tor 
WHERE "roadname" IN ('Albion Rd','Bathurst St','Bayview Ave','Bloor St','Bloor St/Dundas St','College St','Don Mills Rd','Dufferin St','Dundas St','Eglinton Ave','Jane St','Jarvis St','King St','Kipling Ave','Lake Shore Blvd','Lake Shore Blvd/Harbour St','Markham Rd','Martin Grove Rd','Mount Pleasant Rd','ON-2/Kingston Rd','ON-2/Lake Shore Blvd','Queen St','Queensway','Sheppard Ave','St Clair Ave','Steeles Ave','The Queensway','Victoria Park Ave','Yonge St','York Mills Rd' )