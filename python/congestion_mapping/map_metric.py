#map_metric.py
#! python3
"""Automate printings maps of congestion metrics using PyQGIS

This command line utility can automate cycling over multiple years, hours of
the day, and metrics to load layers from the PostgreSQL DB, add them to a 
template map and then print them to a png. 

usage: map_metric.py [-h] -r YYYYMM YYYYMM
                     (-p TIMEPERIOD [TIMEPERIOD ...] | -i HOURS_ITERATE HOURS_IT
ERATE)
                     [-d DBSETTING] [-t TABLENAME]
                     {b,t} [{b,t} ...] {year,quarter,month}

Produce maps of congestion metrics (tti, bti) for different aggregation
periods, timeperiods, and aggregation levels

positional arguments:
  {b,t}                 Map either Buffer Time Index, Travel Time Index or
                        both e.g. b, t, or 'b t'
  {year,quarter,month}  Aggregation level to be used

optional arguments:
  -h, --help            show this help message and exit
  -r YYYYMM YYYYMM, --range YYYYMM YYYYMM
                        Range of months (YYYYMM) to operate overfrom startdate
                        to enddate. Accepts multiple pairs
  -p TIMEPERIOD [TIMEPERIOD ...], --timeperiod TIMEPERIOD [TIMEPERIOD ...]
                        Timeperiod of aggregation, use 1 arg for 1 hour or 2
                        args for a range
  -i HOURS_ITERATE HOURS_ITERATE, --hours_iterate HOURS_ITERATE HOURS_ITERATE
                        Hours to iterate over
  -d DBSETTING, --dbsetting DBSETTING
                        Filename with connection settings to the
                        database(default: opens default.cfg)
  -t TABLENAME, --tablename TABLENAME
                        Table containing metrics congestion.metrics
"""

#TODO import stuff
import argparse
import json
import sys
import logging
import re
from datetime import time
from parsing_utils import parse_args, _validate_multiple_yyyymm_range
SQLS = {#'month':"",
        'year':"""(SELECT row_number() OVER (PARTITION BY metrics.agg_period ORDER BY metrics.{metric} DESC) AS "Rank",
                    tmc_from_to_lookup.street_name AS "Street",
                    inrix_tmc_tor.direction AS "Dir",
                    tmc_from_to_lookup.from_to AS "From - To",
                    to_char(metrics.bti, '9D99'::text) AS "Buffer Time Index",
                    to_char(metrics.tti, '9D99'::text) AS "Travel Time Index",
                    metrics.agg_period AS "Year",
                    inrix_tmc_tor.geom,
                    inrix_tmc_tor.gid
                  FROM congestion.metrics
                    JOIN congestion.aggregation_levels USING (agg_id)
                    JOIN gis.inrix_tmc_tor USING (tmc)
                    JOIN gis.tmc_from_to_lookup USING (tmc)
                  WHERE inrix_tmc_tor.sum_miles > 0.124274 AND aggregation_levels.agg_level = 'year'
                  AND metrics.timeperiod = {timeperiod} AND metrics.agg_period = {agg_period}::DATE
                  ORDER BY metrics.{metric} DESC LIMIT 50)"""#,
#        'quarter':''
}

METRICS = {'b':'bti',
           't':'tti'}

def _new_uri(dbset):
    '''Create a new URI based on the database settings and return it
    
    Args:
        dbset: dictionary of database connection settings
        
    Returns:
        PyQGIS uri object'''
    uri = QgsDataSourceURI()
    uri.setConnection(dbset['host'], "5432", dbset['database'], dbset['user'], dbset['password'])
    return uri

def _get_agg_layer(uri, agg_level = None, agg_period = None, timeperiod = None, layername = None, metric = None):
    '''Create a QgsVectorLayer from a connection and specified parameters
    
    Args:
        uri: PyQGIS uri object
        agg_level: string representing aggregation level, key to SQLS dict
        agg_period: the starting aggregation date for the period as a string
            digestible by PostgreSQL into a DATE
        timeperiod: string representing a PostgreSQL timerange
        layername: string name to give the layer
        
    Returns:
        QgsVectorLayer from the specified sql query with provided layername'''
    if agg_level not in SQLS:
        raise ValueError('Aggregation level: {agg_level} not implemented'.format(agg_level=agg_level))
        
    sql = SQLS[agg_level]
    sql = sql.format(timeperiod = timeperiod, agg_period = agg_period, metric = metric)
    uri.setDataSource("", sql, "geom", "", "gid")
    return QgsVectorLayer(uri.uri(False), layername, 'postgres')


if __name__ == '__main__':
    
    ARGS = parse_args(sys.argv[1:])
    
    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read(ARGS.dbsetting)
    dbset = CONFIG['DBSETTINGS']
    
    try:
        YEARS = _validate_multiple_yyyymm_range(ARGS.years)
    except ValueError as err:
        LOGGER.critical(str(err))
        sys.exit(2)
    #TODO load map template
    URI = _new_uri(dbset)

    for metric in ARGS.metric:
        
        for year in YEARS:
            for month in YEARS[year]:
                yyyymmdd = get_yyyymmdd(year, month)
                if ARGS.hours_iterate:
                    hour_iterator = range(ARGS.hours_iterate[0],ARGS.hours_iterate[1]+1)
                else:
                    hour_iterator = range(ARGS.timeperiod[0],ARGS.timeperiod[0]+1)
                for hour1 in hour_iterator:
                    hour2 = hour1 + 1 if ARGS.hours_iterate else ARGS.timeperiod[1] 
                    timerange = _get_timerange(hour1, hour2)
                    layername = year + month + 'h' + hour1 + ARGS.agg_level
                    layer = _get_agg_layer(URI, agg_level = ARGS.agg_level,
                                   agg_period = yyyymmdd,
                                   timeperiod = timerange,
                                    metric = 
                                   layername = layername)
            
            #TODO Processing stuff