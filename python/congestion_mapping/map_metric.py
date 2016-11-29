#! python3
#import stuff
import argparse
import json
import sys
import logging
import re
from datetime import time

SQLS = {'month':"",
        'year':"""(SELECT row_number() OVER (PARTITION BY metrics.agg_period ORDER BY metrics.bti DESC) AS "Rank",
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
                  ORDER BY metrics.bti DESC LIMIT 50)""",
        'quarter':''}

def parse_args(args):
    '''Parser for the command line arguments'''
    PARSER = argparse.ArgumentParser(description='Produce maps of congestion metrics (tti, bti) for '
                                                 'different aggregation periods, timeperiods, and '
                                                 'aggregation levels')

    PARSER.add_argument('Metric', choices=['b', 't'], nargs='+',
                        help="Map either Buffer Time Index, Travel Time Index or both e.g. b, t, or 'b t'")

    PARSER.add_argument("Aggregation_level", choices=['year', 'quarter', 'month'],
                        help="Aggregation level to be used")

    PARSER.add_argument("-r", "--range", nargs=2, action='append',
                        help="Range of months (YYYYMM) to operate over"
                        "from startdate to enddate. Accepts multiple pairs",
                        metavar=('YYYYMM', 'YYYYMM'), required=True)

    TIMEPERIOD = PARSER.add_mutually_exclusive_group(required=True)
    TIMEPERIOD.add_argument("-p", "--timeperiod", nargs='+', type=int,
                            help="Timeperiod of aggregation, use 1 arg for 1 hour or 2 args for a range")
    TIMEPERIOD.add_argument("-i","--hours_iterate", nargs=2, type=int,
                            help="Hours to iterate over")

    PARSER.add_argument("-d", "--dbsetting",
                        default='default.cfg',
                        help="Filename with connection settings to the database"
                        "(default: opens %(default)s)")
    PARSER.add_argument("-t", "--tablename",
                        default='congestion.metrics',
                        help="Table containing metrics %(default)s")
    parsed_args = PARSER.parse_args(args)
    #    if not (parsed_args.bti or parsed_args.tti):
    #        PARSER.error('No metric specified, add either --bti or --tti')
    if parsed_args.timeperiod and len(parsed_args.timeperiod) > 2:
        PARSER.error('--timeperiod takes one or two arguments')
    return parsed_args


def _get_timerange(time1, time2):
    '''Validate provided times and create a timerange string to be inserted into PostgreSQL'''
    if time1 == time2:
        raise ValueError('2nd time parameter {time2} must be at least 1 hour after first parameter {time1}'.format(time1=time1, time2=time2))
        
    starttime = time(int(time1))
    
    #If the second time is 24, aka midnight, replace with maximum possible time for the range
    if time2 == 24:
        endtime = time.max
    else:
        endtime = time(int(time2))
        
    if starttime > endtime:
        raise ValueError('start time {starttime} after end time {endtime}'.format(starttime=starttime, endtime=endtime))
    
    return 'timerange(\'{starttime}\'::time, \'{endtime}\'::time)'.format(starttime=starttime.isoformat(),
                                                                                  endtime=endtime.isoformat())


def new_uri(dbset):
    '''Create a new URI based on the database settings and return it'''
    uri = QgsDataSourceURI()
    uri.setConnection(dbset['host'], "5432", dbset['database'], dbset['user'], dbset['password'])
    return uri

def _get_agg_layer(uri, agg_level = None, agg_period = None, timeperiod = None, layername = None):

    if agg_level not in SQLS:
        raise ValueError('Aggregation level: {agg_level} not implemented'.format(agg_level=agg_level))
        
    sql = SQLS[agg_level]
    sql = sql.format(timeperiod = timeperiod, agg_period = agg_period)
    uri.setDataSource("", sql, "geom", "", "gid")
    return QgsVectorLayer(uri.uri(False), layername, 'postgres')


if __name__ == '__main__':
    
    ARGS = parse_args(sys.argv[1:])
    
    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read(ARGS.dbsetting)
    dbset = CONFIG['DBSETTINGS']