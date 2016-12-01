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

SQLS = {#'month':"",
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
                  ORDER BY metrics.bti DESC LIMIT 50)"""#,
#        'quarter':''
}

def parse_args(args, prog = None, usage = None):
    '''Parser for the command line arguments
    
    Args:
        sys.argv[1]: command line arguments
        prog: alternate program name, FOR TESTING
        usage: alternate usage message, to suppress FOR TESTING
        
    Returns:
        dictionary of parsed arguments
    '''
    PARSER = argparse.ArgumentParser(description='Produce maps of congestion metrics (tti, bti) for '
                                                 'different aggregation periods, timeperiods, and '
                                                 'aggregation levels', prog=prog, usage=usage)

    PARSER.add_argument('Metric', choices=['b', 't'], nargs='+',
                        help="Map either Buffer Time Index, Travel"
                        "Time Index or both e.g. b, t, or 'b t'."
                        "Make sure to space arguments")

    PARSER.add_argument("Aggregation", choices=['year', 'quarter', 'month'],
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

    if parsed_args.timeperiod and len(parsed_args.timeperiod) > 2:
        PARSER.error('--timeperiod takes one or two arguments')
    return parsed_args

def get_yyyymmdd(yyyy, mm, **kwargs):
    '''Combine integer yyyy and mm into a string date yyyy-mm-dd.'''
    
    if 'dd' not in kwargs:
        dd = '01'    
    elif kwargs['dd'] >= 10:
        dd = str(kwargs['dd'])
    elif kwargs['dd'] < 10:
        dd = '0'+str(kwargs['dd'])

    if mm < 10:
        return str(yyyy)+'-0'+str(mm)+'-01'
    else:
        return str(yyyy)+'-'+str(mm)+'-01'

def _validate_yyyymm_range(yyyymmrange, agg_level):
    '''Validate the two yyyymm command line arguments provided

    Args:
        yyyymmrange: List containing a start and end year-month in yyyymm format
        agg_level: the aggregation level, determines number of months each 
            period spans

    Returns:
        A dictionary with the processed range like {'yyyy':range(mm1,mm2+1)}

    Raises:
        ValueError: If the values entered are incorrect
    '''

    if agg_level not in SQLS:
        raise ValueError('Aggregation level: {agg_level} not implemented'.format(agg_level=agg_level))
    elif agg_level == 'month':
        step = 1
    elif agg_level == 'quarter':
        step = 3

    if len(yyyymmrange) != 2:
        raise ValueError('{yyyymmrange} should contain two YYYYMM arguments'
                         .format(yyyymmrange=yyyymmrange))

    regex_yyyymm = re.compile(r'20\d\d(0[1-9]|1[0-2])')
    yyyy, mm = [], []
    years = {}

    for yyyymm in yyyymmrange:
        if regex_yyyymm.fullmatch(yyyymm):
            if agg_level == 'year' and int(yyyymm[-2:]) != 1:
                raise ValueError('For annual aggregation, month must be 01 not {yyyymm}'
                                 .format(yyyymm=yyyymm))
            elif agg_level == 'quarter' and (int(yyyymm[-2:]) % 3) != 1:
                raise ValueError('For quarterly mapping, month must be in [1,4,7,10] not {yyyymm}'
                                 .format(yyyymm=yyyymm))
            yyyy.append(int(yyyymm[:4]))
            mm.append(int(yyyymm[-2:]))
        else:
            raise ValueError('{yyyymm} is not a valid year-month value of format YYYYMM'
                             .format(yyyymm=yyyymm))

    if yyyy[0] > yyyy[1] or (yyyy[0] == yyyy[1] and mm[0] > mm[1]):
        raise ValueError('Start date {yyyymm1} after end date {yyyymm2}'
                         .format(yyyymm1=yyyymmrange[0], yyyymm2=yyyymmrange[1]))
    
    if agg_level == 'year':
        #Only add January for each year to be mapped
        if yyyy[0] == yyyy[1]:
            years[yyyy[0]] = 1
        else:
            for year in range(yyyy[0], yyyy[1]+1):
                years[year] = 1
    else: 
        #Iterate over years and months with specified aggregation step 
        #(month or quarter)
        if yyyy[0] == yyyy[1]:
            years[yyyy[0]] = range(mm[0], mm[1]+1, step)
        else:
            for year in range(yyyy[0], yyyy[1]+1):
                if year == yyyy[0]:
                    years[year] = range(mm[0], 13, step)
                elif year == yyyy[1]:
                    years[year] = range(1, mm[1]+1, step)
                else:
                    years[year] = range(1, 13, step)

    return years

def _validate_multiple_yyyymm_range(years_list, agg_level):
    '''Validate a list of pairs of yearmonth strings
    
    Takes one or more lists like ['YYYYMM','YYYYMM'] and passes them to 
    _validate_yyyymm_range then merges them back into a dictionary of
    years[YYYY] = [month1, month2, etc]
    
    Args: 
        years_list: a list of lists of yyyymm strings
        agg_level: the aggregation level, determines number of months each 
            period spans
    
    Raises:
        ValueError: If the values entered are incorrect
    
    Returns:
        a dictionary of years[YYYY] = [month1, month2, etc]
    '''
    years = {}
    if len(years_list) == 1:
        years = _validate_yyyymm_range(years_list[0], agg_level)
    else:
        for yearrange in years_list:
            years_to_add = _validate_yyyymm_range(yearrange, agg_level)
            for year_to_add in years_to_add:
                if year_to_add not in years:
                    years[year_to_add] = years_to_add[year_to_add]
                else:
                    years[year_to_add] = set.union(set(years_to_add[year_to_add]),
                                                   set(years[year_to_add]))
    return years

def _get_timerange(time1, time2):
    '''Validate provided times and create a timerange string to be inserted into PostgreSQL
    
    Args:
        time1: Integer first hour
        time2: Integer second hour
        
    Returns:
        String representation creating a PostgreSQL timerange object
    '''
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


def _new_uri(dbset):
    '''Create a new URI based on the database settings and return it
    
    Args:
        dbset: dictionary of database connection settings
        
    Returns:
        PyQGIS uri object'''
    uri = QgsDataSourceURI()
    uri.setConnection(dbset['host'], "5432", dbset['database'], dbset['user'], dbset['password'])
    return uri

def _get_agg_layer(uri, agg_level = None, agg_period = None, timeperiod = None, layername = None):
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
    sql = sql.format(timeperiod = timeperiod, agg_period = agg_period)
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
                _get_agg_layer(URI, agg_level = ARGS.agg_level,
                               agg_period = yyyymmdd,
                               timeperiod = timerange,
                               layername=layername)
            
            #TODO Processing stuff