#import stuff
from datetime import time

SQLS = {'month':"",
        'year':"""SELECT row_number() OVER (PARTITION BY metrics.agg_period ORDER BY metrics.bti DESC) AS "Rank",
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
                  ORDER BY metrics.bti DESC LIMIT 50""",
        'quarter':''}

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

    sql = SQLS[agg_level]
    sql = sql.format(timeperiod = timeperiod, agg_period = agg_period)
    uri.setDataSource("", sql, "geom", "", "gid")
    return QgsVectorLayer(uri.uri(False), layername, 'postgres')


if __name__ == '__main__':
    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read(ARGS.dbsetting)
    dbset = CONFIG['DBSETTINGS']