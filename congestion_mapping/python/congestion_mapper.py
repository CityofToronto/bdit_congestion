#! python3
'''Object definition for congestion mapping
'''

from iteration_mapper import iteration_mapper

class CongestionMapper( IteratingMapper ):
    '''Holds settings for iterating over multiple congestion maps
    '''
    SQLS = {'year':"""(
    SELECT row_number() OVER (PARTITION BY metrics.agg_period ORDER BY metrics.{metric} DESC) AS "Rank",
        tmc_from_to_lookup.street_name AS "Street",
        gis.twochar_direction(inrix_tmc_tor.direction) AS "Dir",
        tmc_from_to_lookup.from_to AS "From - To",
        to_char(metrics.{metric}, '0D99'::text) AS "{metric_name}",
        inrix_tmc_tor.geom
    FROM congestion.metrics
    JOIN congestion.aggregation_levels USING (agg_id)
    JOIN gis.inrix_tmc_tor USING (tmc)
    JOIN gis.tmc_from_to_lookup USING (tmc)
    WHERE inrix_tmc_tor.sum_miles > 0.124274 AND aggregation_levels.agg_level = 'year'
    AND metrics.timeperiod = {timeperiod} AND metrics.agg_period = {agg_period}::DATE
    ORDER BY metrics.{metric} DESC LIMIT 50)"""#,
        #'quarter':''''''
        #'month':''''''
    }

    METRICS = {'b':{'sql_acronym':'bti',
                    'metric_name':'Buffer Time Index',
                    'metric_attr':'Least Reliable',
                    'stat_description':'Buffer Time Index = (95th percentile Time - Median Time)/(Median Time)'
                   },
               't':{'sql_acronym':'tti',
                    'metric_name': 'Travel Time Index',
                    'metric_attr':'Most Congested',
                    'stat_description':'Travel Time Index = Average Travel Time / Free Flow Travel Time'
                   }
              }

    COMPOSER_LABELS = {'map_title': '{agg_period} Top 50 {metric_attr} Road Segments',
                       'time_period': '{period_name} ({from_to_hours})',
                       'stat_description': '{stat_description}'}
    IteratingMapper.COMPOSER_LABELS = COMPOSER_LABELS
    BACKGROUND_LAYERNAMES = [u'CENTRELINE_WGS84', u'to']
    IteratingMapper.BACKGROUND_LAYERNAMES = BACKGROUND_LAYERNAMES
    
    
    def __init__(self, logger, dbsettings, stylepath, templatepath, agg_level, projectfile = None, console = False):
        super(IteratingMapper, self).__init__(logger, dbsettings, stylepath, templatepath, projectfile = None, console = False)
        self.agg_level = agg_level
        self.metric = None
    
    def load_agg_layer(self, yyyymmdd=None, timeperiod=None,
                   layername=None):
        '''Create a QgsVectorLayer from a connection and specified parameters

        Args:
            uri: PyQGIS uri object
            agg_level: string representing aggregation level, key to SQLS dict
            yyyymmdd: the starting aggregation date for the period as a string
                digestible by PostgreSQL into a DATE
            timeperiod: string representing a PostgreSQL timerange
            layername: string name to give the layer

        Returns:
            QgsVectorLayer from the specified sql query with provided layername'''
        
        sql = SQLS[self.agg_level]
        sql = sql.format(timeperiod=timeperiod, agg_period=yyyymmdd, metric=self.metric['sql_acronym'], metric_name=self.metric['metric_name'])
        self.uri.setDataSource("", sql, "geom", "", "Rank")
        self.layer = QgsVectorLayer(uri.uri(False), layername, 'postgres')
        self.map_registry.addMapLayer(layer)
        self.layer.loadNamedStyle(self.stylepath)
    
    def set_metric(self, metric_id):
        if metric_id not in METRICS:
            raise ValueError('{metric_id} is unsupported'.format(metric_id=metric_id))
        self.metric = METRICS[metric_id]

        