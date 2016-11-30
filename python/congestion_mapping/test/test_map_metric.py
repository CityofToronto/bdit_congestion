import unittest
from argparse import ArgumentError
from map_metric import _get_timerange, parse_args
''' Testing file
    Run with `python -m unittest in the root folder'''

class TimeRangeTestCase(unittest.TestCase):
    '''Tests for timerange creation'''
    
    def test_outoforder_times(self):
        '''Test if the proper error is thrown with time1=9 and time2=8'''
        with self.assertRaises(ValueError) as cm:
            _get_timerange(9,8)
        self.assertEqual('start time 09:00:00 after end time 08:00:00', str(cm.exception))
        
    def test_valid_range(self):
        '''Test if the right string is produced from time1=8 and time2=9'''
        valid_result = 'timerange(\'08:00:00\'::time, \'09:00:00\'::time)'
        self.assertEqual(valid_result, _get_timerange(8,9))
        
    def test_equal_numbers(self):
        '''Test if the proper error is thrown if both parameters are equal'''
        with self.assertRaises(ValueError) as cm:
            _get_timerange(8,8)
        self.assertEqual('2nd time parameter 8 must be at least 1 hour after first parameter 8', str(cm.exception))
        
class ArgParseTestCase(unittest.TestCase):
    '''Tests for argument parsing'''

    def test_years_y_single(self):
        '''Test if a single pair of years produces the right values'''
        valid_result = [['201407','201506']]
        args = parse_args('b year -p 8 -r 201407 201506'.split())
        self.assertEqual(valid_result, args.range)

    def test_metric_both(self):
        '''Test if a single pair of years produces the right values'''
        valid_result = ['b','t']
        args = parse_args('b t year -p 8 -r 201407 201506'.split())
        self.assertEqual(valid_result, args.Metric)

    def test_aggregation_level(self):
        '''Test if aggregation level years produces the right value'''
        valid_result = 'year'
        args = parse_args('b t year -p 8 -r 201407 201506'.split())
        self.assertEqual(valid_result, args.Aggregation)

    def test_years_y_multiple(self):
        '''Test if a multiple pair of years produces the right values'''
        valid_result = [['201203', '201301'],['201207', '201209']]
        args = parse_args('b year -p 8 -r 201203 201301 -r 201207 201209'.split())
        self.assertEqual(valid_result, args.range)

    def test_years_y_only_one(self):
        '''Test if a single year produces the right exception'''
        with self.assertRaises(SystemExit) as cm:
            args = parse_args('b year -p 8 -r 201207'.split())
        print(cm.exception)
        self.assertEqual('argument -r/--range: expected 2 arguments', str(cm.exception))

    def test_period_one(self):
        '''Test if a the right value for period is parsed'''
        valid_result = [8]
        args = parse_args('b t year -p 8 -r 201407 201506'.split())
        self.assertEqual(valid_result, args.timeperiod)

    def test_iterate_hours(self):
        '''Test if the right value for iteration hours is parsed'''
        valid_result = [8,9]
        args = parse_args('b t year -i 8 9 -r 201407 201506'.split())
        self.assertEqual(valid_result, args.hours_iterate)

    def test_default_tablename(self):
        '''Test if the right default for tablename is returned'''
        valid_result = 'congestion.metrics'
        args = parse_args('b t year -i 8 9 -r 201407 201506'.split())
        self.assertEqual(valid_result, args.tablename)

if __name__ == '__main__':
    unittest.main()