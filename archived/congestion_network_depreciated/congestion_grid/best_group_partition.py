import configparser
import pandas as pd
import pandas.io.sql as pandasql
from psycopg2 import connect
import numpy
from psycopg2.extras import execute_values
from datetime import datetime
from math import factorial
import numpy as np
from itertools import islice 
from itertools import combinations, chain
CONFIG = configparser.ConfigParser()
CONFIG.read(r'/home/natalie/airflow/db.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)


small_groups = pd.read_sql('''with sets as (
                                select * from congestion.merged_segments_v2 inner join 
                                (select distinct segment_id from congestion.partition_result)long using (segment_id))

                                , int as (select  *, unnest(length_set) as lengths, array_length(length_set, 1) from sets
                                where array_length(length_set, 1) <=30 and array_length(length_set, 1) >2   order by array_length(length_set, 1))
                                select segment_id, start_vid, end_vid, link_set,array_agg(lengths::int order by row_number) as length_set, length
                                from (select row_number() over (), * from int)a	
                                group by segment_id, start_vid, end_vid, link_set, length	

                                                                ''',con)


def get_num_groups(length_set, length):
    num_link = len(length_set)
    long_link = 0
    for i in length_set:
        if i >200:
            long_link = long_link + 1
    groups = 1+ int(length/200) + long_link
    start = int(length/200)
    if groups > num_link:
        groups = num_link
    else: 
        groups = groups
    if start > groups:
        start = num_link-2
    else:
        start = start
        
    if start == 1 or start == 0:
        range = [1, groups]
    else:
        range = [start-1 , groups]
    return num_link, range

def calculate_num_combination(num_link, range1):
    total_com = 0
    for i in range(range1[0], range1[1]):
        x = num_link - 1
        y = i - 1
        com = factorial(x)/(factorial(y)*factorial(x-y))
        total_com = total_com + com
    return total_com    

def split_list(data, n):
    for splits in combinations(range(1, len(data)), n-1):
        result = []
        prev = None
        for split in chain(splits, [None]):
            result.append(data[prev:split])
            prev = split
        yield result
        
def return_results(length_set, groups):
    possibility = []
    for q in range(groups[0], groups[1]):
        split_result = split_list(length_set, q)
        for a in split_result:  
            possibility.append(a)           
    group_result = []
    group_set = []
    for groups in possibility:
        result = []
        sets = []
        for set in groups:
            sum_length = sum(set)
            result.append(sum_length)
            sets.append(set)
        group_result.append(result) 
        group_set.append(sets)
    return group_result, group_set        

def evaluate_results(return_groups):
    set_result = []
    for set in return_groups:
        sum_diff = 0
        for i in range(len(set)):
            diff = abs(200-set[i])
            sum_diff = sum_diff + diff
        set_result.append(sum_diff)  
    selection = set_result.index(min(set_result))
    return selection

def find_link(group_set, selection):
    length_to_split = []
    for i in group_set[selection]:
         length_to_split.append(len(i))

    Input = iter(link_dir) 
    split_link = [list(islice(Input, elem)) 
              for elem in length_to_split] 
    return split_link, group_set[selection]                                

rows = []

start_time = datetime.now()
for index, row in small_groups.iterrows():
    now = datetime.now()
    row = []
    groups = None
    length_set = small_groups['length_set'].iloc[index]
    link_dir = small_groups['link_set'].iloc[index] 
    segment_id = small_groups['segment_id'].iloc[index].astype(float)
    length = small_groups['length'].iloc[index].astype(float)
    print('Segment ID: ' + str(segment_id))
    # Get number of dividing groups based on length and number of links
    num_link, groups = get_num_groups(length_set, length)
    num_group = calculate_num_combination(num_link, groups)

    if num_group <= 15000000:
        print('Number of combination:' + str(num_group))
        # Return divided group set
        return_groups, group_set = return_results(length_set, groups)
        # Make selection based on summed length
        selection = evaluate_results(return_groups)

        done = datetime.now()
        print(done-now)

        # Retrieve selected partitioned array
        result_link, result_length = find_link(group_set, selection)
        return_groups = []
        group_set = []
        # Prepare rows for sql inserting
        for i in range(len(result_link)):
            new_link_set = result_link[i]
            new_length = sum(result_length[i]) 
            new_length_set = result_length[i]
            row = (segment_id, i, new_link_set, new_length_set, new_length)
            rows.append(row)             

end_time = datetime.now()
print(end_time-start_time)              
sql = '''insert into congestion.partition_all_possibility_v2(segment_id, id, link_set, length_set, length) VALUES %s'''    
with con:
    with con.cursor() as cur:
        execute_values(cur, sql, rows)     