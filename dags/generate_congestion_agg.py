"""
airflow_congestion_aggregation

A data aggregation workflow that aggregates travel time data for
three Congestion Network intermediate tables, network_daily, network_monthly,
and centreline_monthly. This DAG is schedule to run only when pull_here DAG
finished running. 
"""
import os
import sys
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

file_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, file_path)
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'congestion_aggregation'

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': datetime(2022, 8, 17),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    dag_id=DAG_NAME,
    default_args=default_args, 
    schedule=None, # triggered by here dag 
    catchup=False,
    tags=["HERE"]
)
def congestion_aggregation():
    
    ## Tasks ##
    
    ## ShortCircuitOperator Tasks, python_callable returns True or False; False means skip downstream tasks
    @task.short_circuit()
    def check_dom(ds=None):
        execution_date = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        return execution_date.day == 1

    ## SQLCheckOperator to check if all of last weeks data is in the data before aggregating
    check_monthly = SQLCheckOperator(
        task_id = 'check_monthly',
        conn_id = 'congestion_bot',
        sql = '''SELECT case when count(distinct dt) = 
                    extract('days' FROM (('{{ ds }}'::date - 1) - ('{{ ds }}'::date - 1 - interval '1 month'))
                    THEN TRUE ELSE FALSE END AS counts
            FROM here.ta
            WHERE
                dt >= '{{ ds }}'::date - 1 - interval '1 month'
                AND dt < '{{ ds }}'::date - 1'''
    )

    ## Postgres Tasks
    # Task to aggregate segment level tt daily
    aggregate_daily = SQLExecuteQueryOperator(
        sql='''SELECT congestion.generate_network_daily('{{ ds }}'::date - 1) ''',
        task_id='aggregate_daily',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 0)


    # Task to aggregate segment level tt monthly
    aggregate_seg_monthly = SQLExecuteQueryOperator(
        sql='''select congestion.generate_network_monthly('{{ ds }}'::date - 1 - interval '1 month');''',
        task_id='aggregate_seg_monthly',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 0
    )

    # Task to aggregate centreline level tt monthly 
    aggregate_cent_monthly = SQLExecuteQueryOperator(
        sql='''select congestion.generate_centreline_monthly('{{ ds }}'::date - 1 - interval '1 month');''',
        task_id='aggregate_cent_monthly',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 0,
    )

    aggregate_daily >> check_dom() >> check_monthly >> [aggregate_seg_monthly, aggregate_cent_monthly]

congestion_aggregation()