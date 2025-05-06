import os
import sys
import logging
import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

file_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, file_path)
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'congestion_refresh'

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 1, 5, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule=None, # triggered by here task 
    catchup=False,
    tags=["HERE"]
)
def congestion_refresh():
    
    @task.short_circuit()
    def check_dow(ds=None):
        execution_date = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        return execution_date.weekday() == 0
        
    ## Postgres Tasks
    # Task to aggregate citywide tti daily
    aggregate_citywide_tti = SQLExecuteQueryOperator(
        sql='''select congestion.generate_citywide_tti_daily('{{ ds }}'::date - 1) ''',
        task_id='aggregate_citywide_tti',
        conn_id='natalie',
        autocommit=True,
        retries = 0
    )

    # Task to aggregate segment-level tti weekly
    aggregate_segments_tti_weekly = SQLExecuteQueryOperator(
        sql='''select congestion.generate_segments_tti_weekly('{{ ds }}'::date - 1)''',
        task_id='aggregate_segments_tti_weekly',
        conn_id='natalie',
        autocommit=True,
        retries = 0
    )
    
    aggregate_citywide_tti >> check_dow() >> aggregate_segments_tti_weekly
    # wait_for_here >> aggregate_citywide_tti >> check_dom >> aggregate_segments_bi_monthly 

congestion_refresh()