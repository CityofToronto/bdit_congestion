"""
airflow_congestion_aggregation

A data aggregation workflow that aggregates travel time data for
three Congestion Network intermediate tables, network_daily_spd. 
This DAG is schedule to run only when pull_here DAG
finished running. 
"""
import os
import sys
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
    from bdit_dag_utils.utils.common_tasks import check_jan_1st, check_1st_of_month
except:
    raise ImportError("Cannot import slack alert functions")

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
    tags=["HERE", "aggregation"]
)
def congestion_aggregation():
    @task_group(tooltip="Tasks to check if necessary to create new partitions and if so, exexcute.")
    def check_partitions():

        create_annual_partition = SQLExecuteQueryOperator(
            task_id='create_annual_partitions',
            pre_execute=check_jan_1st,
            sql=["SELECT congestion.create_yyyy_partition('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)"],
            conn_id='congestion_bot',
            autocommit=True
        )
      
        create_month_partition = SQLExecuteQueryOperator(
            task_id='create_month_partition',
            pre_execute=check_1st_of_month,
            sql="""SELECT congestion.create_yyyymm_partitions('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, '{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}'::int)""",
            conn_id='congestion_bot',
            autocommit=True,
            trigger_rule='none_failed'
        )

        create_annual_partition >> create_month_partition
    ## Postgres Tasks
    # Task to aggregate segment level tt daily
    aggregate_daily = SQLExecuteQueryOperator(
        sql='''SELECT congestion.generate_network_daily_spd('{{ ds }}'::date - 1) ''',
        task_id='aggregate_daily',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 0)
    
    # Data checks TBD 

    check_partitions() >> aggregate_daily

congestion_aggregation()