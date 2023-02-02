"""
airflow_congestion_aggregation

A data aggregation workflow that aggregates travel time data for
three Congestion Network intermediate tables, network_daily, network_monthly,
and centreline_monthly. This DAG is schedule to run only when pull_here DAG
finished running. 
"""
from airflow import DAG
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.sql import SQLCheckOperator

from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Slack alert
SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = ':cat_yell: <@UF4RQFQ11> !!! {task_id} in congestion_aggregation DAG failed.'.format(task_id=context.get('task_instance').task_id)   
    slack_msg = task_msg + """(<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': datetime(2022, 8, 17),
                'email': ['natalie.chan@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('congestion_aggregation', 
          default_args=default_args, 
          schedule_interval='30 16 * * * ', # same as pull_here task 
          catchup=False,
)

## Functions
# check if its the start of the month
def is_day_one(date_to_pull):
    execution_date = datetime.strptime(date_to_pull, "%Y-%m-%d")	
    if execution_date.day == 1:		
        return True
    else: 
        return False


## Tasks ##
## ExternalTaskSensor to wait for pull_here
wait_for_here = ExternalTaskSensor(task_id='wait_for_here',
                                   external_dag_id='pull_here',
                                   external_task_id='pull_here',
                                   start_date=datetime(2020, 1, 5)
                                   )

## ShortCircuitOperator Tasks, python_callable returns True or False; False means skip downstream tasks
check_dom = ShortCircuitOperator(
    task_id='check_dom',
    provide_context=False,
    python_callable=is_day_one,
    op_kwargs={'date_to_pull': '{{ yesterday_ds }}'},
    dag=dag
    )

## SQLCheckOperator to check if all of last weeks data is in the data before aggregating
check_monthly = SQLCheckOperator(task_id = 'check_monthly',
                                 conn_id = 'congestion_bot',
                                 sql = '''SELECT case when count(distinct dt) = 
                                                    extract('days' FROM ('{{ macros.datetime.date(execution_date + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) }}'::date + interval '1 month' - '{{ macros.datetime.date(execution_date + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) }}'::date)) 
                                                    THEN TRUE ELSE FALSE END AS counts
                                          FROM here.ta
                                          WHERE dt >= '{{ macros.datetime.date(execution_date + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) }}'::date and dt < '{{ macros.datetime.date(execution_date + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) }}'::date + interval '1 month' ''',
                                 dag=dag)

## Postgres Tasks
# Task to aggregate segment level tt daily
aggregate_daily = PostgresOperator(sql='''SELECT congestion.generate_network_daily('{{ yesterday_ds }}') ''',
                                   task_id='aggregate_daily',
                                   postgres_conn_id='congestion_bot',
                                   autocommit=True,
                                   retries = 0,
                                   dag=dag)


# Task to aggregate segment level tt monthly
aggregate_seg_monthly = PostgresOperator(sql='''select congestion.generate_network_monthly('{{ macros.datetime.date(execution_date + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) }}');''',
                                     task_id='aggregate_seg_monthly',
                                     postgres_conn_id='congestion_bot',
                                     autocommit=True,
                                     retries = 0,
                                     dag=dag)

# Task to aggregate centreline level tt monthly 
aggregate_cent_monthly = PostgresOperator(sql='''select congestion.generate_centreline_monthly('{{ macros.datetime.date(execution_date + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) }}');''',
                                     task_id='aggregate_cent_monthly',
                                     postgres_conn_id='congestion_bot',
                                     autocommit=True,
                                     retries = 0,
                                     dag=dag)

wait_for_here >> aggregate_daily >> check_dom >> check_monthly >> [aggregate_seg_monthly, aggregate_cent_monthly]
