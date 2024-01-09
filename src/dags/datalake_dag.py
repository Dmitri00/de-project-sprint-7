import pendulum
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator
from airflow.operators.dummy import DummyOperator


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 


args = {
    "owner": "dtoichkin",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

kwargs = {
    'conf': {"spark.driver.maxResultSize": "20g"},
    'num_executors': 2,
    'executor_memory':"4g",
    'executor_cores':2
}

exec_date = '{{ ds }}'
data_raw_events_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/geo/events'
data_matched_events_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/dtoichkin/data/geo/events'
user_zone_report_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/dtoichkin/data/datamart/user_zone_report'
week_zone_report_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/dtoichkin/data/datamart/week_zone_report'
recommendation_zone_report_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/dtoichkin/data/datamart/recomendation_zone_report'

user_stat_depth = 365
user_home_stand_days = 27

with DAG(
        'datalake_dag',
        default_args=args,
        description='',
        catchup=False,
        schedule_interval='0 2 * * * ',
        start_date=pendulum.datetime(2022, 1, 2, tz="UTC"),
        tags=['pyspark', 'hadoop', 'hdfs', 'datalake', 'geo', 'datamart'],
        is_paused_upon_creation=True,
) as dag:
    start = DummyOperator(task_id='start')
    
    

    message_city_match = SparkSubmitOperator(
        task_id="message_city_match",
        dag=dag,
        application="/lessons/message_city_match.py",
        conn_id="yarn_spark",
        application_args=[
            exec_date,
            data_raw_events_path,
            "/user/dtoichkin/data/geo_tz.csv",
            data_matched_events_path,
        ],
        **kwargs
    )

    build_user_geo_stats = SparkSubmitOperator(
        task_id="user_geo_stats",
        dag=dag,
        application="/lessons/user_geo_stats.py",
        conn_id="yarn_spark",
        application_args=[
            exec_date,
            user_stat_depth,
            user_home_stand_days,
            data_matched_events_path,
            user_zone_report_path
        ],
        **kwargs
    )


    
    finish = DummyOperator(task_id='finish')
    
    (
        start 
        >> message_city_match 
        #>> [build_user_geo_stats, built_week_zone_report_task, built_recommendation_zone_report_task] 
        >> finish
    )