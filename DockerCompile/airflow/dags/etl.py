import logging
import time
from pymongo import MongoClient
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


client=MongoClient(host='my_mongodb', port=27017)
db=client.twitter
mongoTweets=db.tweets

engine=create_engine('postgres://postgres:xxxx@my_postgresdb:5432/postgres')

create_query='''
CREATE TABLE IF NOT EXISTS tweetPostgres(
user_time TEXT,
tweepy_time TIMESTAMP,
user_real TEXT,
user_name TEXT,
location TEXT,
hashtags TEXT,
tweet_text TEXT,
sentiment_score REAL);'''

engine.execute(create_query)
logging.critical('ETL Connecting to Postgresdb and creating table!')

def extraction():
    time1=datetime.utcnow()-timedelta(minutes=5)
    time2=datetime.utcnow()
    mongo_dict=list(mongoTweets.find({"tweepy time":{'$gte':time1, '$lte':time2}}))
    logging.critical('ETL retrieved from MongoDB')
    return mongo_dict

def transformation(**context):
    mongo_dict=context['task_instance'].xcom_pull(task_ids="extract")
    s=SentimentIntensityAnalyzer()
    analyzed_tweets=[]
    for tweet in mongo_dict:
        sentiment=s.polarity_scores(tweet['text'])
        tweet['sentiment_score']=sentiment['compound']
        analyzed_tweets.append(tweet)
        logging.critical('ETL calculated sentiment score')
    return analyzed_tweets

def load2postgres(**context):
    analyzed_tweets=context['task_instance'].xcom_pull(task_ids="transform")
    for tweet in analyzed_tweets:
        insert_query=f"""
        INSERT INTO tweetPostgres VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"""
        tweetData=[tweet['User time'], tweet['tweepy time'], tweet['user'],
        tweet['username'], tweet['location'], tweet['hashtags'], tweet['text'],
        tweet['sentiment_score']]
        engine.execute(insert_query, tweetData)
        logging.critical('ETL loaded data to Postgres')

default_args={'owner': 'Gloria',
'start_date': airflow.utils.dates.days_ago(1),
'depends_on_past': False,
'email': 'example@example.com',
'email_on_failure': False,
'email_on_retry': False,
'retries': 1,
'retry_delay': timedelta(minutes=1)
}

dag=DAG('ETL',
description='Get Tweets',
catchup=False,
schedule_interval=timedelta(minutes=5),
default_args= default_args,
is_paused_upon_creation=False
)

t1=PythonOperator(task_id="extract",
python_callable=extraction,
dag=dag
)
t2=PythonOperator(task_id="transform",
provide_context=True,
python_callable=transformation,
dag=dag
)
t3=PythonOperator(task_id="load",
provide_context=True,
python_callable=load2postgres,
dag=dag
)

t1>>t2>>t3
