from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import json
import os
import pandas as pd
 
def fetch_push_save():
    api_key = "YOUR_API_KEY"  # Replace with your actual API key
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        'part': 'snippet,statistics',
        'chart': 'mostPopular',
        'maxResults': 5,
        'regionCode': 'US',
        'key': api_key
    }

    response = requests.get(url, params=params)
    videos = response.json().get("items", [])
 
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_dir, exist_ok=True)
    json_path = os.path.join(data_dir, 'youtube_trends.json')
    with open(json_path, 'w') as f:
        json.dump(videos, f, indent=2)
 
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for video in videos:
        producer.send('youtube_trends', video)
    producer.flush()
    producer.close()
 
def run_random_boost_prediction():
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    json_path = os.path.join(data_dir, 'youtube_trends.json')

    with open(json_path, 'r') as f:
        videos = json.load(f)

    titles = [v['snippet']['title'] for v in videos]
    actual_views = [int(v['statistics'].get('viewCount', 0)) for v in videos]
    predicted_views = [int(v * 1.2) for v in actual_views]  # 20% boost

    df = pd.DataFrame({
        'title': titles,
        'actual_views': actual_views,
        'predicted_views': predicted_views
    })

    output_path = os.path.join(data_dir, 'predicted_views.csv')
    df.to_csv(output_path, index=False)

    print(df)
 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='youtube_trend_kafka_random_boost',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    fetch_and_push = PythonOperator(
        task_id='fetch_push_to_kafka_and_save',
        python_callable=fetch_push_save
    )

    boosted_prediction = PythonOperator(
        task_id='boosted_view_prediction',
        python_callable=run_random_boost_prediction
    )

    fetch_and_push >> boosted_prediction
