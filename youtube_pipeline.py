from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import json
import os
import pandas as pd
import random
 
def fetch_push_save():
    api_key = " APIKEY"  # Replace with your own API key
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

    # Save to JSON
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_dir, exist_ok=True)
    json_path = os.path.join(data_dir, 'youtube_trends.json')
    with open(json_path, 'w') as f:
        json.dump(videos, f, indent=2)

    # Push each video to Kafka topic
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for video in videos:
        producer.send('youtube_trends', video)
    producer.flush()
    producer.close()
 
def run_random_prediction():from airflow.www.fab_security.manager import AUTH_DB

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests
import json
import os
import pandas as pd
from sklearn.linear_model import LinearRegression

# 1. Fetch YouTube Trends → Save to JSON → Push to Kafka
def fetch_push_save():
    api_key = "AIzaSyDY1edBLUsuwgkSjy-BYndXQOsY_6-ov5k"  # Replace with your own API key
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

    # Save to JSON in airflow/data
    data_dir = os.path.join(os.path.dirname(_file_), '..', 'data')
    os.makedirs(data_dir, exist_ok=True)
    json_path = os.path.join(data_dir, 'youtube_trends.json')
    with open(json_path, 'w') as f:
        json.dump(videos, f, indent=2)

    # Push to Kafka
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for video in videos:
        producer.send('youtube_trends', video)
    producer.flush()
    producer.close()

# 2. Run ML Model Prediction: Predict views based on title length
def run_model():
    data_dir = os.path.join(os.path.dirname(_file_), '..', 'data')
    json_path = os.path.join(data_dir, 'youtube_trends.json')

    with open(json_path, 'r') as f:
        videos = json.load(f)

    # Extract title and view count
    titles = [v['snippet']['title'] for v in videos]
    views = [int(v['statistics'].get('viewCount', 0)) for v in videos]

    df = pd.DataFrame({
        'title': titles,
        'views': views,
        'title_length': [len(t) for t in titles]
    })

    # Train simple regression model
    X = df[['title_length']]
    y = df['views']

    model = LinearRegression()
    model.fit(X, y)

    df['predicted_views'] = model.predict(X).astype(int)

    # Save predictions
    output_path = os.path.join(data_dir, 'predicted_views.csv')
    df.to_csv(output_path, index=False)

    print(df[['title', 'views', 'predicted_views']])

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='youtube_trend_kafka_ml',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    fetch_and_push = PythonOperator(
        task_id='fetch_push_to_kafka_and_save',
        python_callable=fetch_push_save
    )

    ml_prediction = PythonOperator(
        task_id='ml_model_prediction',
        python_callable=run_model
    )

    fetch_and_push >> ml_prediction
  
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    json_path = os.path.join(data_dir, 'youtube_trends.json')

    with open(json_path, 'r') as f:
        videos = json.load(f)

    titles = [v['snippet']['title'] for v in videos]
    views = [int(v['statistics'].get('viewCount', 0)) for v in videos]

    predicted_views = []

    for i in range(len(views)):
        # Select random view count from OTHER videos (not itself)
        other_views = views[:i] + views[i+1:] if len(views) > 1 else views
        predicted_views.append(random.choice(other_views))

    df = pd.DataFrame({
        'title': titles,
        'actual_views': views,
        'predicted_views': predicted_views
    })

    output_path = os.path.join(data_dir, 'predicted_views.csv')
    df.to_csv(output_path, index=False)

    print(df[['title', 'actual_views', 'predicted_views']])

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='youtube_trend_kafka_random_selection',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    fetch_and_push = PythonOperator(
        task_id='fetch_push_to_kafka_and_save',
        python_callable=fetch_push_save
    )

    random_prediction = PythonOperator(
        task_id='random_view_prediction',
        python_callable=run_random_prediction
    )

    fetch_and_push >> random_prediction
