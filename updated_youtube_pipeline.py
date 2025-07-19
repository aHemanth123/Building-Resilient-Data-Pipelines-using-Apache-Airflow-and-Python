from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from sklearn.linear_model import LinearRegression
import requests
import pandas as pd
import json
import os



def fetch_push_save():
    api_key =  "Api key "  
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

    
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'input')
    os.makedirs(input_dir, exist_ok=True)

    json_path = os.path.join(input_dir, 'youtube_trends.json')
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


def run_linear_regression_prediction():
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'output')
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(input_dir, 'youtube_trends.json')

    with open(json_path, 'r') as f:
        videos = json.load(f)

     
    records = []
    for v in videos:
        try:
            video_id = v['id']
            title = v['snippet']['title']
            likes = int(v['statistics'].get('likeCount', 0))
            views = int(v['statistics'].get('viewCount', 0))
            records.append((video_id, title, likes, views))
        except Exception as e:
            print(f"Skipping video due to missing fields: {e}")

    if not records:
        print("No valid data for training.")
        return

    
    df = pd.DataFrame(records, columns=['video_id', 'title', 'likes', 'views'])
    X = df[['likes']]
    y = df['views']

    model = LinearRegression()
    model.fit(X, y)
    predicted_views = model.predict(X).astype(int)

    df['predicted_views'] = predicted_views

    # Rearranged output
    df = df[['video_id', 'title', 'likes', 'views', 'predicted_views']]
    df.columns = ['Video ID', 'Title', 'Likes', 'Actual Views', 'Predicted Views']

    output_path = os.path.join(output_dir, 'predicted_views.csv')
    df.to_csv(output_path, index=False)
    print("Linear regression prediction saved:\n", df)

 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
 
with DAG(
    dag_id='youtube_trend_kafka_linear_prediction',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    fetch_and_push = PythonOperator(
        task_id='fetch_push_to_kafka_and_save',
        python_callable=fetch_push_save
    )

    linear_prediction = PythonOperator(
        task_id='linear_regression_view_prediction',
        python_callable=run_linear_regression_prediction
    )

    fetch_and_push >> linear_prediction
