# YouTube Trending Video Pipeline using Kafka, Airflow, and Python

This project demonstrates a complete data engineering pipeline to fetch YouTube trending videos, send data to a Kafka topic, and run random or machine learning-based predictions using Apache Airflow.

---

## 📌 Project Overview

- **Step 1**: Fetch most popular YouTube videos using the YouTube Data API.
- **Step 2**: Save the data locally as JSON.
- **Step 3**: Publish each video entry to a Kafka topic (`youtube_trends`).
- **Step 4**: Run a prediction model using either:
  - Random predictions (mock prediction logic).
  - OR an ML model (`LinearRegression`) on title length.
- **Step 5**: Save results to CSV (`predicted_views.csv`).

---

## 🛠️ Tech Stack

- Apache Kafka
- Apache Zookeeper
- Apache Airflow
- Python (requests, pandas, sklearn)
- KafkaProducer (from `kafka-python`)
- Google YouTube Data API v3

---

## 🧰 Prerequisites

- Python 3.10+
- Java (for Kafka)
- Kafka & Zookeeper installed
- Apache Airflow installed inside a virtualenv

---

## ⚙️ Installation & Setup

# 1. Clone Repository

```bash
git clone https://github.com/your-username/youtube-trend-kafka-airflow.git
cd youtube-trend-kafka-airflow
```
# 2. Start Kafka and Zookeeper
```
Download Kafka from https://kafka.apache.org/downloads
```
# Start Zookeeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
# In a new terminal, start Kafka
```
bin/kafka-server-start.sh config/server.properties
```

# 3. Create Kafka Topic
```
bin/kafka-topics.sh --create --topic youtube_trends --bootstrap-server localhost:9092 --partiti
```
# 4.Create and Activate Virtual Environment
```
python3 -m venv airflow_venv
source airflow_venv/bin/activate

```
### Start scheduler and webserver
```
airflow scheduler

# In a new terminal
airflow webserver --port 8080

```
# 5. Run The Dag 
IN  airflow  UI  trigger it  and the output Saved in CSV

---

###  Used Linear Regression Model to predict views based on likes 


----

# 🤖 Future Improvements
   
Store to  SQL Server     
Power BI / Tableau Visualization            
Dockerize the setup   
