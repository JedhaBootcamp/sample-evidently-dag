FROM apache/airflow:3.1.3

COPY requirements.txt .

RUN pip install -r requirements.txt
