FROM apache/airflow:2.10.1

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN pip uninstall dataclasses -y
