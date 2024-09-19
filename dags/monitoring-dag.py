import glob
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator)
from airflow.sensors.python import PythonSensor
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from airflow.models import Variable

from evidently.ui.workspace.cloud import CloudWorkspace

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

NUMERICAL_FEATURES = ["temp", "atemp", "humidity", "windspeed"]
CATEGORICAL_FEATURES = ["season", "holiday", "workingday"]
EVIDENTLY_CLOUD_TOKEN = Variable.get("EVIDENTLY_CLOUD_TOKEN") # MAKE SURE YOU ADD THAT IN THE AIRFLOW UI (ADMIN > VARIABLES)
EVIDENTLY_CLOUD_PROJECT_ID = Variable.get("EVIDENTLY_CLOUD_PROJECT_ID") # MAKE SURE YOU ADD THAT IN THE AIRFLOW UI (ADMIN > VARIABLES) AND GET YOUR TOKEN DIRECTLY FROM EVIDENTLY CLOUD

def _load_files(data_logs_filename):
    """Returns reference sample DataFrame, the data logs DataFrame and a
    ColumnMapping object, with numerical and categorial features set.
    """
    reference = pd.read_csv("/opt/airflow/data/reference/weather-reference-sample.csv")
    reference = reference.drop(labels="count", axis=1)
    reference["datetime"]=pd.to_datetime(reference["datetime"])

    data_logs = pd.read_csv(data_logs_filename)
    data_logs["datetime"]=pd.to_datetime(data_logs["datetime"])

    return reference, data_logs


def _detect_file(**context):
    """Detects if a file named `week*.csv` is inside `./data/data-drift` folder.

    If yes, it saves the full path to XCom and return True. False otherwise.
    """
    data_logs_list = glob.glob("/opt/airflow/data/data-drift/week*.csv")

    if not data_logs_list:
        return False
    data_logs_filename = max(data_logs_list, key=os.path.getctime)
    context["task_instance"].xcom_push(key="data_logs_filename", value=data_logs_filename)
    return True


def _detect_data_drift(**context):
    """Load the CSV and run the data-drift detections.
    """
    data_logs_filename = context["task_instance"].xcom_pull(key="data_logs_filename")
    reference, data_logs = _load_files(data_logs_filename)

    data_drift_report = Report(metrics=[
        DataDriftPreset(),
    ])

    data_drift_report.run(current_data=data_logs, reference_data=reference, column_mapping=None)

    report = data_drift_report.as_dict()

    if report["metrics"][0]["result"]["dataset_drift"]:
        return "data_drift_detected"
    else:
        return "no_data_drift_detected"


def _data_drift_detected(**context):
    """Produces a report on evidently cloud
    """

    ws = CloudWorkspace(
    token=EVIDENTLY_CLOUD_TOKEN,
    url="https://app.evidently.cloud")

    project = ws.get_project(EVIDENTLY_CLOUD_PROJECT_ID)

    data_logs_filename = context["task_instance"].xcom_pull(key="data_logs_filename")
    reference, data_logs = _load_files(data_logs_filename)

    data_drift_report = Report(metrics=[
        DataDriftPreset(),
    ])

    data_drift_report.run(current_data=data_logs, reference_data=reference, column_mapping=None)
    ws.add_report(project.id, data_drift_report, include_data=True)

def _clean_file(**context):
    data_logs_filename = context["task_instance"].xcom_pull(key="data_logs_filename")
    os.remove(data_logs_filename)


with DAG(dag_id="monitoring_dag", default_args=default_args, schedule_interval="0 16 * * *", catchup=False) as dag:
    
    detect_file = PythonSensor(
        task_id="detect_file",
        python_callable=_detect_file,
    )

    detect_data_drift = BranchPythonOperator(task_id="detect_data_drift", python_callable=_detect_data_drift)
    data_drift_detected = PythonOperator(task_id="data_drift_detected", python_callable=_data_drift_detected)
    no_data_drift_detected = DummyOperator(task_id="no_data_drift_detected")
    clean_file = PythonOperator(task_id="clean_file", python_callable=_clean_file, trigger_rule="one_success")
    end = DummyOperator(task_id="end")

    detect_file >> detect_data_drift >> [data_drift_detected, no_data_drift_detected] >> clean_file >> end
