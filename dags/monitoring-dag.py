"""
Data Drift Monitoring DAG for Weather Data

This DAG monitors weather data for data drift using Evidently AI.
It runs daily at 4 PM and performs the following tasks:
1. Detects new data files in the data-drift directory
2. Analyzes the data for drift compared to a reference dataset
3. If drift is detected, sends a report to Evidently Cloud
4. Optionally cleans up processed files

DAG Schedule: Daily at 4 PM (16:00)
"""

# Standard library imports
import glob
import os
from datetime import datetime, timedelta

# Third-party imports
import pandas as pd
from evidently import Dataset
from evidently import DataDefinition
from evidently import Report
from evidently.presets import DataDriftPreset

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.empty import EmptyOperator  # Replaces DummyOperator in Airflow 3.1+
from airflow.providers.standard.operators.python import (  # Updated from python_operator
    BranchPythonOperator,
    PythonOperator
)

# Evidently imports for Cloud integration
from evidently.ui.workspace import CloudWorkspace

# =============================================================================
# DAG Configuration
# =============================================================================

# Default arguments for all tasks in the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
    "retries": 0,  # Number of times to retry failed tasks
    "retry_delay": timedelta(minutes=1),  # Delay between retries
}

# =============================================================================
# Feature Definitions
# =============================================================================

# Define which columns in our dataset are numerical features
NUMERICAL_FEATURES = ["temp", "atemp", "humidity", "windspeed"]

# Define which columns in our dataset are categorical features
CATEGORICAL_FEATURES = ["season", "holiday", "workingday"]

# =============================================================================
# Evidently Cloud Configuration
# =============================================================================

# NOTE FOR STUDENTS: These variables MUST be set in the Airflow UI before running this DAG
# Go to: Airflow UI > Admin > Variables
# Add these two variables:
# 1. EVIDENTLY_CLOUD_TOKEN - Your authentication token from Evidently Cloud
# 2. EVIDENTLY_CLOUD_PROJECT_ID - Your project ID from Evidently Cloud
#
# Using Variable.get() with default values prevents DAG parsing errors
# If the variables don't exist, the DAG will still load (but tasks will fail at runtime)
EVIDENTLY_CLOUD_TOKEN = Variable.get("EVIDENTLY_CLOUD_TOKEN", default_var=None)
EVIDENTLY_CLOUD_PROJECT_ID = Variable.get("EVIDENTLY_CLOUD_PROJECT_ID", default_var=None)

# =============================================================================
# Helper Functions (Callables for Tasks)
# =============================================================================

def _load_files(data_logs_filename):
    """
    Load and prepare reference and current data for drift detection.
    
    This function loads two datasets:
    1. Reference dataset: Historical baseline data used for comparison
    2. Current dataset: New data to check for drift
    
    Args:
        data_logs_filename (str): Path to the current data file to analyze
        
    Returns:
        tuple: (reference_dataframe, current_dataframe) ready for Evidently analysis
        
    Note for Students:
        - The reference data should represent "normal" or expected data distribution
        - The current data is compared against this reference to detect drift
        - We drop the 'count' column from reference as it's not needed for drift detection
        - Datetime columns must be converted to datetime type for proper analysis
    """
    # Load the reference dataset (baseline for comparison)
    reference = pd.read_csv("/opt/airflow/data/reference/weather-reference-sample.csv")
    
    # Remove the 'count' column as it's not needed for drift detection
    reference = reference.drop(labels="count", axis=1)
    
    # Convert datetime column to proper datetime type (important for time-series analysis)
    reference["datetime"] = pd.to_datetime(reference["datetime"])

    # Load into Evidently Dataset
    reference_dataset = Dataset.from_pandas(reference, data_definition=DataDefinition())

    # Load the current data logs that we want to check for drift
    data_logs = pd.read_csv(data_logs_filename)
    data_logs["datetime"]=pd.to_datetime(data_logs["datetime"])

    data_logs_dataset = Dataset.from_pandas(data_logs, data_definition=DataDefinition())

    # Return both dataframes for Evidently to analyze
    # Note: In newer versions of Evidently, we pass pandas DataFrames directly
    # instead of wrapping them in Dataset objects
    return reference_dataset, data_logs_dataset


def _detect_file(**context):
    """
    Sensor function to detect new data files for processing.
    
    This function is used by a PythonSensor to continuously check for new data files.
    It looks for CSV files matching the pattern 'week*.csv' in the data-drift directory.
    
    Args:
        **context: Airflow context dictionary (provides access to task instance, execution date, etc.)
        
    Returns:
        bool: True if a file is found (sensor succeeds), False otherwise (sensor keeps waiting)
        
    Note for Students:
        - This is a SENSOR function, which means it will keep running until it returns True
        - The PythonSensor will call this function repeatedly (with poke_interval) until True is returned
        - We use XCom (cross-communication) to pass the filename to downstream tasks
        - max(..., key=os.path.getctime) selects the most recently created file
    """
    # Search for all CSV files matching the pattern 'week*.csv' in the data-drift directory
    # Example matches: week1.csv, week2.csv, week10.csv, etc.
    data_logs_list = glob.glob("/opt/airflow/data/data-drift/week*.csv")

    # If no files found, return False (sensor will keep waiting)
    if not data_logs_list:
        print("No files found matching pattern 'week*.csv' in /opt/airflow/data/data-drift/")
        return False
    
    # Select the most recently created file from the list
    # This ensures we process the latest data first
    data_logs_filename = max(data_logs_list, key=os.path.getctime)
    
    print(f"✅ Found data file: {data_logs_filename}")
    
    # Push the filename to XCom so downstream tasks can access it
    # XCom allows tasks to share small amounts of data
    # Note: In Airflow 3.x, we explicitly push with a key for downstream tasks to pull
    ti = context["task_instance"]
    ti.xcom_push(key="data_logs_filename", value=data_logs_filename)
    
    print(f"📤 Pushed filename to XCom with key 'data_logs_filename'")
    
    # Return True to indicate file was found (sensor succeeds and DAG continues)
    return True


def _detect_data_drift(**context):
    """
    Analyze data for drift and determine which branch to take in the DAG.
    
    This function is used by a BranchPythonOperator, which means its return value
    determines which downstream task(s) will be executed next.
    
    Args:
        **context: Airflow context dictionary
        
    Returns:
        str: Task ID of the next task to execute:
             - "data_drift_detected" if drift is found (triggers alert/reporting)
             - "no_data_drift_detected" if no drift (normal operation continues)
             
    Note for Students:
        - BranchPythonOperator allows conditional logic in DAGs (like if/else statements)
        - Evidently's DataDriftPreset checks multiple statistical tests for drift
        - The report is converted to a dictionary to access the drift detection results
        - Only ONE branch will execute based on the return value
    """
    # Pull the filename from XCom (shared by the detect_file sensor)
    # In Airflow 3.x, we need to specify the task_id to pull from
    ti = context["task_instance"]
    data_logs_filename = ti.xcom_pull(
        task_ids="detect_file",
        key="data_logs_filename"
    )
    
    print(f"📥 Pulled from XCom: data_logs_filename = {data_logs_filename}")
    
    # Add validation to ensure we got a filename
    if not data_logs_filename:
        raise ValueError(
            "No data file found! The detect_file sensor should have pushed a filename to XCom. "
            "Check that the sensor task completed successfully and pushed the value."
        )
    
    print(f"📂 Loading data file: {data_logs_filename}")
    
    # Load both reference and current datasets
    reference, data_logs = _load_files(data_logs_filename)

    # Create an Evidently Report with DataDriftPreset
    # DataDriftPreset includes multiple drift detection metrics:
    # - Statistical tests for numerical features (e.g., Kolmogorov-Smirnov test)
    # - Chi-square test for categorical features
    # - Overall dataset drift summary
    data_drift_run = Report([
        DataDriftPreset()
    ])

    # Run the drift detection analysis
    # reference_data: The baseline dataset (what we expect)
    # current_data: The new data we're checking
    data_drift_result = data_drift_run.run(
        current_data=data_logs, 
        reference_data=reference
    )

    # Convert the report to a dictionary to access results
    report = data_drift_result.dict()

    # Check if dataset-level drift was detected
    # report["metrics"][0] is the DataDriftPreset results
    # ["result"]["dataset_drift"] is a boolean indicating if drift was found
    if report["metrics"][0]["value"]["count"] > 0:
        # Drift detected! Return the task_id to send alert and create report
        return "data_drift_detected"
    else:
        # No drift detected, normal operation
        return "no_data_drift_detected"


def _data_drift_detected(**context):
    """
    Generate and upload a detailed drift report to Evidently Cloud.
    
    This function is executed only when data drift is detected (via branching).
    It creates a comprehensive drift report and uploads it to Evidently Cloud
    for visualization and monitoring.
    
    Args:
        **context: Airflow context dictionary
        
    Note for Students:
        - This task only runs if the BranchPythonOperator returns "data_drift_detected"
        - Evidently Cloud provides a web interface to visualize drift reports
        - The report includes detailed statistics, charts, and drift scores
        - include_data=True uploads the actual data for deeper analysis in the UI
        - Make sure EVIDENTLY_CLOUD_TOKEN and EVIDENTLY_CLOUD_PROJECT_ID are set!
    """
    # Check if Evidently Cloud credentials are configured
    if not EVIDENTLY_CLOUD_TOKEN or not EVIDENTLY_CLOUD_PROJECT_ID:
        raise ValueError(
            "Evidently Cloud credentials not configured! "
            "Please set EVIDENTLY_CLOUD_TOKEN and EVIDENTLY_CLOUD_PROJECT_ID "
            "in Airflow Variables (Admin > Variables)"
        )

    # Connect to Evidently Cloud workspace using your credentials
    ws = CloudWorkspace(
        token=EVIDENTLY_CLOUD_TOKEN,
        url="https://app.evidently.cloud"  # Evidently Cloud platform URL
    )

    # Get the specific project where we want to upload the report
    project = ws.get_project(EVIDENTLY_CLOUD_PROJECT_ID)

    # Pull the filename from XCom (shared by the detect_file sensor)
    # In Airflow 3.x, we need to specify the task_id to pull from
    data_logs_filename = context["task_instance"].xcom_pull(
        task_ids="detect_file",
        key="data_logs_filename"
    )
    
    # Add validation to ensure we got a filename
    if not data_logs_filename:
        raise ValueError("No data file found! The detect_file sensor should have pushed a filename to XCom.")
    
    # Load the datasets
    reference, data_logs = _load_files(data_logs_filename)

    # Create a detailed drift report (same configuration as detection step)
    data_drift_report = Report([
        DataDriftPreset(),
    ])

    # Run the drift analysis
    data_drift_result = data_drift_report.run(
        current_data=data_logs, 
        reference_data=reference
    )
    
    # Upload the report to Evidently Cloud
    # This makes it viewable in the web UI with interactive visualizations
    # include_data=True allows drilling down into specific data points in the UI
    ws.add_run(
        project_id=project.id, 
        run=data_drift_result, 
        include_data=True
    )
    # Note: In some Evidently versions, you might need to use:
    # project.add_report(data_drift_report, include_data=True)
    
    print(f"✅ Drift report successfully uploaded to Evidently Cloud!")
    print(f"View it at: https://app.evidently.cloud/projects/{EVIDENTLY_CLOUD_PROJECT_ID}")


def _clean_file(**context):
    """
    Remove the processed data file from the data-drift directory.
    
    This function is optional and can be used to clean up files after processing
    to prevent reprocessing the same data in future DAG runs.
    
    Args:
        **context: Airflow context dictionary
        
    Note for Students:
        - This is useful for preventing disk space issues with accumulating files
        - Only use this if you're sure you don't need the raw files anymore
        - Consider backing up files elsewhere before deleting them
        - This task uses trigger_rule="none_failed_min_one_success" to run after branching
    """
    # Pull the filename from XCom
    # In Airflow 3.x, we need to specify the task_id to pull from
    data_logs_filename = context["task_instance"].xcom_pull(
        task_ids="detect_file",
        key="data_logs_filename"
    )
    
    # Safety check: Make sure the file exists before trying to delete it
    if data_logs_filename and os.path.exists(data_logs_filename):
        os.remove(data_logs_filename)
        print(f"🗑️  Cleaned up processed file: {data_logs_filename}")
    else:
        print(f"⚠️  File not found or already deleted: {data_logs_filename}")


# =============================================================================
# DAG Definition
# =============================================================================

# Create the DAG using a context manager
with DAG(
    dag_id="monitoring_dag",  # Unique identifier for this DAG
    default_args=default_args,  # Apply default configuration to all tasks
    schedule="0 16 * * *",  # Cron expression: run daily at 4:00 PM (updated from schedule_interval in Airflow 3.0+)
    catchup=False,  # Don't run for past dates when DAG is first enabled
    description="Monitor weather data for drift using Evidently AI",
    tags=["monitoring", "data-quality", "evidently"],  # Tags for organization in Airflow UI
) as dag:
    
    # =========================================================================
    # Task 1: Sensor - Wait for New Data Files
    # =========================================================================
    # This sensor continuously checks for new CSV files in the data-drift directory
    # It will "poke" (check) at regular intervals until a file is found
    detect_file = PythonSensor(
        task_id="detect_file",
        python_callable=_detect_file,  # Function to call for checking
        poke_interval=30,  # Check every 30 seconds
        timeout=600,  # Give up after 10 minutes (600 seconds)
        mode="poke",  # "poke" mode: blocks a worker slot while waiting
    )

    # =========================================================================
    # Task 2: Branch - Check for Data Drift
    # =========================================================================
    # This operator creates a conditional branch in the DAG workflow
    # Based on whether drift is detected, it will execute different downstream tasks
    detect_data_drift = BranchPythonOperator(
        task_id="detect_data_drift",
        python_callable=_detect_data_drift,  # Function returns which branch to take
    )

    # =========================================================================
    # Task 3a: Action if Drift Detected
    # =========================================================================
    # This task only runs if the branch operator returns "data_drift_detected"
    # It generates a detailed report and uploads it to Evidently Cloud
    data_drift_detected = PythonOperator(
        task_id="data_drift_detected",
        python_callable=_data_drift_detected,
    )

    # =========================================================================
    # Task 3b: Action if No Drift Detected
    # =========================================================================
    # This is an empty task (does nothing) that represents normal operation
    # It only runs if the branch operator returns "no_data_drift_detected"
    # Note: EmptyOperator replaced DummyOperator in Airflow 3.1+
    no_data_drift_detected = EmptyOperator(
        task_id="no_data_drift_detected",
    )

    # =========================================================================
    # Task 4: Clean Up (Optional)
    # =========================================================================
    # This task removes the processed file to prevent reprocessing
    # trigger_rule="none_failed_min_one_success" means:
    # - Run this task if at least one upstream task succeeded
    # - AND no upstream tasks failed
    # This allows it to run after either branch completes successfully
    clean_file = PythonOperator(
        task_id="clean_file",
        python_callable=_clean_file,
        trigger_rule="none_failed_min_one_success",  # Run after any successful branch
    )

    # =========================================================================
    # Task 5: End Marker
    # =========================================================================
    # Final empty task to mark the end of the DAG
    # Helps visualize the workflow completion in the Airflow UI
    # Note: EmptyOperator replaced DummyOperator in Airflow 3.1+
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",  # Run after any successful branch
    )

    # =========================================================================
    # DAG Workflow Definition
    # =========================================================================
    # Define the order and dependencies between tasks using >> operator
    #
    # Workflow:
    # 1. detect_file (sensor) waits for new data
    #    ↓
    # 2. detect_data_drift (branch) analyzes for drift
    #    ↓
    # 3. Branches into TWO paths:
    #    → data_drift_detected (upload report to Cloud)
    #    → no_data_drift_detected (do nothing)
    #    ↓
    # 4. Both branches converge to clean_file (cleanup)
    #    ↓
    # 5. end (completion marker)
    #
    # Note for Students:
    # - >> means "then" or "followed by"
    # - [task1, task2] creates parallel branches
    # - Tasks in [] will run independently based on the branch condition
    
    detect_file >> detect_data_drift
    detect_data_drift >> [data_drift_detected, no_data_drift_detected]
    [data_drift_detected, no_data_drift_detected] >> clean_file >> end
