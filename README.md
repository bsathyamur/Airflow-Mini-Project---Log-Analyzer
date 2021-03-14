# Airflow-Mini-Project- Log-Analyzer

#### Objective: create a log analyzer in Python to monitor the DAG Airflow log errors for the scheduled jobs. The log analyzer should show the following information:
- The total count of error messages
- A detailed message regarding each error

#### Details of the DAG

1. Task1 - python operator to the parse the log files from airflow log folder for the job tasks related to TSLA
2. Task2 - Bash operator to print the cumulative error count and error list for the job tasks related to TSLA
3. Task3 - python operator to the parse the log files from airflow log folder for the job tasks related to AAPL
4. Task4 - Bash operator to print the cumulative error count and error list for the job tasks related to AAPL

#### DAG Dependencies

Task1 >> Task2
Task3 >> Task4

For sample error log output refer bashoperator-log-sample.log
