# udacity-nd027-data-pipelines

Project submission for Udacity Data Engineering Nanodegree - Data Pipelines

## Summary

## Install

In `airflow.cfg` update `dags_folder` and `plugins_folder` to the project subdirectories. Set `load_examples = False`.

```bash
$ pip install -r requirements.txt
$ airflow initdb
$ airflow scheduler
$ airflow webserver
```

## Pipeline

![DAG graph](img/dag-graph.png)
