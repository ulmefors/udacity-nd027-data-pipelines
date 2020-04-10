# udacity-nd027-data-pipelines

Project submission for Udacity Data Engineering Nanodegree - Data Pipelines

## Summary

## Install

In `airflow.cfg` update `dags_folder` and `plugins_folder` to the project subdirectories. Set `load_examples = False`.

Install python requirements
```bash
$ pip install -r requirements.txt
```

Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

Choose DB/PASSWORD in `redshift.cfg`.

Create IAM role, Redshift cluster, and configure TCP connectivity
```bash
$ python create_redshift_cluster.py
```

Take note of Redshift cluster endpoint.

```bash
$ airflow initdb
$ airflow scheduler
$ airflow webserver
```

### Airflow web UI

Go to Admin > Connections apge and click `Create`.

On the create connection page, enter the following values:

* Conn Id: `aws_credentials`
* Conn Type: `Amazon Web Services`
* Login: AWS Access Key ID
* Password: AWS Secret Access Key

Click `Save and Add Another`

* Conn Id: `redshift`
* Conn Type: `Postgres`
* Host: `<Redshift cluster endpoint>`
* Schema: `dev`
* Login: `awsuser`
* Password: `<Redshift password>`
* Port: `5439`

## Pipeline

![DAG graph](img/dag-graph.png)
