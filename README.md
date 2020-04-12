# udacity-nd027-data-pipelines

Project submission for Udacity Data Engineering Nanodegree - Data Pipelines

## Summary

## Install

Install python requirements

```bash
$ pip install -r requirements.txt
```

Configure Airflow

In `airflow.cfg` (`~/airflow`) update `dags_folder` and `plugins_folder` to the project subdirectories. Set `load_examples = False`.

Set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

Choose `DB/PASSWORD` in `redshift.cfg`.

Create IAM role, Redshift cluster, configure TCP connectivity, and create Redshift tables
```bash
$ python create_redshift_cluster.py --query_file create_tables.sql
```

### Start Airflow

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
* Host: `<Redshift cluster endpoint from redshift.cfg>`
* Schema: `dev`
* Login: `awsuser`
* Password: `<Redshift db password from redshift.cfg>`
* Port: `5439`

### Tear down

Delete IAM role and Redshift cluster

```
$ python create_cluster.py --delete
```

## Pipeline

![DAG graph](img/dag-graph.png)

## Further work

* Load dimensions with a subdag