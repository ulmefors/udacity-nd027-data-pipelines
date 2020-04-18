import logging

from airflow import DAG
from airflow.operators import LoadDimensionOperator

def get_load_dimension_table_subdag(
        parent_dag_name,
        task_id,
        default_args,
        postgres_conn_id,
        sql_queries,
        tables,
        truncate_flags,
        *args,
        **kwargs):

    dag = DAG(
        dag_id=f'{parent_dag_name}.{task_id}',
        default_args=default_args,
        **kwargs,
    )

    if (len(tables) != len(sql_queries)) or (len(sql_queries) != len(truncate_flags)):
        logging.error('Tables, SQL queries and truncate settings not of same length')
        raise ValueError('Tables, SQL queries and truncate settings not of same length')

    tasks = []
    for table, query, truncate in zip(tables, sql_queries, truncate_flags):
        task = LoadDimensionOperator(
            task_id=f'Load_{table}_dim_table',
            dag=dag,
            postgres_conn_id=postgres_conn_id,
            sql=query,
            table=table,
            truncate=truncate,
        )
        tasks.append(task)

    return dag
