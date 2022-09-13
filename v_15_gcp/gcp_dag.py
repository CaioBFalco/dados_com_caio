####################################################################################
## AUTOR: CAIO FALCO (DADOS COM CAIO)
## DATA: 12/09/2022
## DESCRICAO: DAG QUE REALIZA CARGA DE ARQUIVOS CSV NO BIGQUERY, GERA UMA TABELA FILTRADA E MOVE OS ARQUIVOS FONTES.
####################################################################################

from datetime import datetime, timedelta  
import airflow
from airflow import DAG
from airflow.operators import bash_operator
import airflow.providers.google.cloud.operators.bigquery
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'schedule_interval': '30 20 * * *'
}

with DAG(    
    dag_id='gcp_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['gcp','dados-com-caio','big-query'],
) as dag:    

	t1_load_csv_bq = GCSToBigQueryOperator(
    task_id='loading_csv_from_gcs_to_bq',
    bucket='meu-bucket-gcs',
    source_objects='v15/landing_zone/*.csv',
	field_delimiter=',',
	skip_leading_rows=1,
    destination_project_dataset_table=f"meu_dataset.tb_raw_ny_bike_trips_airflow", 
    schema_fields=[
        {'name': 'tripduration', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'starttime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
		{'name': 'stoptime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'start_station_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
		{'name': 'start_station_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'start_station_latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
		{'name': 'start_station_longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'end_station_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
		{'name': 'end_station_name', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'end_station_latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'end_station_longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
		{'name': 'usertype', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'birth_year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
		{'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

	t2_creating_table_from_query = BigQueryExecuteQueryOperator(
    task_id="querying_and_creating_bq_table",
    sql="""
    SELECT
      *
    FROM
      `projeto-gcp-356114.meu_dataset.tb_raw_ny_bike_trips_airflow`
	WHERE STARTTIME BETWEEN '2017-01-01T00:00:00' AND '2017-06-30T23:59:59' AND
	GENDER = 'female' AND 
	birth_year > 1999
    """,
    destination_dataset_table=f"projeto-gcp-356114.meu_dataset.tb_filtered_ny_bike_trips_airflow",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="cnx-composer-bigquery",
    use_legacy_sql=False,
)

	t3_move_src_files_to_bkp = GCSToGCSOperator(
    task_id="move_src_file_to_bkp",
    source_bucket='meu-bucket-gcs',
    source_object='v15/landing_zone/*.csv',
    destination_bucket='meu-bucket-gcs',
    destination_object='v15/bkp/',
    move_object=True,
)

	t1_load_csv_bq >> t2_creating_table_from_query >> t3_move_src_files_to_bkp


