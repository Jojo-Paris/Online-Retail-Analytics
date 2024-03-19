from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

from airflow.models.baseoperator import chain


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['retail'],
)

def retail():

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/datasets/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='jodeci_retail_bucket',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id="retail",
        gcp_conn_id="gcp",
    )

    gcs_to_bigq = GCSToBigQueryOperator(
        task_id='gcs_to_bigq',
	    bucket='jodeci_retail_bucket/raw',
        source_objects=['online_retail.csv'],
        destination_project_dataset_table='airflow-retail-416720.retail.raw_invoices',
        schema_fields=[
                        {'name': 'InvoiceNo', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'StockCode', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'Quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'InvoiceDate', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'UnitPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'CustomerID', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
                    ],
        gcp_conn_id="gcp",
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.checks.check_function import check

        return check(scan_name, checks_subpath)
    
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform'],
        ))
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.checks.check_function import check

        return check(scan_name, checks_subpath)
    
    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report'],
        ))
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.checks.check_function import check

        return check(scan_name, checks_subpath)

    chain(
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_bigq,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report()
    )
    
retail()