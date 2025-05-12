import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Данные вашей инфраструктуры
YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBCDEFCZEFdbcB7PXI4jhZ5k8m841QLSqokns7g/95PP satantsev@LAPTOP-GHSE3I1H'
YC_DP_SUBNET_ID = 'e9bn19s0n5rk3bp454pl'
YC_DP_SA_ID = 'ajebq3jhtfqf3cbqujlo'
YC_DP_METASTORE_URI = '10.128.0.30'
YC_BUCKET = 'airflow-dataproc'

with DAG(
    'DATA_INGEST',
    schedule_interval='@hourly',
    tags=['data-processing-and-airflow'],
    start_date=datetime.datetime.now(),
    max_active_runs=1,
    catchup=False
) as ingest_dag:
    
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.1',
        masternode_resource_preset='s2.small',   # минимальный ресурсный пресет
        masternode_disk_type='network-hdd',
        masternode_disk_size=32,                 # уменьшенный размер диска
        computenode_resource_preset='s2.small',  # уменьшенный ресурсный пресет
        computenode_disk_type='network-hdd',
        computenode_disk_size=32,                # уменьшенный размер диска
        computenode_count=1,                     # уменьшенное количество узлов
        computenode_max_hosts_count=3,           # уменьшенное максимальное масштабирование
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/create-table.py',
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
