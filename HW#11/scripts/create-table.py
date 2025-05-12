from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Создание Spark-сессии
spark = SparkSession.builder \
    .appName("json-parse-to-parquet") \
    .enableHiveSupport() \
    .getOrCreate()

# Пути к файлам
json_path = "s3a://etl-sources/user_activity_data.json"  
parquet_sessions_path = "s3a://airflow-dataproc/sessions.parquet"
parquet_actions_path = "s3a://airflow-dataproc/actions.parquet"

# Чтение JSON
raw_df = spark.read.option("multiline", "true").json(json_path)

# Извлечение и развертывание массивов
sessions_df = raw_df.select(F.explode("sessions").alias("session")).select("session.*")
actions_df = raw_df.select(F.explode("actions").alias("action")).select("action.*")

# Сохранение в формате Parquet
sessions_df.write.mode("overwrite").parquet(parquet_sessions_path)
actions_df.write.mode("overwrite").parquet(parquet_actions_path)
