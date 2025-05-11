sql=SQLContext(sc)
import pyspark.sql.functions as F



# Пути к файлам
json_path = ""  
parquet_sessions_path = ""
parquet_actions_path = ""

# Чтение JSON
raw_df = spark.read.option("multiline", "true").json(json_path)

# Извлечение и развертывание массивов
sessions_df = raw_df.select(F.explode("sessions").alias("session")).select("session.*")
actions_df = raw_df.select(F.explode("actions").alias("action")).select("action.*")

# Сохранение в формате Parquet
sessions_df.write.mode("overwrite").parquet(parquet_sessions_path)
actions_df.write.mode("overwrite").parquet(parquet_actions_path)

