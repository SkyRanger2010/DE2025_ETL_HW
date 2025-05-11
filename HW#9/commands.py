sql=SQLContext(sc)
import pyspark.sql.functions as F

df = spark.read.option("multiline", "true").json("s3a://etl-storages/json_data.json", miltiLine=True)
