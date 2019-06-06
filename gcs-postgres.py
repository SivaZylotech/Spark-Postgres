from utils.gcs_helper import GCSHelper
import io
import os
import pandas as pd
from pyspark.sql import SparkSession 

spark = SparkSession.builder.getOrCreate()

gcs_client = GCSHelper()
gcs_dataset = gcs_client.download_io_object_from_gcs('zylo-test', "test.csv").decode("utf-8")
first_row = True
schema = None
total_data = []
print(gcs_dataset.split("\n"))
for row in gcs_dataset.split("\n"):
    if first_row:
        schema = row.split(",")
        schema[-1] = schema[-1].rstrip()
        schema = tuple(schema)
        first_row = False
    else:
        temp_data = row.split(",")
        temp_data[-1] = temp_data[-1].rstrip()
        total_data.append(tuple(temp_data))

df = spark.createDataFrame(total_data, schema=schema)
print(df.show())
df.select("contact_id") \
  .show()

mode = "overwrite"
url = "jdbc:postgresql://<ip>:5432/postgres"
properties = {"user": "postgres","password": "postgres","driver": "org.postgresql.Driver"}
df.write.jdbc(url=url, table="zt_contacts", mode=mode, properties=properties)
