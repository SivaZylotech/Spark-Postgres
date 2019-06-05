from utils.gcs_helper import GCSHelper
import io
import os
import pandas as pd
from pyspark.sql import SparkSession 

gcs_client = GCSHelper()
out = gcs_client.download_io_object_from_gcs('zylo-test', "test.csv").decode("utf-8")
with open("temp.csv", "w") as fp:
    for row in out.split("\n"):
        fp.write(row)
        fp.write("\n")

spark = SparkSession.builder.getOrCreate()
df = spark.read \
          .format("csv") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .load("temp.csv")
df.show()
#df.select(df.email, df.gender) \
#  .filter(df.contact_id == 0) \
#  .show()

mode = "overwrite"
url = "jdbc:postgresql://<ip>:5432/postgres"
properties = {"user": "postgres","password": "postgres","driver": "org.postgresql.Driver"}
df.write.jdbc(url=url, table="zt_contacts", mode=mode, properties=properties)

os.remove("temp.csv")
