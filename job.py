from pyspark.sql import SparkSession

# Create the spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.5.jar") \
    .getOrCreate()

# Dummy data
data = [{"id":100, "name": "siva"}, {"id":200, "name": "sankar"}] 
df = spark.createDataFrame(data)
print("-----")
df.show()
print("-----")

# Write to postgreSQL
mode = "overwrite"
url = "jdbc:postgresql://<ip>:5432/postgres"
properties = {"user": "postgres","password": "postgres","driver": "org.postgresql.Driver"}
df.write.jdbc(url=url, table="test", mode=mode, properties=properties)

# Read from PostgreSQL
df = spark.read \
     .format("jdbc") \
     .option("url", "jdbc:postgresql://<ip>:5432/postgres") \
     .option("dbtable", "test") \
     .option("user", "postgres") \
     .option("password", "postgres") \
     .option("driver", "org.postgresql.Driver") \
     .load()

print("=====")
df.show()
print("=====")
