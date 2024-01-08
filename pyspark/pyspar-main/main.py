"""Entry point for the ETL application Example skeleton script
"""
from pyspark.sql import SparkSession



# ## Get or create spark or spark://spark:7077
spark: SparkSession = SparkSession.builder\
    .appName('ETL Concept Script')\
    .master('spark://spark:7077')\
    .getOrCreate()

print("pyspark",spark.version)
# # Read the source CSV file into a DataFrame
df = spark.read.csv("/opt/data/raw_data.csv", sep=',', header=True, inferSchema=True)
df.write.csv("/opt/data/output.csv")
