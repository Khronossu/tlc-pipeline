"""Load taxi_zone_lookup CSV into Iceberg meta.taxi_zone_lookup."""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

CSV_PATH = "/opt/project/dbt/seeds/taxi_zone_lookup.csv"
TABLE = "iceberg.meta.taxi_zone_lookup"

spark = (
    SparkSession.builder
    .appName("seed_taxi_zones")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

df = spark.read.csv(CSV_PATH, header=True, schema=schema)

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.meta")

df.writeTo(TABLE).createOrReplace()

count = spark.table(TABLE).count()
print(f"Loaded {count} rows into {TABLE}", flush=True)
spark.stop()
sys.exit(0)
