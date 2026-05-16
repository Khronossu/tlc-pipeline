"""Create Iceberg namespaces for silver and gold layers."""
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("init_namespaces").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for ns in ["iceberg.meta", "iceberg.silver", "iceberg.gold", "iceberg.serving"]:
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        print(f"Namespace ready: {ns}", flush=True)
    except Exception as e:
        if "already exists" in str(e).lower() or "AlreadyExistsException" in str(e):
            print(f"Namespace already exists: {ns}", flush=True)
        else:
            print(f"Warning: could not create {ns}: {e}", flush=True)

spark.stop()
sys.exit(0)
