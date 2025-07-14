from pyspark.sql import SparkSession

# --- Configuration for JDBC driver ---
# Replace with the actual path to your downloaded PostgreSQL JDBC driver JAR
jdbc_driver_path = "/Users/gcgbarbosa/repos/fio/omop/postgresql-42.7.5.jar"
# Or, if running on a cluster, ensure the JAR is available on the classpath of the executors.
# For local testing, you can use --driver-class-path and --jars in spark-submit
# or configure it directly in the SparkSession as shown below.

spark = (
    SparkSession.builder.appName("ParquetToPostgres")
    .config("spark.jars", jdbc_driver_path)
    .config("spark.driver.extraClassPath", jdbc_driver_path)
    .getOrCreate()
)
