from spark_session import spark
import postgress_parameters as pg_params

from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import col

class Person:
    @classmethod
    def ingest(cls):
        parquet_file_path = "parquet/tb_person.parquet"  # Can be HDFS, S3, local, etc.
        df_parquet = spark.read.parquet(parquet_file_path)

        df_parquet.printSchema()  # Inspect the schema
        df_parquet.show(5)  # See some data

        POSTGRES_TABLE = (
            "person"  # Ensure this matches your table name, including schema if applicable
        )

        NOT_NULL_COLUMNS = [
            "person_id",
            "gender_concept_id",
            "year_of_birth",
            "race_concept_id",
            "ethnicity_concept_id"
        ]

        df_transformed = df_parquet.select(
            col("person_id").cast(IntegerType()),
            col("gender_concept_id").cast(
                IntegerType()
            ),  # Assuming this string can be cast to integer
            col("year_of_birth").cast(IntegerType()),
            col("month_of_birth").cast(IntegerType()),
            col("day_of_birth").cast(IntegerType()),
            col("birth_datetime").cast(
                TimestampType()
            ),  # Ensure this is compatible, Spark's timestamp should work
            col("race_concept_id").cast(
                IntegerType()
            ),  # Assuming this string can be cast to integer
            col("ethnicity_concept_id").cast(
                IntegerType()
            ),  # Assuming this string can be cast to integer
            col("gender_source_value"),
            col("race_source_value"),
        ).dropna(subset=NOT_NULL_COLUMNS)


        df_transformed.write.format("jdbc").option("url", pg_params.jdbc_url).option(
            "dbtable", POSTGRES_TABLE
        ).options(**pg_params.connection_properties).mode("append").save()
