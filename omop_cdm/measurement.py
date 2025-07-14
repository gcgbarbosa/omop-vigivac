from pyspark.sql.types import IntegerType, DateType, TimestampType
from pyspark.sql.functions import col

# Assuming spark_session.py contains:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("OMOPDataIngestion").getOrCreate()
from spark_session import spark

import postgress_parameters as pg_params


class Measurement:
    """
    Handles the ingestion of data from a Parquet file into the
    PostgreSQL 'public.measurement' table.
    """

    @classmethod
    def ingest(cls):
        """
        Reads the measurement Parquet file, transforms the data,
        and loads it into the PostgreSQL 'public.measurement' table.
        """

        parquet_file_path = (
            "parquet/tb_measurement_data.parquet"  # Can be HDFS, S3, local, etc.
        )
        POSTGRES_TABLE = "public.measurement"

        # Define columns that are NOT NULL in the PostgreSQL target table.
        # Rows with NULLs in any of these columns will be dropped before loading.
        NOT_NULL_COLUMNS = [
            "measurement_id",
            "person_id",
            "measurement_concept_id",
            "measurement_date",
            "measurement_type_concept_id",
        ]

        try:
            print(f"Reading Parquet file from: {parquet_file_path}")
            df_parquet = spark.read.parquet(parquet_file_path)

            print("Parquet file schema for measurement:")
            df_parquet.printSchema()
            print("First 5 rows of Parquet data for measurement:")
            df_parquet.show(5)

            # Select and cast columns to match the PostgreSQL 'measurement' table schema.
            # All relevant Parquet columns are already IntegerType or DateType,
            # so direct casting to IntegerType or DateType is appropriate.
            # Columns in PostgreSQL not present in Parquet (and are nullable) will be NULL.
            df_transformed = df_parquet.select(
                col("measurement_id").cast(IntegerType()),
                col("person_id").cast(IntegerType()),
                col("measurement_concept_id").cast(IntegerType()),
                col("measurement_date").cast(DateType()),
                # measurement_datetime not in parquet, will be NULL in PG
                # measurement_time not in parquet, will be NULL in PG
                col("measurement_type_concept_id").cast(IntegerType()),
                # operator_concept_id not in parquet, will be NULL in PG
                # value_as_number not in parquet, will be NULL in PG
                col("value_as_concept_id").cast(IntegerType()),
                # unit_concept_id not in parquet, will be NULL in PG
                # range_low not in parquet, will be NULL in PG
                # range_high not in parquet, will be NULL in PG
                # provider_id not in parquet, will be NULL in PG
                col("visit_occurrence_id").cast(IntegerType()),
                # visit_detail_id not in parquet, will be NULL in PG
                # measurement_source_value not in parquet, will be NULL in PG
                # measurement_source_concept_id not in parquet, will be NULL in PG
                # unit_source_value not in parquet, will be NULL in PG
                # value_source_value not in parquet, will be NULL in PG
            )

            print("\nTransformed DataFrame schema (before null filtering):")
            df_transformed.printSchema()
            print("First 5 rows of transformed data (before null filtering):")
            df_transformed.show(5)

            # Drop records with NULLs in columns that are defined as NOT NULL in PostgreSQL.
            print(f"\nDropping rows with NULLs in required columns: {NOT_NULL_COLUMNS}")
            initial_row_count = df_transformed.count()
            df_filtered = df_transformed.na.drop(subset=NOT_NULL_COLUMNS)
            filtered_row_count = df_filtered.count()

            print(f"Initial row count: {initial_row_count}")
            print(f"Row count after filtering NULLs: {filtered_row_count}")
            print(f"Number of rows dropped: {initial_row_count - filtered_row_count}")

            print("\nFirst 5 rows of filtered data:")
            df_filtered.show(5)

            # Write the filtered and transformed DataFrame to PostgreSQL.
            print(f"\nWriting data to PostgreSQL table: {POSTGRES_TABLE}")
            df_filtered.write.format("jdbc").option("url", pg_params.jdbc_url).option(
                "dbtable", POSTGRES_TABLE
            ).options(**pg_params.connection_properties).mode("append").save()

            print(
                f"\nData successfully loaded into PostgreSQL table: {POSTGRES_TABLE}!"
            )

        except Exception as e:
            print(f"An error occurred during Measurement ingestion: {e}")
            # Consider more specific error handling or logging here.
