from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import col

# Assuming spark_session.py contains:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("OMOPDataIngestion").getOrCreate()
from spark_session import spark

import postgress_parameters as pg_params


class ObservationPeriod:
    """
    Handles the ingestion of data from a Parquet file into the
    PostgreSQL 'public.observation_period' table.
    """

    @classmethod
    def ingest(cls):
        """
        Reads the observation_period Parquet file, transforms the data,
        and loads it into the PostgreSQL 'public.observation_period' table.
        """
        parquet_file_path = "parquet/tb_observation_period.parquet"
        POSTGRES_TABLE = "public.observation_period"

        # Define columns that are NOT NULL in the PostgreSQL target table.
        # Rows with NULLs in any of these columns will be dropped before loading.
        NOT_NULL_COLUMNS = [
            "observation_period_id",
            "person_id",
            "observation_period_start_date",
            "observation_period_end_date",
            "period_type_concept_id",
        ]

        try:
            print(f"Reading Parquet file from: {parquet_file_path}")
            df_parquet = spark.read.parquet(parquet_file_path)

            print("Parquet file schema for observation_period:")
            df_parquet.printSchema()
            print("First 5 rows of Parquet data for observation_period:")
            df_parquet.show(5)

            # Select and cast columns to match the PostgreSQL 'observation_period' table schema.
            # Columns not present in the PostgreSQL table (like 'id_vigvac') are ignored.
            # Columns in PostgreSQL that are not in Parquet (and are nullable) will be NULL.
            df_transformed = df_parquet.select(
                col("observation_period_id").cast(IntegerType()),
                col("person_id").cast(IntegerType()),
                col("observation_period_start_date").cast(DateType()),
                col("observation_period_end_date").cast(DateType()),
                col("period_type_concept_id").cast(
                    IntegerType()
                ),  # Cast string to integer
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
            print(f"An error occurred during ObservationPeriod ingestion: {e}")
            # Consider more specific error handling or logging here.
