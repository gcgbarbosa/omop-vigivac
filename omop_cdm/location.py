from pyspark.sql.functions import col, substring
from pyspark.sql.types import IntegerType

import postgress_parameters as pg_params

# Assuming spark_session.py contains:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("OMOPDataIngestion").getOrCreate()
from spark_session import spark


class Location:
    """
    Handles the ingestion of data from a Parquet file into the
    PostgreSQL 'public.location' table.
    Adjusted to handle string length limits and type casting.
    """

    @classmethod
    def ingest(cls):
        """
        Reads the location Parquet file, transforms the data,
        and loads it into the PostgreSQL 'public.location' table.
        """
        parquet_file_path = "parquet/tb_location.parquet"
        POSTGRES_TABLE = "public.location"

        # Define columns that are NOT NULL in the PostgreSQL target table.
        # Rows with NULLs in any of these columns will be dropped before loading.
        NOT_NULL_COLUMNS = [
            "location_id",
        ]

        try:
            print(f"Reading Parquet file from: {parquet_file_path}")
            df_parquet = spark.read.parquet(parquet_file_path)

            print("Parquet file schema for location:")
            df_parquet.printSchema()
            print("First 5 rows of Parquet data for location:")
            df_parquet.show(5)

            # Select and cast columns to match the PostgreSQL 'location' table schema.
            # - Parquet 'integer' for location_id is directly compatible with PG 'integer'.
            # - String columns are truncated to fit PostgreSQL VARCHAR limits.
            # - Columns in PG not in Parquet (address_1, address_2, zip) will be NULL.
            # - Columns in Parquet not in PG (country_concept_id, country_source_value) are ignored.
            df_transformed = df_parquet.select(
                col("location_id").cast(IntegerType()),
                # address_1 not in parquet, will be NULL in PG
                # address_2 not in parquet, will be NULL in PG
                substring(col("city"), 1, 50).alias("city"),  # Truncate to 50 chars
                substring(col("state"), 1, 2).alias("state"),  # Truncate to 2 chars
                # zip not in parquet, will be NULL in PG
                substring(col("county"), 1, 20).alias("county"),  # Truncate to 20 chars
                substring(col("location_source_value"), 1, 50).alias(
                    "location_source_value"
                ),  # Truncate to 50 chars
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
            print(f"An error occurred during Location ingestion: {e}")
            # Consider more specific error handling or logging here.
