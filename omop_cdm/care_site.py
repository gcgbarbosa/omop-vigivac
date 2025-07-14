from pyspark.sql.functions import col, substring
from pyspark.sql.types import IntegerType

import postgress_parameters as pg_params

from spark_session import spark


class CareSite:
    """
    Handles the ingestion of data from a Parquet file into the
    PostgreSQL 'public.care_site' table.
    Adjusted to handle string length limits and type casting.
    """

    @classmethod
    def ingest(cls):
        """
        Reads the care_site Parquet file, transforms the data,
        and loads it into the PostgreSQL 'public.care_site' table.
        """
        POSTGRES_TABLE = "care_site"

        # Define columns that are NOT NULL in the PostgreSQL target table.
        # Rows with NULLs in any of these columns will be dropped before loading.
        NOT_NULL_COLUMNS = [
            "care_site_id",
        ]

        try:
            csv_file_path = "csv/tb_care_site.csv"  # Can be HDFS, S3, local, etc.
            df_csv = spark.read.csv(csv_file_path, header=True)

            print("Parquet file schema for care_site:")
            df_csv.printSchema()
            print("First 5 rows of Parquet data for care_site:")
            df_csv.show(5)

            # Select and cast columns to match the PostgreSQL 'care_site' table schema.
            # - Parquet 'string' types for IDs are cast to IntegerType.
            # - String columns are truncated to fit PostgreSQL VARCHAR limits.
            df_transformed = df_csv.select(
                col("care_site_id").cast(
                    IntegerType()
                ),  # Parquet is string, PG is integer
                # Truncate care_site_name to 255 characters
                substring(col("care_site_name"), 1, 255).alias("care_site_name"),
                # TODO: had to remove place_of_service because it was not mapped
                # col("place_of_service_concept_id").cast(
                #     IntegerType()
                # ),  # Parquet is string, PG is integer
                col("location_id").cast(
                    IntegerType()
                ),  # Parquet is string, PG is integer
                # Truncate care_site_source_value to 50 characters
                substring(col("care_site_source_value"), 1, 50).alias(
                    "care_site_source_value"
                ),
                # Truncate place_of_service_source_value to 50 characters
                substring(col("place_of_service_source_value"), 1, 50).alias(
                    "place_of_service_source_value"
                ),
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
            print(f"An error occurred during CareSite ingestion: {e}")
            # Consider more specific error handling or logging here.
