from pyspark.sql.types import IntegerType, DateType, TimestampType
from pyspark.sql.functions import col

# Assuming spark_session.py contains:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("OMOPDataIngestion").getOrCreate()
from spark_session import spark

import postgress_parameters as pg_params


class VisitOccurrence:
    """
    Handles the ingestion of data from a Parquet file into the
    PostgreSQL 'public.visit_occurrence' table.
    """

    @classmethod
    def ingest(cls):
        """
        Reads the visit_occurrence Parquet file, transforms the data,
        and loads it into the PostgreSQL 'public.visit_occurrence' table.
        """
        parquet_file_path = "parquet/tb_visit_occurrence_data.parquet"
        POSTGRES_TABLE = "visit_occurrence"

        # Define columns that are NOT NULL in the PostgreSQL target table.
        # Rows with NULLs in any of these columns will be dropped before loading.
        NOT_NULL_COLUMNS = [
            "visit_occurrence_id",
            "person_id",
            "visit_concept_id",
            "visit_start_date",
            "visit_end_date",
            "visit_type_concept_id",
        ]

        try:
            print(f"Reading Parquet file from: {parquet_file_path}")
            df_parquet = spark.read.parquet(parquet_file_path)

            print("Parquet file schema for visit_occurrence:")
            df_parquet.printSchema()
            print("First 5 rows of Parquet data for visit_occurrence:")
            df_parquet.show(5)

            # Select and cast columns to match the PostgreSQL 'visit_occurrence' table schema.
            # Columns in PostgreSQL not present in Parquet (and are nullable) will be NULL.
            # Parquet 'long' maps to Spark's LongType, casting to IntegerType for PostgreSQL 'integer'.
            # Parquet 'string' for care_site_id needs to be cast to IntegerType for PostgreSQL.
            df_transformed = df_parquet.select(
                col("visit_occurrence_id").cast(IntegerType()),
                col("person_id").cast(IntegerType()),
                col("visit_concept_id").cast(IntegerType()),
                col("visit_start_date").cast(DateType()),
                # visit_start_datetime not in parquet, will be NULL in PG
                col("visit_end_date").cast(DateType()),
                # visit_end_datetime not in parquet, will be NULL in PG
                col("visit_type_concept_id").cast(IntegerType()),
                # provider_id not in parquet, will be NULL in PG
                col("care_site_id").cast(
                    IntegerType()
                ),  # Parquet is string, PG is integer
                # visit_source_value not in parquet, will be NULL in PG
                # visit_source_concept_id not in parquet, will be NULL in PG
                # admitting_source_concept_id not in parquet, will be NULL in PG
                # admitting_source_value not in parquet, will be NULL in PG
                # discharge_to_concept_id not in parquet, will be NULL in PG
                # discharge_to_source_value not in parquet, will be NULL in PG
                # preceding_visit_occurrence_id not in parquet, will be NULL in PG
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
            print(f"An error occurred during VisitOccurrence ingestion: {e}")
            # Consider more specific error handling or logging here.
