from pyspark.sql.functions import col, substring
from pyspark.sql.types import DateType, IntegerType

import postgress_parameters as pg_params

# Assuming spark_session.py contains:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("OMOPDataIngestion").getOrCreate()
from spark_session import spark


class DrugExposure:
    """
    Handles the ingestion of data from a Parquet file into the
    PostgreSQL 'public.drug_exposure' table.
    """

    @classmethod
    def ingest(cls):
        """
        Reads the drug_exposure Parquet file, transforms the data,
        and loads it into the PostgreSQL 'public.drug_exposure' table.
        """

        parquet_file_path = (
            "parquet/tb_drug_exposure_v6.parquet"  # Can be HDFS, S3, local, etc.
        )
        POSTGRES_TABLE = "drug_exposure"

        # Define columns that are NOT NULL in the PostgreSQL target table.
        # Rows with NULLs in any of these columns will be dropped before loading.
        NOT_NULL_COLUMNS = [
            "drug_exposure_id",
            "person_id",
            "drug_concept_id",
            "drug_exposure_start_date",
            "drug_exposure_end_date",  # Mapped from parquet.drug_exposure_end_datetime
            "drug_type_concept_id",
        ]

        try:
            print(f"Reading Parquet file from: {parquet_file_path}")
            df_parquet = spark.read.parquet(parquet_file_path)

            print("Parquet file schema for drug_exposure:")
            df_parquet.printSchema()
            print("First 5 rows of Parquet data for drug_exposure:")
            df_parquet.show(5)

            # Select and cast columns to match the PostgreSQL 'drug_exposure' table schema.
            # - Parquet 'long' types are cast to IntegerType for PostgreSQL 'integer'.
            # - Parquet 'date' types are cast to DateType for PostgreSQL 'date'.
            # - Parquet 'string' types for concept IDs are cast to IntegerType.
            # - Note: PostgreSQL 'numeric' for quantity is handled by Spark's default
            #   conversion from numeric types, or can be explicitly cast if needed.
            df_transformed = df_parquet.select(
                col("drug_exposure_id").cast(IntegerType()),
                col("person_id").cast(IntegerType()),
                col("drug_concept_id").cast(
                    IntegerType()
                ),  # Parquet is long, PG is integer
                col("drug_exposure_start_date").cast(DateType()),
                # drug_exposure_start_datetime not in parquet, will be NULL in PG
                # Map parquet.drug_exposure_end_datetime (date) to PG's drug_exposure_end_date (date)
                col("drug_exposure_end_datetime")
                .alias("drug_exposure_end_date")
                .cast(DateType()),
                # drug_exposure_end_datetime (PG) will be NULL as it's not in parquet
                # verbatim_end_date not in parquet, will be NULL in PG
                col("drug_type_concept_id").cast(
                    IntegerType()
                ),  # Parquet is string, PG is integer
                # stop_reason not in parquet, will be NULL in PG
                # refills not in parquet, will be NULL in PG
                # quantity not in parquet, will be NULL in PG
                # days_supply not in parquet, will be NULL in PG
                # sig not in parquet, will be NULL in PG
                # route_concept_id not in parquet, will be NULL in PG
                # lot_number not in parquet, will be NULL in PG
                # provider_id not in parquet, will be NULL in PG
                col("visit_occurrence_id").cast(
                    IntegerType()
                ),  # Parquet is long, PG is integer
                # visit_detail_id not in parquet, will be NULL in PG

                substring(col("drug_source_value"), 1, 50).alias("drug_source_value"),
                # drug_source_concept_id not in parquet, will be NULL in PG
                # route_source_value not in parquet, will be NULL in PG
                # dose_unit_source_value not in parquet, will be NULL in PG
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
            print(f"An error occurred during DrugExposure ingestion: {e}")
            # Consider more specific error handling or logging here.
