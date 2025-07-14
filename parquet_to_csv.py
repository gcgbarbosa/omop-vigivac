import pandas as pd

# 1. Define your file paths
parquet_file_path = "tb_condition_occurence_data.parquet"
csv_file_path = "tb_condition_occurence_data.csv"

parquet_file_path = "tb_condition_occurence_from_measurement.parquet"
csv_file_path = "tb_condition_occurence_from_measurement.csv"

parquet_file_path = "tb_drug_exposure_v6.parquet"
csv_file_path = "tb_drug_exposure.csv"

parquet_file_path = "tb_location.parquet"
csv_file_path = "tb_location.csv"

parquet_file_path = "tb_measurement_data.parquet"
csv_file_path = "tb_measurement_data.csv"

parquet_file_path = "tb_observation_period.parquet"
csv_file_path = "tb_observation_period.csv"


parquet_file_path = "tb_person.parquet"
csv_file_path = "tb_person.csv"


parquet_file_path = "tb_visit_occurrence_data.parquet"
csv_file_path = "tb_visit_occurrence_data.csv"



# 2. Read the Parquet file
print(f"Reading Parquet file: {parquet_file_path}...")
df = pd.read_parquet(parquet_file_path)

# 3. Save to CSV
# index=False prevents pandas from writing the DataFrame index as a column
print(f"Saving to CSV file: {csv_file_path}...")
df.to_csv(csv_file_path, index=False)

print("Conversion complete!")
