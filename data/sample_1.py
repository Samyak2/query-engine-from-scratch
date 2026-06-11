import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

# Define the number of rows to generate
num_rows = 10_000

# Initialize Faker for synthetic data generation
fake = Faker()

# Static list of countries to sample from
countries = ["Italy", "India", "Japan", "France", "Germany"]

# Generate the data using Faker
id_data = list(range(1, num_rows + 1))
name_data = [fake.name() for _ in range(num_rows)]
age_data = [fake.random_int(min=18, max=90) for _ in range(num_rows)]
country_data = [fake.random_element(countries) for _ in range(num_rows)]

# Convert the Python lists to PyArrow arrays
# Explicitly declaring the integer types as int32 keeps the Parquet file size optimized
id_array = pa.array(id_data, type=pa.int32())
name_array = pa.array(name_data, type=pa.string())
age_array = pa.array(age_data, type=pa.int32())
country_array = pa.array(country_data, type=pa.string())

# Construct the PyArrow Table
table = pa.Table.from_arrays(
    [id_array, name_array, age_array, country_array],
    names=["id", "name", "age", "country"],
)

# Write the table to a Parquet file
output_filename = "data/sample_1.parquet"
pq.write_table(table, output_filename)

print(f"Successfully wrote {num_rows} rows to '{output_filename}'!")
