from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

# Define the number of rows to generate
num_rows = 10_000

# Match sample_1.py's generated user ID range
num_users = 10_000

# Initialize Faker for synthetic data generation
fake = Faker()

# Static list of items to sample from
items = [
    "PS5",
    "Xbox",
    "PC",
    "Nintendo Switch",
    "Steam Deck",
    "Monitor",
    "Keyboard",
    "Mouse",
    "Headphones",
    "Microphone",
    "Webcam",
    "Laptop Stand",
    "USB-C Hub",
    "External SSD",
    "Graphics Card",
    "Gaming Chair",
    "Desk Mat",
    "Controller",
    "VR Headset",
    "Router",
    "Smartphone",
    "Tablet",
    "Smartwatch",
    "Bluetooth Speaker",
    "E-reader",
]

start_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
end_time = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

# Generate the data using Faker
id_data = list(range(1, num_rows + 1))
user_id_data = [fake.random_int(min=1, max=num_users) for _ in range(num_rows)]
item_data = [fake.random_element(items) for _ in range(num_rows)]
time_data = [
    fake.date_time_between_dates(
        datetime_start=start_time,
        datetime_end=end_time,
        tzinfo=timezone.utc,
    )
    .isoformat()
    .replace("+00:00", "Z")
    for _ in range(num_rows)
]

# Convert the Python lists to PyArrow arrays
# Explicitly declaring the integer types as int32 keeps the Parquet file size optimized
id_array = pa.array(id_data, type=pa.int32())
user_id_array = pa.array(user_id_data, type=pa.int32())
item_array = pa.array(item_data, type=pa.string())
time_array = pa.array(time_data, type=pa.string())

# Construct the PyArrow Table
table = pa.Table.from_arrays(
    [id_array, user_id_array, item_array, time_array],
    names=["id", "user_id", "item", "time"],
)

# Write the table to a Parquet file
output_filename = "data/orders.parquet"
pq.write_table(table, output_filename)

print(f"Successfully wrote {num_rows} rows to '{output_filename}'!")
