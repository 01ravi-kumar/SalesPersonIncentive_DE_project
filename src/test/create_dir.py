import os

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "data_files2"))

directories = [
    "customer_data_mart",
    "error_files",
    "file_from_s3",
    "mysql_database_mount_point",
    "sales_partition_data",
    "sales_team_data_mart",
    "spark_data"
]

for dir_name in directories:
    os.makedirs(os.path.join(base_dir, dir_name), exist_ok=True)