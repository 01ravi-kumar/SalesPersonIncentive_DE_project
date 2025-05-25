import datetime
import shutil
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")))

from resources.dev import config
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import spark_session
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.write.parquet_writer import ParquetWriter
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.sales_incentive_calculations import sales_mart_calculation_table_write
from src.main.delete.local_file_delete import delete_local_file

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import concat_ws, lit, expr


aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider( decrypt(aws_access_key), decrypt(aws_secret_key) )

s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

logger.info("List of Buckets: %s",response['Buckets'])

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]

connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if  csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f'''
                    SELECT DISTINCT file_name 
                    FROM de_db.product_staging_table
                    WHERE file_name in ({str(total_csv_files)[1:-1]}) and status='I' 
                '''
    logger.info(f"dynamically statement created : {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed. Please check")
else:
    logger.info('Last run was successful!!')


try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,config.bucket_name,folder_path)

    logger.info(f'Absolute path on s3 bucket for csv file {s3_absolute_file_path}')

    if not s3_absolute_file_path:
        logger.info(f'No files available at {folder_path}')
        raise Exception("No data available to process")

except Exception as e:
    logger.error(f'Exited with error:- {e}')
    raise e 

prefix = f's3://{config.bucket_name}/'
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]

logging.info(f"File path available on s3 under {config.bucket_name} bucket and folder name is {file_paths}" )
try:
    downloader = S3FileDownloader(s3_client,config.bucket_name, config.local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"File download error: {e}")
    sys.exit()

all_files = os.listdir(config.local_directory)
logger.info(F'List of files presented at my local directory after download {all_files}')

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory,files)))
    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")

else:
    logger.error("there is no data to process")
    raise Exception("there is no data to process")


logger.info("************************************* LISTING THE FILES *************************************")
logger.info(f"List of csv files that needs to be processed {csv_files}")

logger.info("************************************* Creating Spark Session *************************************")
spark = spark_session()
logger.info("Spark session created")

correct_files = []

for data in csv_files:
    data_schema = spark.read.format('csv')\
                        .option('header','true')\
                        .load(data).columns
    
    logger.info(f'Schema for the {data} is {data_schema}')
    logger.info(f'Mandatory columns schema is {config.mandatory_columns}')
    missing_columns = set(config.mandatory_columns) - set(data_schema)

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f'No missing column for the {data}')
        correct_files.append(data)


logger.info(f'************************* List of correct files {correct_files} *************************')
logger.info(f'************************* List of error files {error_files} *************************')
logger.info(f'************************* Moving error data to error directory if any *************************')

error_folder_path_local = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_path_local,file_name)

            shutil.move(file_path,destination_path)

            logger.info(f"moved '{file_name}' from s3 file path to '{destination_path}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix, destination_prefix,file_name)
            logger.info(f'{message}')
        else:
            logger.info(f"'{file_path} does not exist.")
else:
    logger.info(f'******************** There is no error file in our dataset ********************')



logger.info(f'******************** Updating the product_staging_table indicating that process is started ********************')
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"""
                    INSERT INTO {db_name}.{config.product_staging_table}
                    (file_name,file_location,created_date,status)
                    VALUES ('{filename}','{filename}','{formatted_date}','A')
                    """
        insert_statements.append(statement)
    logger.info(f'Insert statement created for staging table --- {insert_statements}')
    logger.info(f'******************** Connecting with MYSQL server ********************')
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info(f'******************** MYSQL server connected successfully ********************')
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info(f'******************** There is no error file in our dataset to process ********************')
    raise Exception("******************** No data available with correct files ********************")

logger.info(f'******************** Fixing extra column coming from source ********************')

schema = StructType([
    StructField('customer_id',IntegerType(),True),
    StructField('store_id',IntegerType(),True),
    StructField('product_name',StringType(),True),
    StructField('sales_date',DateType(),True),
    StructField('sales_person_id',IntegerType(),True),
    StructField('price',FloatType(),True),
    StructField('quantity',IntegerType(),True),
    StructField('total_cost',FloatType(),True),
    StructField('additional_column',StringType(),True)
])

final_df_to_process = spark.createDataFrame([],schema=schema)

for data in correct_files:
    data_df = spark.read.format("csv")\
                    .option('header','true')\
                    .option('inferSchema','true')\
                    .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source is {extra_columns}")

    if extra_columns:
        data_df = data_df.withColumn('additional_column',concat_ws(",",*extra_columns))\
                        .select('customer_id', 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity', 'total_cost', 'additional_column')
        logger.info(f"processed {data} and added 'additional_column'")
    else:
        data_df = data_df.withColumn('additional_column',lit(None))\
                        .select('customer_id', 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity', 'total_cost', 'additional_column')

    final_df_to_process = final_df_to_process.union(data_df)

logger.info(f'******************** Final dataframe from source which will be going to process ********************')
final_df_to_process.show()

database_client = DatabaseReader(config.url, config.properties)

logger.info(f'******************** Loading customer table into customer_table_df ********************')
customer_table_df= database_client.create_dataframe(spark,config.customer_table_name)

logger.info(f'******************** Loading product table into product_table_df ********************')
product_table_df = database_client.create_dataframe(spark,config.product_table)

logger.info(f'******************** Loading staging table into product_staging_table_df ********************')
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)

logger.info(f'******************** Loading sales team table into sales_team_table_df ********************')
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

logger.info(f'******************** Loading store table into store_table_df ********************')
store_table_df = database_client.create_dataframe(spark,config.store_table)

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)


logger.info(f'******************** Final Enriched Data ********************')
s3_customer_store_sales_df_join.show()

logger.info(f'******************** Write the data into the customer data mart ********************')
final_customer_data_mart_df = s3_customer_store_sales_df_join\
                                .select("ct.customer_id","ct.first_name","ct.last_name","ct.address","ct.pincode",'phone_number',"sales_date","total_cost")

logger.info(f'******************** Final Data for customer data mart ********************')
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)
logger.info(f'******************** customer data written to local dist at {config.customer_data_mart_local_file} ********************')


logger.info(f'******************** Data movement from local to s3 form customer data mart ********************')

s3_uploader = UploadToS3(s3_client)
s3_dir = config.s3_customer_datamart_directory

message = s3_uploader.upload_to_s3(s3_dir,
                                   config.bucket_name,
                                   config.customer_data_mart_local_file)

logger.info(f'{message}')

logger.info(f'******************** write the data into sales team data mart ********************')
final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select(
    "store_id","sales_person_id","sales_person_first_name","sales_person_last_name","store_manager_name","manager_id","is_manager","sales_person_address","sales_person_pincode","sales_date","total_cost",expr("SUBSTRING(sales_date,1,7) AS sales_month"))

logger.info(f'******************** Final data for sales team data mart ********************')
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)

logger.info(f'******************** sales team data written to local dist at {config.sales_team_data_mart_local_file} ********************')


logger.info(f'******************** Data movement from local to s3 form sales team data mart ********************')

s3_dir = config.s3_sales_datamart_directory

message = s3_uploader.upload_to_s3(s3_dir,
                                   config.bucket_name,
                                   config.sales_team_data_mart_local_file)

logger.info(f'{message}')

final_sales_team_data_mart_df.write.format('parquet')\
                            .option('header','true')\
                            .mode('overwrite')\
                            .partitionBy('sales_month','store_id')\
                            .option('path',config.sales_team_data_mart_partitioned_local_file)\
                            .save()

s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000

for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)


logger.info(f'******************** Calculate customer every month purchased amount ********************')
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info(f'******************** Calculate of customer mart done and written into the table  ********************')


logger.info(f'******************** Calculate sales every month billed amount ********************')
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info(f'******************** Calculate of sales mart done and written into the table  ********************')

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f'{message}')

logger.info(f'******************** Deleting sales data from local  ********************')
delete_local_file(config.local_directory)
logger.info(f'******************** Deleted sales data from local  ********************')

logger.info(f'******************** Deleting customer data from local  ********************')
delete_local_file(config.customer_data_mart_local_file)
logger.info(f'******************** Deleted sales data from local  ********************')

logger.info(f'******************** Deleting sales team data from local  ********************')
delete_local_file(config.sales_team_data_mart_local_file)
logger.info(f'******************** Deleted sales team data from local  ********************')

logger.info(f'******************** Deleting partitioned sales data from local  ********************')
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info(f'******************** Deleted partitioned sales data from local  ********************')

current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"""
                    UPDATE {db_name}.{config.product_staging_table}
                    SET status = 'I', updated_date = '{formatted_date}'
                    WHERE file_name = '{filename}'
                    """
        update_statements.append(statement)
    logger.info(f'Update statement created for staging table --- {update_statements}')
    logger.info(f'******************** Connecting with MYSQL server ********************')
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info(f'******************** MYSQL server connected successfully ********************')
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info(f'******************** There is an error while changig the status of the activity in {config.product_staging_table}. Please look into it. ********************')
    # raise Exception(f'******************** There is an error while changig the status of the activity in {config.product_staging_table}. Please look into it. ********************')
    sys.exit()

input("Press enter to terminate")