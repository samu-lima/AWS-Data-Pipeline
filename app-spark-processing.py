from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Setup Spark Aplication
spark = SparkSession \
        .builder \
        .appName('job-1-spark') \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')\
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')\
        .getOrCreate()

# Defining logging method to use only INFO for DEV [INFO , ERROR]
spark.sparkContext.setLogLevel('ERROR')

def read_csv(bucket, path):
    # Reading data from datalake
    df = spark.read.format('csv')\
        .option('header', 'True')\
        .csv('inferSchema', 'True')\
    
    # Print data from raw
    print('\nPrint data read from raw')
    print(df.show(5))

    # Print dataframe schema
    print('\nPrint dataframe schema read from raw')
    print(df.printSchema())
    return df

def read_delta(bucket, path):
    df = spark.read.format('delta')\
        .load(f'{bucket}/{path}')
    return df

def write_processed(bucket, path, data_format, mode):
    print('\nWriting data from raw to delta on processed zone..')
    try:
        df.write.format(data_format)\
            .mode(mode)\
            .option('mergeSchema','True')\
            .save(f'{bucket}/{path}')
        print(f'Data successfully written on processed!')
        return 0
    except Exception as err:
        print(f'Fail to write data on processed: {err}')
        return 1

def write_processed_partitioned(bucket, path, col_partition, data_format, mode):
    print('\nWriting data from raw to delta on processed zone ..')
    try:
        df.write.format(data_format)\
            .partitonBy(col_partition)\
            .mode(mode)\
            .option('mergeSchema','True')\
            .save(f'{bucket}/{path}')
        print(f'Data successfully written on processed!')
        return 0
    except Exception as err:
        print(f'Fail to write data on processed: {err}')
        return 1

def write_curated(bucket, path, dataframe, data_format, mode):
    # Convert processed data to parquet and write on curated zone
    print('\nWriting data on curated zone..')
    try:
        dataframe.write.format(data_format)\
            .mode(mode)\
            .save(f'{bucket}/{path}')
        print(f'Data successfully written on curated zone!')
        return 0 
    except Exception as err:
        print(f'Fail to write data on curated zone: {err}')
        return 1
    
bucket_raw = 'raw-pipelineproject-606275137584'
bucket_processed = 'processed2-pipelineproject-606275137584'
bucket_curated = 'curated2-pipelineproject-606275137584'

print('Data from customers..\n')
# Read data from raw
df = read_csv(bucket_raw, 'public/customers/')

# Process data and write on processed zone
write_processed(bucket_processed, 'customers', 'delta', 'overwrite')

# Read data from processed zone..
print("Customer Data read from processed on Delta\n")
df = read_delta(bucket_processed, 'customers')

# Print info from database
print (df.show())

print ("Product data...\n")

# Read da from raw
df = read_csv(bucket_raw,'public/products/')

# Process data and write on processed zone
write_processed(bucket_processed,"products","delta","overwrite")

# Read data from processed zone..
print ("Data read from processed on Delta..\n")
df = read_delta(bucket_processed,"products")

# Print info from database
print (df.show())

print ("Order Data...\n")

# Read da from raw
df = read_csv(bucket_raw,'public/orders/')

# Process data and write on processed zone
write_processed(bucket_processed,"orders","delta","overwrite")

# Read data from processed zone..
print ("Data read from processed on Delta..\n")
df = read_delta(bucket_processed,"orders")

# Print info from database
print (df.show())

# stop aplication
spark.stop()
