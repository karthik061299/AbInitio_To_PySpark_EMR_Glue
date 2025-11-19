# ====================================================
# Author:        AAVA
# Date:          
# Description:   Retail Data Mart Ingest Pipeline - Converts Ab Initio ETL logic to PySpark EMR Glue
# ====================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, isnan, isnull, trim, length
from pyspark.sql.types import DecimalType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import sys

# Import transformation functions from converted XFR module
from Retail_Converted_XFR import (
    transform_cleanse_transform,
    transform_pricing_logic,
    transform_rollup_logic
)

# Import schema definitions from converted DML module
from Retail_Converted_DML import (
    raw_input_schema,
    product_dimension_schema,
    enriched_schema,
    summary_schema
)

# Initialize Spark Session and Glue Context
spark = SparkSession.builder \n    .appName("Retail_Data_Mart_Ingest") \n    .config("spark.sql.adaptive.enabled", "true") \n    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \n    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'AWS_BUCKET_URL',
    'PROJECT_DIR',
    'ERROR_LOG_PATH',
    'PRODUCT_MISS_PATH'
])

job.init(args['JOB_NAME'], args)

# Configuration parameters (equivalent to Ab Initio parameters)
AWS_BUCKET_URL = args.get('AWS_BUCKET_URL', 's3://shopsmart-retail-data/daily_batch/')
PROJECT_DIR = args.get('PROJECT_DIR', 's3://shopsmart-retail-data/retail_project')
ERROR_LOG_PATH = args.get('ERROR_LOG_PATH', f'{PROJECT_DIR}/log/rejects_Retail_Data_Mart_Ingest.log')
PRODUCT_MISS_PATH = args.get('PRODUCT_MISS_PATH', f'{PROJECT_DIR}/data/out/product_misses.dat')

print("Starting Retail Data Mart Ingest Pipeline...")

# =============================================================================
# COMPONENT 1: INPUT FILE (Raw Transactions from AWS S3)
# =============================================================================
print("Step 1: Reading raw transaction data from S3...")
raw_transactions_path = f"{AWS_BUCKET_URL}/transactions_raw.dat"

# Read raw transaction data with schema
raw_transactions_df = spark.read \n    .option("delimiter", "|") \n    .option("header", "false") \n    .schema(raw_input_schema) \n    .csv(raw_transactions_path)

print(f"Raw transactions loaded: {raw_transactions_df.count()} records")

# =============================================================================
# COMPONENT 2: INPUT FILE (Product Dimension)
# =============================================================================
print("Step 2: Reading product dimension data...")
product_dim_path = f"{PROJECT_DIR}/data/dim/product_dim.dat"

# Read product dimension data with schema
product_dim_df = spark.read \n    .option("delimiter", "|") \n    .option("header", "false") \n    .schema(product_dimension_schema) \n    .csv(product_dim_path)

print(f"Product dimension loaded: {product_dim_df.count()} records")

# =============================================================================
# COMPONENT 3: REFORMAT (Cleanse & Validate)
# =============================================================================
print("Step 3: Cleansing and validating raw data...")

# Apply cleanse transformation using XFR function
cleansed_df = transform_cleanse_transform(raw_transactions_df)

# Validation logic - identify reject records
reject_condition = (
    isnull(col("txn_id")) |
    isnull(col("store_id")) |
    isnull(col("product_sku")) |
    (length(trim(col("txn_id"))) == 0) |
    (length(trim(col("store_id"))) == 0) |
    (length(trim(col("product_sku"))) == 0) |
    (col("total_amount") <= 0)
)

# Split into clean and reject records
clean_data_df = cleansed_df.filter(~reject_condition)
reject_data_df = raw_transactions_df.filter(reject_condition) \n    .withColumn("error_message", lit("Invalid or missing required fields"))

print(f"Clean records: {clean_data_df.count()}")
print(f"Reject records: {reject_data_df.count()}")

# =============================================================================
# COMPONENT 4: DEDUP SORT (Remove Duplicate Transactions)
# =============================================================================
print("Step 4: Removing duplicate transactions...")

# Remove duplicates based on txn_id (keeping first occurrence)
deduped_df = clean_data_df.dropDuplicates(["txn_id"])

print(f"Records after deduplication: {deduped_df.count()}")

# =============================================================================
# COMPONENT 5: JOIN (Enrich with Product Info)
# =============================================================================
print("Step 5: Enriching data with product information...")

# Inner join with product dimension on product_sku
enriched_df = deduped_df.alias("txn").join(
    product_dim_df.alias("prod"),
    col("txn.product_sku") == col("prod.product_sku"),
    "inner"
).select(
    col("txn.txn_id"),
    col("txn.store_id"),
    col("txn.txn_date"),
    col("txn.product_sku"),
    col("prod.category"),
    col("txn.total_amount"),
    col("prod.standard_cost"),
    col("txn.tax_amount"),
    col("txn.final_bill"),
    col("txn.loyalty_points")
)

# Identify records that didn't match (product lookup misses)
product_misses_df = deduped_df.alias("txn").join(
    product_dim_df.alias("prod"),
    col("txn.product_sku") == col("prod.product_sku"),
    "left_anti"
)

print(f"Enriched records: {enriched_df.count()}")
print(f"Product lookup misses: {product_misses_df.count()}")

# =============================================================================
# COMPONENT 6: REFORMAT (Apply Pricing Rules)
# =============================================================================
print("Step 6: Applying pricing rules...")

# Apply pricing transformation using XFR function
priced_df = transform_pricing_logic(enriched_df)

print(f"Records after pricing: {priced_df.count()}")

# =============================================================================
# COMPONENT 7: SORT (Prepare for Rollup)
# =============================================================================
print("Step 7: Sorting data for rollup aggregation...")

# Sort by store_id and txn_date for efficient rollup processing
sorted_df = priced_df.orderBy("store_id", "txn_date")

print("Data sorted for rollup processing")

# =============================================================================
# COMPONENT 8: ROLLUP (Store Aggregation)
# =============================================================================
print("Step 8: Performing store-level aggregation...")

# Apply rollup transformation using XFR function
summary_df = transform_rollup_logic(sorted_df)

print(f"Summary records generated: {summary_df.count()}")

# =============================================================================
# COMPONENT 9: OUTPUT FILE (Final Summary)
# =============================================================================
print("Step 9: Writing final summary report...")

summary_output_path = f"{PROJECT_DIR}/data/out/daily_summary.dat"

# Write summary data to S3
summary_df.coalesce(1) \n    .write \n    .mode("overwrite") \n    .option("delimiter", "|") \n    .option("header", "false") \n    .csv(summary_output_path)

print(f"Summary report written to: {summary_output_path}")

# =============================================================================
# COMPONENT 10: OUTPUT FILE (Cleanse Rejects)
# =============================================================================
print("Step 10: Writing cleanse reject records...")

if reject_data_df.count() > 0:
    reject_data_df.coalesce(1) \n        .write \n        .mode("overwrite") \n        .option("delimiter", "|") \n        .option("header", "false") \n        .csv(ERROR_LOG_PATH)
    print(f"Reject records written to: {ERROR_LOG_PATH}")
else:
    print("No reject records to write")

# =============================================================================
# COMPONENT 11: OUTPUT FILE (Product Lookup Misses)
# =============================================================================
print("Step 11: Writing product lookup misses...")

if product_misses_df.count() > 0:
    product_misses_df.coalesce(1) \n        .write \n        .mode("overwrite") \n        .option("delimiter", "|") \n        .option("header", "false") \n        .csv(PRODUCT_MISS_PATH)
    print(f"Product misses written to: {PRODUCT_MISS_PATH}")
else:
    print("No product lookup misses to write")

# =============================================================================
# PIPELINE COMPLETION AND CLEANUP
# =============================================================================
print("\n=== PIPELINE EXECUTION SUMMARY ===")
print(f"Raw transactions processed: {raw_transactions_df.count()}")
print(f"Clean records after validation: {clean_data_df.count()}")
print(f"Records after deduplication: {deduped_df.count()}")
print(f"Records enriched with product data: {enriched_df.count()}")
print(f"Final summary records: {summary_df.count()}")
print(f"Reject records: {reject_data_df.count()}")
print(f"Product lookup misses: {product_misses_df.count()}")
print("\nRetail Data Mart Ingest Pipeline completed successfully!")

# Commit the Glue job
job.commit()

# Stop Spark session
spark.stop()