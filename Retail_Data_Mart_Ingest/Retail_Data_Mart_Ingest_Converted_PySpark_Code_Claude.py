# ====================================================
# Author:        AAVA
# Date:          
# Description:   Retail Data Mart Ingest Pipeline - Ab Initio to PySpark EMR Glue Conversion
# ====================================================

"""
Retail Data Mart Ingest Pipeline
Converted from Ab Initio .mp file to PySpark EMR Glue

This pipeline processes daily retail transaction data by:
1. Reading raw transactions from AWS S3
2. Reading product dimension data
3. Cleansing and validating transaction data
4. Removing duplicate transactions
5. Enriching with product information via inner join
6. Applying pricing rules and tax calculations
7. Sorting and aggregating by store and date
8. Writing final summary report and error logs
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, lit, floor, expr, to_date
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType

# Import transformation functions from converted XFR module
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, lit, floor, expr
from pyspark.sql.types import DecimalType

# =============================================================================
# SCHEMA DEFINITIONS (Converted from DML files)
# =============================================================================

# Raw_Input.dml - Raw Transaction Data from AWS S3
raw_input_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("txn_date_str", StringType(), True),  # Format expected: YYYY-MM-DD
    StructField("customer_id", StringType(), True),
    StructField("product_sku", StringType(), True),
    StructField("quantity_str", StringType(), True),
    StructField("unit_price_str", StringType(), True),
    StructField("payment_type", StringType(), True)
])

# Product_Dimension.dml - Product dimension table
product_dimension_schema = StructType([
    StructField("product_sku", StringType(), True),   # Key for joining
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("standard_cost", DecimalType(10, 2), True),
    StructField("newline", StringType(), True)
])

# Enriched.dml - Cleaned and Enriched Transaction Data
enriched_schema = StructType([
    StructField("txn_id", DecimalType(10, 0), True),
    StructField("store_id", StringType(), True),
    StructField("txn_date", DateType(), True),
    StructField("product_sku", StringType(), True),
    StructField("category", StringType(), True),
    StructField("total_amount", DecimalType(10, 2), True),
    StructField("standard_cost", DecimalType(10, 2), True),
    StructField("tax_amount", DecimalType(10, 2), True),
    StructField("final_bill", DecimalType(10, 2), True),
    StructField("loyalty_points", DecimalType(5, 0), True)
])

# Summary.dml - Daily Store Aggregation Report
summary_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("report_date", DateType(), True),
    StructField("total_gross_sales", DecimalType(15, 2), True),
    StructField("total_tax_collected", DecimalType(15, 2), True),
    StructField("total_transaction_count", DecimalType(10, 0), True),
    StructField("newline", StringType(), True)
])

# =============================================================================
# TRANSFORMATION FUNCTIONS (Converted from XFR files)
# =============================================================================

# Cleanse_Transform.xfr - Data cleansing and validation
def transform_cleanse_transform(df: DataFrame) -> DataFrame:
    """
    Cleanse and validate raw transaction data
    - Convert string fields to appropriate data types
    - Calculate total_amount from quantity and unit_price
    - Initialize tax and billing fields
    """
    return df.withColumn("txn_id", col("txn_id").cast(DecimalType(10,0))) \
             .withColumn("txn_date", expr("to_date(txn_date_str, 'yyyy-MM-dd')")) \
             .withColumn("total_amount", col("quantity_str").cast(DecimalType(10,2)) * col("unit_price_str").cast(DecimalType(10,2))) \
             .withColumn("tax_amount", lit(0)) \
             .withColumn("final_bill", lit(0)) \
             .withColumn("loyalty_points", lit(0))

# Pricing_Logic.xfr - Apply pricing rules and tax calculations
def transform_pricing_logic(df: DataFrame) -> DataFrame:
    """
    Apply pricing rules, tax calculations, and loyalty points
    - Calculate tax amount based on 8.5% tax rate
    - Calculate final bill (total + tax)
    - Calculate loyalty points (1 point per $10 spent)
    """
    tax_rate = 0.085
    return df.withColumn("tax_amount", col("total_amount") * tax_rate) \
             .withColumn("final_bill", col("total_amount") + (col("total_amount") * tax_rate)) \
             .withColumn("loyalty_points", floor(col("total_amount") / 10).cast(DecimalType(5,0)))

# Rollup_Logic.xfr - Store aggregation logic
def transform_rollup_logic(df: DataFrame) -> DataFrame:
    """
    Aggregate transaction data by store and date
    - Sum total sales, tax collected, and transaction count
    - Group by store_id and transaction date
    """
    return df.groupBy("store_id", "txn_date").agg(
        spark_sum("final_bill").alias("total_gross_sales"),
        spark_sum("tax_amount").alias("total_tax_collected"),
        count("*").alias("total_transaction_count")
    ).withColumn("report_date", col("txn_date")).withColumn("newline", lit("\n"))

# =============================================================================
# MAIN ETL PIPELINE
# =============================================================================

def main():
    # Initialize Spark and Glue contexts
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Configuration parameters (equivalent to Ab Initio parameters)
    AWS_BUCKET_URL = "s3://shopsmart-retail-data/daily_batch/"
    PROJECT_DIR = "/opt/retail_project"
    ERROR_LOG_PATH = f"{PROJECT_DIR}/log/rejects_Retail_Data_Mart_Ingest.log"
    PRODUCT_MISS_PATH = f"{PROJECT_DIR}/data/out/product_misses.dat"
    SUMMARY_OUTPUT_PATH = f"{PROJECT_DIR}/data/out/daily_summary.dat"
    
    print("Starting Retail Data Mart Ingest Pipeline...")
    
    # =============================================================================
    # COMPONENT 1: INPUT FILE (Raw Transactions from AWS S3)
    # =============================================================================
    print("Step 1: Reading raw transaction data from S3...")
    raw_transactions_df = spark.read \
        .option("delimiter", "|") \
        .option("header", "false") \
        .schema(raw_input_schema) \
        .csv(f"{AWS_BUCKET_URL}/transactions_raw.dat")
    
    print(f"Raw transactions count: {raw_transactions_df.count()}")
    
    # =============================================================================
    # COMPONENT 2: INPUT FILE (Product Dimension)
    # =============================================================================
    print("Step 2: Reading product dimension data...")
    product_dim_df = spark.read \
        .option("delimiter", "|") \
        .option("header", "false") \
        .schema(product_dimension_schema) \
        .csv(f"{PROJECT_DIR}/data/dim/product_dim.dat")
    
    print(f"Product dimension count: {product_dim_df.count()}")
    
    # =============================================================================
    # COMPONENT 3: REFORMAT (Cleanse & Validate)
    # =============================================================================
    print("Step 3: Cleansing and validating transaction data...")
    
    # Apply cleansing transformation
    try:
        cleansed_df = transform_cleanse_transform(raw_transactions_df)
        
        # Filter out invalid records (null txn_id, invalid dates, negative amounts)
        valid_df = cleansed_df.filter(
            (col("txn_id").isNotNull()) & 
            (col("txn_date").isNotNull()) & 
            (col("total_amount") > 0)
        )
        
        # Capture reject records
        reject_df = cleansed_df.filter(
            (col("txn_id").isNull()) | 
            (col("txn_date").isNull()) | 
            (col("total_amount") <= 0)
        ).withColumn("error_message", lit("Invalid transaction data"))
        
        print(f"Valid records after cleansing: {valid_df.count()}")
        print(f"Rejected records: {reject_df.count()}")
        
    except Exception as e:
        print(f"Error in cleansing step: {str(e)}")
        raise
    
    # =============================================================================
    # COMPONENT 4: DEDUP SORT (Remove Duplicate Transactions)
    # =============================================================================
    print("Step 4: Removing duplicate transactions...")
    
    # Remove duplicates based on txn_id
    deduped_df = valid_df.dropDuplicates(["txn_id"])
    
    print(f"Records after deduplication: {deduped_df.count()}")
    
    # =============================================================================
    # COMPONENT 5: JOIN (Enrich with Product Info)
    # =============================================================================
    print("Step 5: Enriching with product information...")
    
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
    
    # Capture records that didn't match (unused records)
    unmatched_df = deduped_df.alias("txn").join(
        product_dim_df.alias("prod"),
        col("txn.product_sku") == col("prod.product_sku"),
        "left_anti"
    )
    
    print(f"Records after product enrichment: {enriched_df.count()}")
    print(f"Unmatched product records: {unmatched_df.count()}")
    
    # =============================================================================
    # COMPONENT 6: REFORMAT (Apply Pricing Rules)
    # =============================================================================
    print("Step 6: Applying pricing rules and tax calculations...")
    
    # Apply pricing transformation
    priced_df = transform_pricing_logic(enriched_df)
    
    print(f"Records after pricing calculations: {priced_df.count()}")
    
    # =============================================================================
    # COMPONENT 7: SORT (Prepare for Rollup)
    # =============================================================================
    print("Step 7: Sorting data for aggregation...")
    
    # Sort by store_id and txn_date for efficient rollup
    sorted_df = priced_df.orderBy("store_id", "txn_date")
    
    # =============================================================================
    # COMPONENT 8: ROLLUP (Store Aggregation)
    # =============================================================================
    print("Step 8: Aggregating data by store and date...")
    
    # Apply rollup transformation
    summary_df = transform_rollup_logic(sorted_df)
    
    print(f"Summary records generated: {summary_df.count()}")
    
    # =============================================================================
    # COMPONENT 9: OUTPUT FILE (Final Summary)
    # =============================================================================
    print("Step 9: Writing final summary report...")
    
    # Write summary to output location
    summary_df.coalesce(1).write \
        .mode("overwrite") \
        .option("delimiter", "|") \
        .option("header", "false") \
        .csv(SUMMARY_OUTPUT_PATH)
    
    print(f"Summary report written to: {SUMMARY_OUTPUT_PATH}")
    
    # =============================================================================
    # COMPONENT 10: OUTPUT FILE (Cleanse Rejects)
    # =============================================================================
    print("Step 10: Writing cleanse reject records...")
    
    if reject_df.count() > 0:
        reject_df.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "false") \
            .csv(ERROR_LOG_PATH)
        
        print(f"Reject records written to: {ERROR_LOG_PATH}")
    else:
        print("No reject records to write")
    
    # =============================================================================
    # COMPONENT 11: OUTPUT FILE (Product Lookup Misses)
    # =============================================================================
    print("Step 11: Writing product lookup misses...")
    
    if unmatched_df.count() > 0:
        unmatched_df.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "false") \
            .csv(PRODUCT_MISS_PATH)
        
        print(f"Product misses written to: {PRODUCT_MISS_PATH}")
    else:
        print("No product lookup misses to write")
    
    # =============================================================================
    # PIPELINE COMPLETION
    # =============================================================================
    print("\n=== Pipeline Execution Summary ===")
    print(f"Total raw transactions processed: {raw_transactions_df.count()}")
    print(f"Valid records after cleansing: {valid_df.count()}")
    print(f"Records after deduplication: {deduped_df.count()}")
    print(f"Records after product enrichment: {enriched_df.count()}")
    print(f"Final summary records: {summary_df.count()}")
    print(f"Reject records: {reject_df.count()}")
    print(f"Product lookup misses: {unmatched_df.count()}")
    print("\nRetail Data Mart Ingest Pipeline completed successfully!")
    
    # Commit the job
    job.commit()

if __name__ == "__main__":
    main()