# ====================================================
# Author:        AAVA
# Date:          
# Description:   Retail Data Mart Ingest Pipeline - Ab Initio to PySpark EMR Glue Conversion
# ====================================================

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, lit, floor, expr, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

# Import transformation functions from converted XFR module
from Retail_Converted_XFR import (
    transform_rollup_logic,
    transform_pricing_logic,
    transform_cleanse_transform
)

# Import schema definitions from converted DML module
from Retail_Converted_DML import (
    raw_input_schema,
    summary_schema,
    enriched_schema,
    product_dimension_schema
)

# Initialize Glue context and Spark session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration parameters (equivalent to Ab Initio parameters)
AWS_BUCKET_URL = "s3://shopsmart-retail-data/daily_batch/"
PROJECT_DIR = "s3://shopsmart-retail-data/retail_project"
ERROR_LOG_PATH = f"{PROJECT_DIR}/log/rejects_retail_data_mart_ingest.log"
PRODUCT_MISS_PATH = f"{PROJECT_DIR}/data/out/product_misses.dat"
OUTPUT_SUMMARY_PATH = f"{PROJECT_DIR}/data/out/daily_summary.dat"

def main():
    """
    Main ETL pipeline following the exact Ab Initio flow sequence:
    1. Read AWS S3 Raw Transactions
    2. Read Product Dimension
    3. Cleanse Data
    4. Dedup Transactions
    5. Enrichment Join
    6. Apply Pricing
    7. Sort for Rollup
    8. Store Aggregation
    9. Write Summary
    10. Handle Rejects and Misses
    """
    
    try:
        # =========================================================================
        # COMPONENT 1: INPUT FILE (Raw Transactions from AWS S3)
        # =========================================================================
        print("Step 1: Reading raw transactions from S3...")
        raw_transactions_df = spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(raw_input_schema) \
            .csv(f"{AWS_BUCKET_URL}/transactions_raw.dat")
        
        print(f"Raw transactions count: {raw_transactions_df.count()}")
        
        # =========================================================================
        # COMPONENT 2: INPUT FILE (Product Dimension)
        # =========================================================================
        print("Step 2: Reading product dimension data...")
        product_dim_df = spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(product_dimension_schema) \
            .csv(f"{PROJECT_DIR}/data/dim/product_dim.dat")
        
        print(f"Product dimension count: {product_dim_df.count()}")
        
        # =========================================================================
        # COMPONENT 3: REFORMAT (Cleanse & Validate)
        # =========================================================================
        print("Step 3: Cleansing and validating data...")
        
        # Apply cleanse transformation using XFR function
        cleansed_df = transform_cleanse_transform(raw_transactions_df)
        
        # Separate good records from rejects based on validation rules
        good_records_df = cleansed_df.filter(
            col("txn_id").isNotNull() & 
            (col("txn_id") > 0) &
            col("store_id").isNotNull() &
            col("txn_date").isNotNull() &
            col("product_sku").isNotNull() &
            (col("total_amount") > 0)
        )
        
        # Reject records with validation errors
        reject_records_df = cleansed_df.filter(
            col("txn_id").isNull() | 
            (col("txn_id") <= 0) |
            col("store_id").isNull() |
            col("txn_date").isNull() |
            col("product_sku").isNull() |
            (col("total_amount") <= 0)
        ).withColumn("error_message", lit("Validation failed: Invalid or missing required fields"))
        
        print(f"Good records count: {good_records_df.count()}")
        print(f"Reject records count: {reject_records_df.count()}")
        
        # =========================================================================
        # COMPONENT 4: DEDUP SORT (Remove Duplicate Transactions)
        # =========================================================================
        print("Step 4: Removing duplicate transactions...")
        
        # Remove duplicates based on txn_id (keeping first occurrence)
        deduped_df = good_records_df.dropDuplicates(["txn_id"])
        
        # Calculate duplicate records for logging
        duplicate_count = good_records_df.count() - deduped_df.count()
        print(f"Deduplicated records count: {deduped_df.count()}")
        print(f"Duplicate records removed: {duplicate_count}")
        
        # =========================================================================
        # COMPONENT 5: JOIN (Enrich with Product Info)
        # =========================================================================
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
        
        # Capture records that didn't match (product lookup misses)
        product_misses_df = deduped_df.alias("txn").join(
            product_dim_df.alias("prod"),
            col("txn.product_sku") == col("prod.product_sku"),
            "left_anti"
        )
        
        print(f"Enriched records count: {enriched_df.count()}")
        print(f"Product lookup misses count: {product_misses_df.count()}")
        
        # =========================================================================
        # COMPONENT 6: REFORMAT (Apply Pricing Rules)
        # =========================================================================
        print("Step 6: Applying pricing rules and calculations...")
        
        # Apply pricing transformation using XFR function
        priced_df = transform_pricing_logic(enriched_df)
        
        print(f"Priced records count: {priced_df.count()}")
        
        # =========================================================================
        # COMPONENT 7: SORT (Prepare for Rollup)
        # =========================================================================
        print("Step 7: Sorting data for rollup aggregation...")
        
        # Sort by store_id and txn_date for efficient rollup processing
        sorted_df = priced_df.orderBy("store_id", "txn_date")
        
        print(f"Sorted records count: {sorted_df.count()}")
        
        # =========================================================================
        # COMPONENT 8: ROLLUP (Store Aggregation)
        # =========================================================================
        print("Step 8: Performing store-level aggregation...")
        
        # Apply rollup transformation using XFR function
        summary_df = transform_rollup_logic(sorted_df)
        
        print(f"Summary records count: {summary_df.count()}")
        
        # =========================================================================
        # COMPONENT 9: OUTPUT FILE (Final Summary)
        # =========================================================================
        print("Step 9: Writing final summary report...")
        
        # Write summary to output location
        summary_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "false") \
            .csv(OUTPUT_SUMMARY_PATH)
        
        print(f"Summary written to: {OUTPUT_SUMMARY_PATH}")
        
        # =========================================================================
        # COMPONENT 10: OUTPUT FILE (Cleanse Rejects)
        # =========================================================================
        print("Step 10: Writing cleanse reject records...")
        
        if reject_records_df.count() > 0:
            reject_records_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("delimiter", "|") \
                .option("header", "false") \
                .csv(ERROR_LOG_PATH)
            
            print(f"Reject records written to: {ERROR_LOG_PATH}")
        else:
            print("No reject records to write")
        
        # =========================================================================
        # COMPONENT 11: OUTPUT FILE (Product Lookup Misses)
        # =========================================================================
        print("Step 11: Writing product lookup misses...")
        
        if product_misses_df.count() > 0:
            product_misses_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("delimiter", "|") \
                .option("header", "false") \
                .csv(PRODUCT_MISS_PATH)
            
            print(f"Product misses written to: {PRODUCT_MISS_PATH}")
        else:
            print("No product lookup misses to write")
        
        # =========================================================================
        # JOB COMPLETION SUMMARY
        # =========================================================================
        print("\n=== ETL JOB COMPLETION SUMMARY ===")
        print(f"Raw transactions processed: {raw_transactions_df.count()}")
        print(f"Good records after cleansing: {good_records_df.count()}")
        print(f"Records after deduplication: {deduped_df.count()}")
        print(f"Records enriched with product info: {enriched_df.count()}")
        print(f"Final summary records generated: {summary_df.count()}")
        print(f"Reject records: {reject_records_df.count()}")
        print(f"Product lookup misses: {product_misses_df.count()}")
        print("ETL pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error in ETL pipeline: {str(e)}")
        raise e
    
    finally:
        # Commit the Glue job
        job.commit()

if __name__ == "__main__":
    main()

# =========================================================================
# ADDITIONAL UTILITY FUNCTIONS FOR MONITORING AND DEBUGGING
# =========================================================================

def validate_data_quality(df: DataFrame, stage_name: str) -> None:
    """
    Utility function to validate data quality at each stage
    """
    print(f"\n=== Data Quality Check: {stage_name} ===")
    print(f"Record count: {df.count()}")
    print(f"Null values per column:")
    
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        print(f"  {column}: {null_count}")

def log_processing_metrics(df: DataFrame, stage_name: str) -> None:
    """
    Utility function to log processing metrics
    """
    print(f"\n=== Processing Metrics: {stage_name} ===")
    print(f"Record count: {df.count()}")
    print(f"Partition count: {df.rdd.getNumPartitions()}")
    
    # Sample data preview
    print("Sample data (first 5 rows):")
    df.show(5, truncate=False)

# =========================================================================
# ERROR HANDLING AND RECOVERY FUNCTIONS
# =========================================================================

def handle_processing_errors(df: DataFrame, error_path: str) -> DataFrame:
    """
    Generic error handling function for processing stages
    """
    try:
        # Validate DataFrame is not empty
        if df.count() == 0:
            print("Warning: Empty DataFrame detected")
            return df
        
        # Check for critical columns
        required_columns = ["txn_id", "store_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        return df
        
    except Exception as e:
        print(f"Error in processing: {str(e)}")
        # Log error details to S3 for debugging
        error_df = spark.createDataFrame([(str(e),)], ["error_message"])
        error_df.write.mode("append").csv(error_path)
        raise e

# =========================================================================
# PERFORMANCE OPTIMIZATION FUNCTIONS
# =========================================================================

def optimize_dataframe_partitioning(df: DataFrame, partition_column: str = None) -> DataFrame:
    """
    Optimize DataFrame partitioning for better performance
    """
    if partition_column and partition_column in df.columns:
        # Repartition by specified column for better join performance
        return df.repartition(col(partition_column))
    else:
        # Default repartitioning based on cluster size
        optimal_partitions = spark.sparkContext.defaultParallelism * 2
        return df.repartition(optimal_partitions)

def cache_intermediate_results(df: DataFrame, cache_level: str = "MEMORY_AND_DISK") -> DataFrame:
    """
    Cache intermediate results for reuse
    """
    from pyspark import StorageLevel
    
    if cache_level == "MEMORY_ONLY":
        return df.cache()
    elif cache_level == "DISK_ONLY":
        return df.persist(StorageLevel.DISK_ONLY)
    else:
        return df.persist(StorageLevel.MEMORY_AND_DISK)

# =========================================================================
# DATA LINEAGE AND AUDIT FUNCTIONS
# =========================================================================

def log_data_lineage(source_path: str, target_path: str, transformation: str, record_count: int) -> None:
    """
    Log data lineage information for audit purposes
    """
    lineage_info = {
        "source_path": source_path,
        "target_path": target_path,
        "transformation": transformation,
        "record_count": record_count,
        "processing_timestamp": spark.sql("SELECT current_timestamp()").collect()[0][0]
    }
    
    print(f"Data Lineage: {lineage_info}")
    
    # Optionally write to audit table or log file
    lineage_df = spark.createDataFrame([lineage_info])
    lineage_df.write.mode("append").parquet(f"{PROJECT_DIR}/audit/data_lineage")

# =========================================================================
# CONFIGURATION AND ENVIRONMENT SETUP
# =========================================================================

def setup_spark_configuration():
    """
    Configure Spark settings for optimal performance
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    print("Spark configuration optimized for EMR Glue execution")

# Initialize optimized Spark configuration
setup_spark_configuration()

# =========================================================================
# END OF CONVERTED PYSPARK EMR GLUE PIPELINE
# =========================================================================