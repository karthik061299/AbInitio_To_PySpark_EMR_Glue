# ====================================================
# Author:        AAVA
# Date:          <leave it blank>
# Description:   Retail Data Mart Ingest Pipeline - Ab Initio to PySpark EMR Glue Conversion
# ====================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, lit, floor, expr
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

# ====================================================
# SCHEMA DEFINITIONS (Converted from DML files)
# ====================================================

# Raw Input Schema
raw_input_schema = StructType([
    StructField("txn_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("txn_date_str", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_sku", StringType(), True),
    StructField("quantity_str", StringType(), True),
    StructField("unit_price_str", StringType(), True),
    StructField("payment_type", StringType(), True)
])

# Product Dimension Schema
product_dimension_schema = StructType([
    StructField("product_sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("standard_cost", DecimalType(10, 2), True),
    StructField("newline", StringType(), True)
])

# Enriched Schema
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

# Summary Schema
summary_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("report_date", DateType(), True),
    StructField("total_gross_sales", DecimalType(15, 2), True),
    StructField("total_tax_collected", DecimalType(15, 2), True),
    StructField("total_transaction_count", DecimalType(10, 0), True),
    StructField("newline", StringType(), True)
])

# ====================================================
# TRANSFORMATION FUNCTIONS (Converted from XFR files)
# ====================================================

# Cleanse Transform Function
def transform_cleanse_transform(df):
    """Cleanse and validate raw transaction data"""
    return df.withColumn("txn_id", col("txn_id").cast(DecimalType(10,0))) \
             .withColumn("txn_date", expr("to_date(txn_date_str, 'yyyy-MM-dd')")) \
             .withColumn("total_amount", col("quantity_str").cast(DecimalType(10,2)) * col("unit_price_str").cast(DecimalType(10,2))) \
             .withColumn("tax_amount", lit(0)) \
             .withColumn("final_bill", lit(0)) \
             .withColumn("loyalty_points", lit(0))

# Pricing Logic Function
def transform_pricing_logic(df):
    """Apply pricing rules and calculate tax, final bill, and loyalty points"""
    tax_rate = 0.085
    return df.withColumn("tax_amount", col("total_amount") * tax_rate) \
             .withColumn("final_bill", col("total_amount") + (col("total_amount") * tax_rate)) \
             .withColumn("loyalty_points", floor(col("total_amount") / 10).cast(DecimalType(5,0)))

# Rollup Logic Function
def transform_rollup_logic(df):
    """Aggregate transactions by store and date"""
    return df.groupBy("store_id", "txn_date").agg(
        spark_sum("final_bill").alias("total_gross_sales"),
        spark_sum("tax_amount").alias("total_tax_collected"),
        count("*").alias("total_transaction_count")
    ).withColumn("report_date", col("txn_date")).withColumn("newline", lit("\n"))

# ====================================================
# MAIN ETL PIPELINE
# ====================================================

def main():
    # Initialize Spark and Glue contexts
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    spark = SparkSession.builder.appName("Retail_Data_Mart_Ingest").getOrCreate()
    glueContext = GlueContext(spark)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Configuration parameters
    aws_bucket_url = "s3://shopsmart-retail-data/daily_batch/"
    project_dir = "/retail_project"
    error_log_path = f"{project_dir}/log/rejects_Retail_Data_Mart_Ingest.log"
    product_miss_path = f"{project_dir}/data/out/product_misses.dat"
    
    try:
        # ====================================================
        # COMPONENT 1: READ AWS S3 (Raw Transactions)
        # ====================================================
        print("Step 1: Reading raw transaction data from S3...")
        raw_transactions_df = spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(raw_input_schema) \
            .csv(f"{aws_bucket_url}/transactions_raw.dat")
        
        print(f"Raw transactions count: {raw_transactions_df.count()}")
        
        # ====================================================
        # COMPONENT 2: READ PRODUCT DIMENSION
        # ====================================================
        print("Step 2: Reading product dimension data...")
        product_dim_df = spark.read \
            .option("delimiter", "|") \
            .option("header", "false") \
            .schema(product_dimension_schema) \
            .csv(f"{project_dir}/data/dim/product_dim.dat")
        
        print(f"Product dimension count: {product_dim_df.count()}")
        
        # ====================================================
        # COMPONENT 3: CLEANSE DATA (Reformat with validation)
        # ====================================================
        print("Step 3: Cleansing and validating transaction data...")
        
        # Apply cleanse transformation
        cleansed_df = transform_cleanse_transform(raw_transactions_df)
        
        # Filter valid records (non-null txn_id and txn_date)
        valid_records = cleansed_df.filter(
            col("txn_id").isNotNull() & 
            col("txn_date").isNotNull() & 
            col("total_amount").isNotNull() &
            (col("total_amount") > 0)
        )
        
        # Filter invalid records for rejection
        invalid_records = cleansed_df.filter(
            col("txn_id").isNull() | 
            col("txn_date").isNull() | 
            col("total_amount").isNull() |
            (col("total_amount") <= 0)
        ).withColumn("error_message", lit("Invalid transaction data"))
        
        print(f"Valid records count: {valid_records.count()}")
        print(f"Invalid records count: {invalid_records.count()}")
        
        # ====================================================
        # COMPONENT 4: DEDUP SORT (Remove Duplicate Transactions)
        # ====================================================
        print("Step 4: Removing duplicate transactions...")
        
        # Remove duplicates based on txn_id
        deduped_df = valid_records.dropDuplicates(["txn_id"])
        
        print(f"Deduplicated records count: {deduped_df.count()}")
        
        # ====================================================
        # COMPONENT 5: ENRICHMENT JOIN (Inner join with Product Dimension)
        # ====================================================
        print("Step 5: Enriching transactions with product information...")
        
        # Inner join on product_sku
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
        
        # Identify unmatched records (product lookup misses)
        unmatched_records = deduped_df.alias("txn").join(
            product_dim_df.alias("prod"),
            col("txn.product_sku") == col("prod.product_sku"),
            "left_anti"
        )
        
        print(f"Enriched records count: {enriched_df.count()}")
        print(f"Unmatched records count: {unmatched_records.count()}")
        
        # ====================================================
        # COMPONENT 6: APPLY PRICING (Reformat with pricing rules)
        # ====================================================
        print("Step 6: Applying pricing rules...")
        
        # Apply pricing transformation
        priced_df = transform_pricing_logic(enriched_df)
        
        print(f"Priced records count: {priced_df.count()}")
        
        # ====================================================
        # COMPONENT 7: SORT FOR ROLLUP (Prepare for aggregation)
        # ====================================================
        print("Step 7: Sorting data for rollup...")
        
        # Sort by store_id and txn_date
        sorted_df = priced_df.orderBy("store_id", "txn_date")
        
        # ====================================================
        # COMPONENT 8: STORE AGGREGATION (Rollup by store and date)
        # ====================================================
        print("Step 8: Performing store aggregation...")
        
        # Apply rollup transformation
        aggregated_df = transform_rollup_logic(sorted_df)
        
        print(f"Aggregated records count: {aggregated_df.count()}")
        
        # ====================================================
        # COMPONENT 9: WRITE SUMMARY (Final output)
        # ====================================================
        print("Step 9: Writing final summary report...")
        
        # Write aggregated results to output
        aggregated_df.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "false") \
            .csv(f"{project_dir}/data/out/daily_summary.dat")
        
        # ====================================================
        # COMPONENT 10: WRITE CLEANSE REJECTS (Error handling)
        # ====================================================
        print("Step 10: Writing cleanse rejects...")
        
        if invalid_records.count() > 0:
            invalid_records.coalesce(1).write \
                .mode("overwrite") \
                .option("delimiter", "|") \
                .option("header", "false") \
                .csv(error_log_path)
        
        # ====================================================
        # COMPONENT 11: WRITE PRODUCT MISSES (Error handling)
        # ====================================================
        print("Step 11: Writing product lookup misses...")
        
        if unmatched_records.count() > 0:
            unmatched_records.coalesce(1).write \
                .mode("overwrite") \
                .option("delimiter", "|") \
                .option("header", "false") \
                .csv(product_miss_path)
        
        print("Pipeline completed successfully!")
        
        # Show sample results
        print("\n=== SAMPLE FINAL RESULTS ===")
        aggregated_df.show(10, truncate=False)
        
    except Exception as e:
        print(f"Error in pipeline execution: {str(e)}")
        raise e
    
    finally:
        job.commit()
        spark.stop()

if __name__ == "__main__":
    main()