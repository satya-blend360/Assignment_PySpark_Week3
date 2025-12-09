"""
PySpark ETL Pipeline for Amazon Sales Data Analysis
====================================================
This pipeline processes large-scale sales data, performs data cleaning,
and calculates comprehensive KPIs for business insights.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, round as _round, 
    when, lit, month, year, countDistinct, datediff,
    max as _max, min as _min, stddev, percentile_approx, regexp_replace, length, trim
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
import sys

import os
# Initialize Spark Session
# def create_spark_session(app_name="Amazon_Sales_ETL"):
#     """Create and configure Spark session"""
#     spark = SparkSession.builder \
#         .appName(app_name) \
#         .config("spark.sql.adaptive.enabled", "true") \
#         .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
#         .config("spark.sql.shuffle.partitions", "200") \
#         .getOrCreate()
    
#     spark.sparkContext.setLogLevel("WARN")
#     print(f"✓ Spark Session Created: {app_name}")
#     print(f"✓ Spark Version: {spark.version}")
#     return spark

def create_spark_session(app_name="Amazon_Sales_ETL"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "200")
        # Bypass Hadoop FS
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.hadoop.util.Shell", "false")
        .config("spark.hadoop.util.Shell.winutils", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("dfs.client.use.datanode.hostname", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark Session Created: {app_name}")
    print(f"✓ Spark Version: {spark.version}")
    return spark



# 1. DATA INGESTION
def read_sales_data(spark, input_path):
    """Read raw CSV data from HDFS or local filesystem"""
    print("\n" + "="*60)
    print("STEP 1: DATA INGESTION")
    print("="*60)
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    print(f"✓ Data loaded from: {input_path}")
    print(f"✓ Total records: {df.count():,}")
    print(f"✓ Total columns: {len(df.columns)}")
    
    return df

def enforce_numeric_if_exists(df, cols):
    from pyspark.sql.functions import regexp_replace, when, trim, col
    from pyspark.sql.types import DoubleType
    for c in cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                when(trim(col(c)) == "", None)
                .otherwise(regexp_replace(col(c), "[^0-9.-]", ""))
                .cast(DoubleType())
            )
    return df


# 2. DATA QUALITY & CLEANING
def clean_data(df):
    """Transform and clean data - handle missing values, duplicates"""
    print("\n" + "="*60)
    print("STEP 2: DATA CLEANING & TRANSFORMATION")
    print("="*60)
    
    # Check initial data quality
    total_records = df.count()
    print(f"\n📊 Initial Record Count: {total_records:,}")
    
    # Show missing values per column
    print("\n📋 Missing Values Analysis:")
    missing_counts = df.select([
        _sum(when(trim(col(c)) == "", 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    for col_name, missing in missing_counts.items():
        if missing > 0:
            pct = (missing / total_records) * 100
            print(f"   - {col_name}: {missing:,} ({pct:.2f}%)")
    
    print("\n🔧 Cleaning Operations:")
    
    # 1. Remove duplicates
    initial_count = df.count()
    df_clean = df.dropDuplicates(['Order ID'])
    duplicates_removed = initial_count - df_clean.count()
    print(f"   ✓ Removed {duplicates_removed:,} duplicate orders")
    
    # 2. Replace missing values
    df_clean = df_clean.fillna({
        'Amount': 0.0,
        'Qty': 0,
        'Status': 'Unknown',
        'Courier Status': 'Unknown',
        'ship-city': 'Unknown',
        'ship-state': 'Unknown',
        'promotion-ids': 'No Promotion'
    })
    
    print(f"   ✓ Filled missing values")
    
    # 3. Clean numeric fields that already exist
    df_clean = enforce_numeric_if_exists(df_clean, ["Amount", "Qty"])
    
    # 4. Filter out cancelled orders with zero revenue
    df_clean = df_clean.withColumn(
        "Is_Valid_Sale",
        when((col("Status") != "Cancelled") & (col("Amount") > 0), True).otherwise(False)
    )
    
    print(f"   ✓ Data type corrections applied")
    print(f"\n✓ Final cleaned records: {df_clean.count():,}")
    
    return df_clean

# 3. FEATURE ENGINEERING
def enrich_data(df):
    """Add derived columns for analysis"""
    print("\n" + "="*60)
    print("STEP 3: FEATURE ENGINEERING")
    print("="*60)
    
    # Calculate profit margin (assuming 30% cost)
    df = df.withColumn("Cost", col("Amount") * 0.7) \
           .withColumn("Profit", col("Amount") * 0.3) \
           .withColumn("Profit_Margin_Pct", lit(30.0))
    
    # Price per item
    df = df.withColumn(
        "Price_Per_Item",
        when(col("Qty") > 0, col("Amount") / col("Qty")).otherwise(col("Amount"))
    )
    
    # B2B flag conversion
    df = df.withColumn("Is_B2B", when(col("B2B") == "True", 1).otherwise(0))
    
    # Fulfillment type flag
    df = df.withColumn("Is_Amazon_Fulfilled", when(col("fulfilled-by") == "Amazon", 1).otherwise(0))
    
    # Promotion flag
    df = df.withColumn("Has_Promotion", when(col("promotion-ids") != "No Promotion", 1).otherwise(0))
    
    print("✓ Added derived columns:")
    print("   - Cost, Profit, Profit_Margin_Pct")
    print("   - Price_Per_Item")
    print("   - Is_B2B, Is_Amazon_Fulfilled, Has_Promotion")

    # Ensure profit/cost/price fields are proper numbers
    df = enforce_numeric_if_exists(df, ["Profit", "Cost", "Price_Per_Item"])

    # Refresh valid sales check after enrichment
    df = df.withColumn(
        "Is_Valid_Sale",
        when((col("Status") != "Cancelled") & (col("Amount") > 0), True).otherwise(False)
    )
    
    return df


def enforce_numeric(df, columns):
    from pyspark.sql.functions import regexp_replace, when, col
    from pyspark.sql.types import DoubleType, IntegerType
    
    for c in columns:
        df = df.withColumn(
            c,
            when(trim(col(c)) == "", None)
            .otherwise(regexp_replace(col(c), "[^0-9.-]", ""))
            .cast(DoubleType())
        )
    return df


# 4. KPI CALCULATIONS
def calculate_kpis(df):
    """Calculate comprehensive business KPIs"""
    print("\n" + "="*60)
    print("STEP 4: KPI CALCULATIONS")
    print("="*60)
    
    # Filter for valid sales only
    df_valid = df.filter(col("Is_Valid_Sale") == True)
    
    kpis = {}
    
    # KPI 1: Monthly Revenue
    print("\n📈 KPI 1: Monthly Revenue")
    monthly_revenue = df_valid.groupBy("Year", "Month", "MonthName") \
        .agg(
            _round(_sum("Amount"), 2).alias("Total_Revenue"),
            count("Order ID").alias("Order_Count"),
            _sum("Qty").alias("Total_Units_Sold")
        ) \
        .orderBy("Year", "Month")
    
    kpis['monthly_revenue'] = monthly_revenue
    monthly_revenue.show(10)
    
    # KPI 2: Profit Margin Analysis
    print("\n💰 KPI 2: Overall Profit Margin")
    profit_margin = df_valid.agg(
        _round(_sum("Amount"), 2).alias("Total_Revenue"),
        _round(_sum("Profit"), 2).alias("Total_Profit"),
        _round((_sum("Profit") / _sum("Amount")) * 100, 2).alias("Profit_Margin_Pct")
    )
    
    kpis['profit_margin'] = profit_margin
    profit_margin.show()
    
    # KPI 3: Region-wise Sales (State-wise)
    print("\n🗺️  KPI 3: Region-wise Sales (Top 10 States)")
    region_sales = df_valid.groupBy("ship-state") \
        .agg(
            _round(_sum("Amount"), 2).alias("Total_Revenue"),
            count("Order ID").alias("Order_Count"),
            _sum("Qty").alias("Total_Units"),
            _round(avg("Amount"), 2).alias("Avg_Order_Value")
        ) \
        .orderBy(col("Total_Revenue").desc())
    
    kpis['region_sales'] = region_sales
    region_sales.show(10)
    
    # KPI 4: Average Order Value (AOV)
    print("\n💵 KPI 4: Average Order Value")
    aov = df_valid.agg(
        _round(avg("Amount"), 2).alias("Average_Order_Value"),
        _round(_max("Amount"), 2).alias("Max_Order_Value"),
        _round(_min("Amount"), 2).alias("Min_Order_Value"),
        _round(stddev("Amount"), 2).alias("Std_Dev")
    )
    
    kpis['aov'] = aov
    aov.show()
    
    # KPI 5: Category Performance
    print("\n📦 KPI 5: Category Performance")
    category_performance = df_valid.groupBy("Category") \
        .agg(
            _round(_sum("Amount"), 2).alias("Total_Revenue"),
            count("Order ID").alias("Order_Count"),
            _sum("Qty").alias("Total_Units"),
            _round(avg("Amount"), 2).alias("Avg_Order_Value"),
            _round(_sum("Profit"), 2).alias("Total_Profit")
        ) \
        .orderBy(col("Total_Revenue").desc())
    
    kpis['category_performance'] = category_performance
    category_performance.show()
    
    # KPI 6: Fulfillment Performance
    print("\n🚚 KPI 6: Fulfillment Channel Performance")
    fulfillment_perf = df_valid.groupBy("fulfilled-by") \
        .agg(
            _round(_sum("Amount"), 2).alias("Total_Revenue"),
            count("Order ID").alias("Order_Count"),
            _round(avg("Amount"), 2).alias("Avg_Order_Value"),
            _round((count(when(col("Status").contains("Delivered"), True)) / count("*")) * 100, 2).alias("Delivery_Success_Rate")
        ) \
        .orderBy(col("Total_Revenue").desc())
    
    kpis['fulfillment_performance'] = fulfillment_perf
    fulfillment_perf.show()
    
    # KPI 7: B2B vs B2C Performance
    print("\n🏢 KPI 7: B2B vs B2C Sales")
    b2b_analysis = df_valid.groupBy("B2B") \
        .agg(
            _round(_sum("Amount"), 2).alias("Total_Revenue"),
            count("Order ID").alias("Order_Count"),
            _round(avg("Amount"), 2).alias("Avg_Order_Value"),
            _sum("Qty").alias("Total_Units")
        ) \
        .orderBy(col("Total_Revenue").desc())
    
    kpis['b2b_analysis'] = b2b_analysis
    b2b_analysis.show()
    
    # KPI 8: Promotion Impact
    print("\n🎁 KPI 8: Promotion Impact Analysis")
    promotion_impact = df_valid.groupBy("Has_Promotion") \
        .agg(
            _round(_sum("Amount"), 2).alias("Total_Revenue"),
            count("Order ID").alias("Order_Count"),
            _round(avg("Amount"), 2).alias("Avg_Order_Value")
        )
    
    kpis['promotion_impact'] = promotion_impact
    promotion_impact.show()
    
    # KPI 9: Top Products (by ASIN)
    print("\n⭐ KPI 9: Top 10 Products by Revenue")
    top_products = df_valid.groupBy("ASIN", "Category", "Style") \
        .agg(
            _round(_sum("Amount"), 2).alias("Total_Revenue"),
            _sum("Qty").alias("Total_Units_Sold"),
            count("Order ID").alias("Order_Count")
        ) \
        .orderBy(col("Total_Revenue").desc())
    
    kpis['top_products'] = top_products
    top_products.show(10)
    
    # KPI 10: Order Status Distribution
    print("\n📊 KPI 10: Order Status Distribution")
    status_dist = df.groupBy("Status") \
        .agg(
            count("Order ID").alias("Order_Count"),
            _round(_sum("Amount"), 2).alias("Total_Amount"),
            _round((count("*") / df.count()) * 100, 2).alias("Percentage")
        ) \
        .orderBy(col("Order_Count").desc())
    
    kpis['status_distribution'] = status_dist
    status_dist.show()
    
    return kpis

# # 5. WRITE OUTPUTS
# def write_outputs(spark, df, kpis, output_base_path):
#     """Write aggregated results to Parquet format"""
#     print("\n" + "="*60)
#     print("STEP 5: WRITING OUTPUTS")
#     print("="*60)
    
#     # Write cleaned and enriched data
#     enriched_path = f"{output_base_path}/enriched_sales_data"
#     df.write.mode("overwrite").parquet(enriched_path)
#     print(f"✓ Enriched data written to: {enriched_path}")
    
#     # Write each KPI to separate Parquet files
#     for kpi_name, kpi_df in kpis.items():
#         kpi_path = f"{output_base_path}/kpis/{kpi_name}"
#         kpi_df.write.mode("overwrite").parquet(kpi_path)
#         print(f"✓ {kpi_name} written to: {kpi_path}")
    
#     # Create a summary report
#     print("\n📋 Creating Summary Report...")
#     summary_path = f"{output_base_path}/summary_report"
    
#     # Combine key metrics into one summary
#     total_records = df.count()
#     valid_sales = df.filter(col("Is_Valid_Sale") == True).count()
    
#     summary_data = [
#         ("Total_Orders", total_records),
#         ("Valid_Sales", valid_sales),
#         ("Cancellation_Rate_Pct", round((1 - valid_sales/total_records) * 100, 2))
#     ]
    
#     summary_df = spark.createDataFrame(summary_data, ["Metric", "Value"])
#     summary_df.write.mode("overwrite").parquet(summary_path)
#     print(f"✓ Summary report written to: {summary_path}")
    
#     print("\n✅ All outputs written successfully!")
def write_outputs(spark, df, kpis, output_base_path):
    print("\n" + "="*60)
    print("STEP 5: WRITING OUTPUTS")
    print("="*60)

    # Ensure directory exists before Spark writes
    os.makedirs(f"{output_base_path}/enriched_sales_data", exist_ok=True)
    os.makedirs(f"{output_base_path}/kpis", exist_ok=True)
    os.makedirs(f"{output_base_path}/summary_report", exist_ok=True)
    
# 6. MAIN PIPELINE
def run_etl_pipeline(input_path, output_path):
    """Main ETL pipeline orchestration"""
    print("\n" + "="*60)
    print("🚀 PYSPARK ETL PIPELINE - AMAZON SALES ANALYSIS")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Step 1: Read data
        df_raw = read_sales_data(spark, input_path)
        
        # Step 2: Clean data
        df_clean = clean_data(df_raw)
        
        # Step 3: Enrich data
        df_enriched = enrich_data(df_clean)
        
        # Step 4: Calculate KPIs
        kpis = calculate_kpis(df_enriched)
        
        # Step 5: Write outputs
        write_outputs(spark, df_enriched, kpis, output_path)
        
        print("\n" + "="*60)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        return df_enriched, kpis
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        raise
    finally:
        spark.stop()
        print("\n✓ Spark session stopped")


# EXECUTION
if __name__ == "__main__":
    # Configuration
    INPUT_PATH = "Cleaned_Amazon_Sale_Report.csv"  # Change to HDFS path if needed
    OUTPUT_PATH = "./output"  # Change to HDFS path if needed
    
    # Run pipeline
    df_final, kpis_dict = run_etl_pipeline(INPUT_PATH, OUTPUT_PATH)
    
    print("\n" + "="*60)
    print("📦 OUTPUT STRUCTURE:")
    print("="*60)
    print(f"{OUTPUT_PATH}/")
    print("├── enriched_sales_data/")
    print("├── kpis/")
    print("│   ├── monthly_revenue/")
    print("│   ├── profit_margin/")
    print("│   ├── region_sales/")
    print("│   ├── aov/")
    print("│   ├── category_performance/")
    print("│   ├── fulfillment_performance/")
    print("│   ├── b2b_analysis/")
    print("│   ├── promotion_impact/")
    print("│   ├── top_products/")
    print("│   └── status_distribution/")
    print("└── summary_report/")