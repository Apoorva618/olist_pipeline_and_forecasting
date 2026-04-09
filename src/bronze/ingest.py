"""
Bronze layer ingestion functions.
Loads raw CSV files from Volumes into Delta tables with minimal transformation.
"""

import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def ingest_bronze_table(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    logger=None
) -> DataFrame:
    """
    Load a CSV file from Volume and write to Bronze Delta table.
    """
    
    start_time = time.time()
    source_file = source_path.split("/")[-1]
    
    try:
        # Explicit format to avoid inference issues
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load(source_path)
        
        rows_in = df.count()
        
        # Add metadata columns
        df = df.withColumn("_source_file", F.lit(source_file)) \
               .withColumn("_ingested_at", F.current_timestamp())
        
        # Write to Bronze
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        
        rows_out = spark.table(target_table).count()
        duration = time.time() - start_time
        
        # Log success
        if logger:
            logger.log(
                layer="bronze",
                table_name=target_table.split(".")[-1],
                source=source_file,
                rows_in=rows_in,
                rows_out=rows_out,
                status="SUCCESS",
                duration=duration
            )
        else:
            print(f"+ {target_table} | {rows_out:,} rows | SUCCESS ({duration:.1f}s)")
        
        return spark.table(target_table)
    
    except Exception as e:
        duration = time.time() - start_time
        
        if logger:
            logger.log(
                layer="bronze",
                table_name=target_table.split(".")[-1],
                source=source_file,
                rows_in=0,
                rows_out=0,
                status="FAILED",
                error_msg=str(e),
                duration=duration
            )
        else:
            print(f"X {target_table} | FAILED: {e}")
        raise


def ingest_all_bronze_tables(
    spark: SparkSession,
    source_volume: str,
    target_database: str,
    logger=None
) -> dict:
    """
    Ingest all Olist CSV files from a Volume into Bronze layer.
    """
    
    tables = {
        "olist_customers": "olist_customers_dataset.csv",
        "olist_sellers": "olist_sellers_dataset.csv",
        "olist_geolocation": "olist_geolocation_dataset.csv",
        "olist_orders": "olist_orders_dataset.csv",
        "olist_order_items": "olist_order_items_dataset.csv",
        "olist_order_payments": "olist_order_payments_dataset.csv",
        "olist_order_reviews": "olist_order_reviews_dataset.csv",
        "olist_products": "olist_products_dataset.csv",
        "olist_product_category_name_translation": "product_category_name_translation.csv"
    }
    
    results = {}
    
    for table_name, file_name in tables.items():
        source_path = f"{source_volume}/{file_name}"
        target_table = f"{target_database}.{table_name}"
        
        print(f"\n→ Ingesting {table_name}...")
        
        try:
            df = ingest_bronze_table(spark, source_path, target_table, logger)
            results[table_name] = df
        except Exception as e:
            results[table_name] = None
    
    return results


def get_bronze_table_stats(spark: SparkSession, database: str = "workspace.olist_bronze") -> DataFrame:
    """
    Get row counts and column counts for all Bronze tables.
    """
    
    tables = spark.sql(f"SHOW TABLES IN {database}").collect()
    
    stats = []
    for table in tables:
        table_name = table.tableName
        full_name = f"{database}.{table_name}"
        
        df = spark.table(full_name)
        stats.append({
            "table_name": table_name,
            "row_count": df.count(),
            "column_count": len(df.columns)
        })
    
    return spark.createDataFrame(stats)