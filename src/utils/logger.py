"""
Pipeline logging utilities for Olist medallion architecture.
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType


class PipelineLogger:
    
    # Explicit schema to avoid inference issues
    LOG_SCHEMA = StructType([
        StructField("layer", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("source", StringType(), True),
        StructField("rows_in", LongType(), True),
        StructField("rows_out", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("duration_sec", DoubleType(), True),
        StructField("logged_at", TimestampType(), True),
        StructField("logged_by", StringType(), True)
    ])
    
    def __init__(self, spark: SparkSession, log_table: str):
        self.spark = spark
        self.log_table = log_table
        self._ensure_log_table_exists()
    
    def _ensure_log_table_exists(self):
        parts = self.log_table.split(".")
        catalog = parts[0]
        schema = parts[1]
        
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.log_table} (
                layer STRING,
                table_name STRING,
                source STRING,
                rows_in BIGINT,
                rows_out BIGINT,
                status STRING,
                error_message STRING,
                duration_sec DOUBLE,
                logged_at TIMESTAMP,
                logged_by STRING
            ) 
            USING DELTA
        """)
        
        print(f"Log table ready: {self.log_table}")
    
    def log(self, layer, table_name, source, rows_in, rows_out, status, error_msg=None, duration=None):
        try:
            current_user = self.spark.sql("SELECT current_user()").collect()[0][0]
        except:
            current_user = "unknown"
        
        # Use tuple format with explicit schema (not dict)
        log_entry = [(
            layer,
            table_name,
            source,
            int(rows_in),
            int(rows_out),
            status,
            error_msg,
            float(duration) if duration else None,
            datetime.now(),
            current_user
        )]
        
        # Create DataFrame with explicit schema
        log_df = self.spark.createDataFrame(log_entry, schema=self.LOG_SCHEMA)
        log_df.write.format("delta").mode("append").saveAsTable(self.log_table)
        
        icon = "+" if status == "SUCCESS" else "X" if status == "FAILED" else "!"
        duration_str = f" ({duration:.1f}s)" if duration else ""
        print(f"{icon} {layer}.{table_name} | {rows_out:,} rows | {status}{duration_str}")
    
    def get_recent_logs(self, limit=20):
        return self.spark.sql(f"SELECT * FROM {self.log_table} ORDER BY logged_at DESC LIMIT {limit}")