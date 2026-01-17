import os
import json
import logging
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import from_json, col, count, avg, sum as spark_sum, current_timestamp, row_number, desc, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global Spark session variable
spark = None

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'shipments')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_DB = os.getenv('MONGO_DB', 'supplychain')
MONGO_USER = os.getenv('MONGO_USER', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'supplychain')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')

# MongoDB connection URI
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"

# PostgreSQL connection
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Define schema for incoming Kafka messages
def get_shipment_schema():
    """Define the schema for shipment events from Kafka"""
    return StructType([
        StructField("shipment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("shipping_date", StringType(), True),
        StructField("delivery_date", StringType(), True),
        
        # Product information
        StructField("product_name", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("product_price", DoubleType(), True),
        StructField("order_item_quantity", IntegerType(), True),
        
        # Customer information
        StructField("customer_fname", StringType(), True),
        StructField("customer_lname", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("customer_country", StringType(), True),
        
        # Shipping information
        StructField("shipping_mode", StringType(), True),
        StructField("order_city", StringType(), True),
        StructField("order_state", StringType(), True),
        StructField("order_country", StringType(), True),
        StructField("days_for_shipment_scheduled", IntegerType(), True),
        StructField("days_for_shipping_real", IntegerType(), True),
        
        # Status and performance
        StructField("delivery_status", StringType(), True),
        StructField("late_delivery_risk", IntegerType(), True),
        
        # Financial
        StructField("sales", DoubleType(), True),
        StructField("order_item_discount", DoubleType(), True),
        StructField("order_item_profit_ratio", DoubleType(), True),
        
        # Metadata
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True)
    ])

def create_spark_session():
    """Create and configure Spark session"""
    global spark
    logger.info("🚀 Creating Spark Session...")
    
    spark = SparkSession.builder \
        .appName("SupplyChainProcessor") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ Spark Session created successfully")
    logger.info(f"📊 MongoDB URI: {MONGO_URI}")
    logger.info(f"📊 PostgreSQL URL: {POSTGRES_URL}")
    return spark

def read_from_kafka(spark):
    """Read streaming data from Kafka"""
    logger.info(f"📡 Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📥 Subscribing to topic: {KAFKA_TOPIC}")
    
    # Read from Kafka with resilience configs
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("kafka.session.timeout.ms", "30000") \
        .option("kafka.request.timeout.ms", "40000") \
        .load()
    
    logger.info("✅ Connected to Kafka successfully")
    
    # Parse JSON from Kafka value
    schema = get_shipment_schema()
    
    parsed_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("kafka_key", "raw_value", "topic", "partition", "offset", "kafka_timestamp", "data.*")
    
    logger.info("✅ Kafka messages parsed successfully")
    return parsed_df

# ========== MongoDB Write Functions ==========

def write_raw_events_to_mongo(batch_df, batch_id):
    """Write raw events to MongoDB raw_events collection"""
    try:
        logger.info(f"📝 Writing batch {batch_id} - Raw events to MongoDB...")
        
        # Select raw data for archiving
        raw_events = batch_df.select(
            "raw_value",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp"
        ).withColumn("processed_at", current_timestamp())
        
        # Write to MongoDB
        raw_events.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", MONGO_DB) \
            .option("collection", "raw_events") \
            .save()
        
        logger.info(f"✅ Batch {batch_id} - Raw events written to MongoDB")
    except Exception as e:
        logger.error(f"❌ Error writing raw events batch {batch_id}: {e}")

def write_shipments_to_mongo(batch_df, batch_id):
    """Write structured shipment data to MongoDB shipments collection"""
    try:
        logger.info(f"📝 Writing batch {batch_id} - Shipments to MongoDB...")
        
        # Select all shipment fields
        shipments = batch_df.select(
            "shipment_id",
            "order_id",
            "order_date",
            "shipping_date",
            "delivery_date",
            "product_name",
            "category_name",
            "product_price",
            "order_item_quantity",
            "customer_fname",
            "customer_lname",
            "customer_city",
            "customer_state",
            "customer_country",
            "shipping_mode",
            "order_city",
            "order_state",
            "order_country",
            "days_for_shipment_scheduled",
            "days_for_shipping_real",
            "delivery_status",
            "late_delivery_risk",
            "sales",
            "order_item_discount",
            "order_item_profit_ratio",
            "timestamp",
            "event_type"
        ).withColumn("inserted_at", current_timestamp())
        
        # Write to MongoDB
        shipments.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", MONGO_DB) \
            .option("collection", "shipments") \
            .save()
        
        count_val = shipments.count()
        logger.info(f"✅ Batch {batch_id} - {count_val} shipments written to MongoDB")
    except Exception as e:
        logger.error(f"❌ Error writing shipments batch {batch_id}: {e}")

def write_aggregations_to_mongo(batch_df, batch_id):
    """Write basic aggregations to MongoDB"""
    try:
        logger.info(f"📊 Computing aggregations for batch {batch_id}...")
        
        # Aggregation 1: Count by carrier (shipping_mode)
        carrier_agg = batch_df.groupBy("shipping_mode") \
            .agg(
                count("*").alias("shipment_count"),
                avg("days_for_shipping_real").alias("avg_delivery_days"),
                spark_sum("sales").alias("total_sales")
            ).withColumn("updated_at", current_timestamp())
        
        carrier_agg.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", MONGO_DB) \
            .option("collection", "carrier_aggregations") \
            .save()
        
        logger.info(f"✅ Batch {batch_id} - Carrier aggregations written")
        
        # Aggregation 2: Count by category
        category_agg = batch_df.groupBy("category_name") \
            .agg(
                count("*").alias("shipment_count"),
                spark_sum("sales").alias("total_sales"),
                avg("order_item_quantity").alias("avg_quantity")
            ).withColumn("updated_at", current_timestamp())
        
        category_agg.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", MONGO_DB) \
            .option("collection", "category_aggregations") \
            .save()
        
        logger.info(f"✅ Batch {batch_id} - Category aggregations written")
        
    except Exception as e:
        logger.error(f"❌ Error writing aggregations batch {batch_id}: {e}")

# ========== PostgreSQL Write Functions ==========

def write_to_postgres(df, table_name, mode="append"):
    """Helper function to write DataFrame to PostgreSQL"""
    try:
        df.write \
            .jdbc(
                url=POSTGRES_URL,
                table=table_name,
                mode=mode,
                properties=POSTGRES_PROPERTIES
            )
        logger.info(f"✅ Written to PostgreSQL table: {table_name}")
    except Exception as e:
        logger.error(f"❌ Error writing to PostgreSQL table {table_name}: {e}")

def write_carrier_performance_to_postgres(batch_df, batch_id):
    """Write carrier performance metrics to PostgreSQL"""
    try:
        logger.info(f"📊 Computing carrier performance for batch {batch_id}...")
        
        carrier_stats = batch_df.groupBy("shipping_mode") \
            .agg(
                count("*").alias("total_shipments"),
                avg("days_for_shipping_real").alias("avg_delivery_days"),
                (spark_sum(col("late_delivery_risk")) / count("*") * 100).alias("on_time_rate"),
                spark_sum("late_delivery_risk").alias("delayed_shipments")
            ) \
            .withColumnRenamed("shipping_mode", "carrier_name") \
            .withColumn("updated_at", current_timestamp())
        
        write_to_postgres(carrier_stats, "carrier_performance", "append")
        
    except Exception as e:
        logger.error(f"❌ Error writing carrier performance: {e}")

def write_category_analytics_to_postgres(batch_df, batch_id):
    """Write category analytics to PostgreSQL"""
    try:
        logger.info(f"📊 Computing category analytics for batch {batch_id}...")
        
        category_stats = batch_df.groupBy("category_name") \
            .agg(
                count("*").alias("total_shipments"),
                spark_sum("sales").alias("total_sales"),
                avg("order_item_quantity").alias("avg_quantity"),
                avg("product_price").alias("avg_price")
            ) \
            .withColumn("updated_at", current_timestamp())
        
        write_to_postgres(category_stats, "category_analytics", "append")
        
    except Exception as e:
        logger.error(f"❌ Error writing category analytics: {e}")

def write_city_stats_to_postgres(batch_df, batch_id):
    """Write city statistics to PostgreSQL"""
    try:
        logger.info(f"📊 Computing city stats for batch {batch_id}...")
        
        city_stats = batch_df.groupBy("customer_city", "customer_state", "customer_country") \
            .agg(
                count("*").alias("total_shipments"),
                spark_sum("sales").alias("total_sales"),
                avg("days_for_shipping_real").alias("avg_delivery_days")
            ) \
            .withColumnRenamed("customer_city", "city_name") \
            .withColumnRenamed("customer_state", "state") \
            .withColumnRenamed("customer_country", "country") \
            .withColumn("updated_at", current_timestamp())
        
        write_to_postgres(city_stats, "city_stats", "append")
        
    except Exception as e:
        logger.error(f"❌ Error writing city stats: {e}")

def write_shipment_stats_to_postgres(batch_df, batch_id):
    """Write overall shipment statistics to PostgreSQL"""
    try:
        logger.info(f"📊 Computing shipment stats for batch {batch_id}...")
        
        total = batch_df.count()
        delivered = batch_df.filter(col("delivery_status") == "Shipping on time").count()
        delayed = batch_df.filter(col("late_delivery_risk") == 1).count()
        in_transit = total - delivered
        avg_days = batch_df.agg(avg("days_for_shipping_real")).collect()[0][0]
        on_time_rate = (delivered / total * 100) if total > 0 else 0
        
        stats = spark.createDataFrame([
            Row(
                timestamp=current_timestamp(),
                total_shipments=total,
                delivered=delivered,
                in_transit=in_transit,
                delayed=delayed,
                avg_delivery_days=float(avg_days) if avg_days else 0.0,
                on_time_delivery_rate=float(on_time_rate)
            )
        ])
        
        write_to_postgres(stats, "shipment_stats", "append")
        
    except Exception as e:
        logger.error(f"❌ Error writing shipment stats: {e}")

def write_delayed_shipments_to_postgres(batch_df, batch_id):
    """Write delayed shipments to PostgreSQL"""
    try:
        logger.info(f"📊 Identifying delayed shipments for batch {batch_id}...")
        
        delayed = batch_df.filter(col("late_delivery_risk") == 1) \
            .select(
                "shipment_id",
                "order_id",
                col("customer_fname").alias("customer_name"),
                "customer_city",
                "shipping_mode",
                col("days_for_shipping_real").alias("delay_days"),
                "delivery_status",
                "late_delivery_risk"
            ) \
            .withColumn("detected_at", current_timestamp())
        
        if delayed.count() > 0:
            write_to_postgres(delayed, "delayed_shipments", "append")
        else:
            logger.info(f"⚠️ No delayed shipments in batch {batch_id}")
        
    except Exception as e:
        logger.error(f"❌ Error writing delayed shipments: {e}")

def write_hourly_shipments_to_postgres(batch_df, batch_id):
    """Write hourly shipment volume to PostgreSQL"""
    try:
        logger.info(f"📊 Computing hourly shipments for batch {batch_id}...")
        
        hourly = batch_df.withColumn("hour", date_trunc("hour", current_timestamp())) \
            .groupBy("hour") \
            .agg(
                count("*").alias("shipment_count"),
                spark_sum("sales").alias("total_sales")
            ) \
            .withColumnRenamed("hour", "hour_timestamp") \
            .withColumn("created_at", current_timestamp())
        
        write_to_postgres(hourly, "hourly_shipments", "append")
        
    except Exception as e:
        logger.error(f"❌ Error writing hourly shipments: {e}")

def write_shipping_mode_stats_to_postgres(batch_df, batch_id):
    """Write shipping mode statistics to PostgreSQL"""
    try:
        logger.info(f"📊 Computing shipping mode stats for batch {batch_id}...")
        
        mode_stats = batch_df.groupBy("shipping_mode") \
            .agg(
                count("*").alias("total_shipments"),
                avg("days_for_shipping_real").alias("avg_delivery_days"),
                spark_sum("sales").alias("total_sales")
            ) \
            .withColumn("updated_at", current_timestamp())
        
        write_to_postgres(mode_stats, "shipping_mode_stats", "append")
        
    except Exception as e:
        logger.error(f"❌ Error writing shipping mode stats: {e}")

def write_route_analytics_to_postgres(batch_df, batch_id):
    """Write route analytics to PostgreSQL"""
    try:
        logger.info(f"📊 Computing route analytics for batch {batch_id}...")
        
        routes = batch_df.groupBy("order_city", "customer_city") \
            .agg(
                count("*").alias("shipment_count"),
                avg("days_for_shipping_real").alias("avg_delivery_days"),
                spark_sum("sales").alias("total_sales")
            ) \
            .withColumnRenamed("order_city", "origin_city") \
            .withColumnRenamed("customer_city", "destination_city") \
            .withColumn("updated_at", current_timestamp())
        
        write_to_postgres(routes, "route_analytics", "append")
        
    except Exception as e:
        logger.error(f"❌ Error writing route analytics: {e}")

# ========== Batch Processing ==========

def process_batch(batch_df, batch_id):
    """Process each batch and write to MongoDB and PostgreSQL"""
    try:
        if batch_df.isEmpty():
            logger.info(f"⚠️ Batch {batch_id} is empty, skipping...")
            return
        
        logger.info("=" * 60)
        logger.info(f"🔄 Processing Batch {batch_id}")
        logger.info("=" * 60)
        
        # Write to MongoDB
        write_raw_events_to_mongo(batch_df, batch_id)
        write_shipments_to_mongo(batch_df, batch_id)
        write_aggregations_to_mongo(batch_df, batch_id)
        
        # Write to PostgreSQL
        write_carrier_performance_to_postgres(batch_df, batch_id)
        write_category_analytics_to_postgres(batch_df, batch_id)
        write_city_stats_to_postgres(batch_df, batch_id)
        write_shipment_stats_to_postgres(batch_df, batch_id)
        write_delayed_shipments_to_postgres(batch_df, batch_id)
        write_hourly_shipments_to_postgres(batch_df, batch_id)
        write_shipping_mode_stats_to_postgres(batch_df, batch_id)
        write_route_analytics_to_postgres(batch_df, batch_id)
        
        logger.info(f"✅ Batch {batch_id} processing complete!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Error processing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# ========== Main ==========

def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("🚀 SUPPLY CHAIN SPARK PROCESSOR STARTING")
    logger.info("=" * 60)
    logger.info(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"MongoDB: {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")
    logger.info(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info("=" * 60)
    
    try:
        # Create Spark session
        create_spark_session()
        
        # Read from Kafka
        shipment_stream = read_from_kafka(spark)
        
        # Process and write to databases
        logger.info("📊 Starting stream processing with MongoDB and PostgreSQL writes...")
        
        query = shipment_stream.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()
        
        logger.info("✅ Stream processing started. Writing to databases...")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

if __name__ == "__main__":
    main()