import os
import time
import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import logging

#setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurations
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'shipments')
CSV_PATH = os.getenv('CSV_PATH', '/app/data/DataCoSupplyChainDataset.csv')
STREAM_SPEED = float(os.getenv('STREAM_SPEED', '0.1'))  # Delay between messages in seconds

def create_kafka_producer():
    """Create and return kafka producer with retry logic"""
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer = lambda v: json.dumps(v).encode('utf-8'),
                acks = 'all',
                retries = 3
            )
            logger.info(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer

        except Exception as e:
            logger.warning(f"⚠️ Attempt {attempt + 1}/{max_retries} - Cannot connect to Kafka: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ Failed to connect to Kafka after all retries")
                raise

def load_csv_data(csv_path):
    """Load and prepare CSV data"""
    try:
        logger.info(f"✅ Loading CSV from {csv_path}")
        df = pd.read_csv(csv_path, encoding='latin-1')
        logger.info(f"✅ Loaded {len(df)} records from CSV")
        logger.info(f"📊 Columns: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"❌ Error loading CSV: {e}")
        raise

# Create shipment event
def create_shipment_event(row):
    """Convert CSV row to shipment event"""
    try:
        event = {
            'shipment_id': str(row.get('Order Id', '')),
            'order_id': str(row.get('Order Id', '')),
            'order_date': str(row.get('order date (DateOrders)', '')),
            'shipping_date': str(row.get('shipping date (DateOrders)', '')),
            'delivery_date': str(row.get('Delivery Status', '')),
            
            # Product information
            'product_name': str(row.get('Product Name', '')),
            'category_name': str(row.get('Category Name', '')),
            'product_price': float(row.get('Product Price', 0)),
            'order_item_quantity': int(row.get('Order Item Quantity', 0)),
            
            # Customer information
            'customer_fname': str(row.get('Customer Fname', '')),
            'customer_lname': str(row.get('Customer Lname', '')),
            'customer_city': str(row.get('Customer City', '')),
            'customer_state': str(row.get('Customer State', '')),
            'customer_country': str(row.get('Customer Country', '')),
            
            # Shipping information
            'shipping_mode': str(row.get('Shipping Mode', '')),
            'order_city': str(row.get('Order City', '')),  # Warehouse/Origin
            'order_state': str(row.get('Order State', '')),
            'order_country': str(row.get('Order Country', '')),
            'days_for_shipment_scheduled': int(row.get('Days for shipment (scheduled)', 0)),
            'days_for_shipping_real': int(row.get('Days for shipping (real)', 0)),
            
            # Status and performance
            'delivery_status': str(row.get('Delivery Status', '')),
            'late_delivery_risk': int(row.get('Late_delivery_risk', 0)),
            
            # Financial
            'sales': float(row.get('Sales', 0)),
            'order_item_discount': float(row.get('Order Item Discount', 0)),
            'order_item_profit_ratio': float(row.get('Order Item Profit Ratio', 0)),
            
            # Metadata
            'timestamp': datetime.now().isoformat(),
            'event_type': 'SHIPMENT_CREATED'
        }
        
        return event
    except Exception as e:
        logger.error(f"❌ Error creating event from row: {e}")
        return None


# Stream data to kafka
def stream_data(producer, df):
    logger.info(f"🚀 Starting to stream {len(df)} records to topic '{KAFKA_TOPIC}'")
    logger.info(f"⏱️ Stream speed: {STREAM_SPEED} seconds between messages")

    success_count = 0
    error_count = 0

    try:
        for index, row in df.iterrows():
            try:
                # Create shipment from a row
                event = create_shipment_event(row)

                if event:
                    # Send to kafka
                    producer.send(KAFKA_TOPIC, value=event)
                    success_count += 1

                    # Log process every 100 records
                    if success_count % 100 == 0:
                        logger.info(f"📤 Streamed {success_count}/{len(df)} records ({(success_count/len(df)*100):.1f}%)")
                    
                    time.sleep(STREAM_SPEED)
                else:
                    error_count += 1

            except Exception as e:
                    logger.error(f"❌ Error streaming record {index}: {e}")
                    continue
            
        # Flush remaining messages
        producer.flush()

        logger.info("=" * 60)
        logger.info(f"✅ STREAMING COMPLETE!")
        logger.info(f"📊 Total records: {len(df)}")
        logger.info(f"✅ Successfully streamed: {success_count}")
        logger.info(f"❌ Errors: {error_count}")
        logger.info("=" * 60)
    
    except KeyboardInterrupt:
        logger.info("⚠️ Streaming interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error during streaming: {e}")
    finally:
        producer.close()
        logger.info("🔒 Kafka producer closed")

def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("🚀 SUPPLY CHAIN DATA PRODUCER STARTING")
    logger.info("=" * 60)
    logger.info(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"CSV Path: {CSV_PATH}")
    logger.info("=" * 60)

    try:
        # Create kafka producer
        producer = create_kafka_producer()

        # Load CSV data
        df = load_csv_data(CSV_PATH)

        # Stream data to kafka
        stream_data(producer, df)

    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        exit(1)
    
if __name__ == "__main__":
    main()




