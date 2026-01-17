-- Supply Chain Analytics Database Schema
-- This script runs automatically when PostgreSQL container starts for the first time

-- Table 1: Real-time shipment statistics (aggregated per hour)
CREATE TABLE IF NOT EXISTS shipment_stats (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    total_shipments INTEGER DEFAULT 0,
    delivered INTEGER DEFAULT 0,
    in_transit INTEGER DEFAULT 0,
    delayed INTEGER DEFAULT 0,
    avg_delivery_days FLOAT DEFAULT 0,
    on_time_delivery_rate FLOAT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: Carrier performance metrics
CREATE TABLE IF NOT EXISTS carrier_performance (
    id SERIAL PRIMARY KEY,
    carrier_name VARCHAR(100) NOT NULL,
    total_shipments INTEGER DEFAULT 0,
    avg_delivery_days FLOAT DEFAULT 0,
    on_time_rate FLOAT DEFAULT 0,
    delayed_shipments INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Route analytics (origin to destination)
CREATE TABLE IF NOT EXISTS route_analytics (
    id SERIAL PRIMARY KEY,
    origin_city VARCHAR(100) NOT NULL,
    destination_city VARCHAR(100) NOT NULL,
    shipment_count INTEGER DEFAULT 0,
    avg_delivery_days FLOAT DEFAULT 0,
    total_sales DECIMAL(12,2) DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: City-wise shipment statistics
CREATE TABLE IF NOT EXISTS city_stats (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state VARCHAR(100),
    country VARCHAR(100),
    total_shipments INTEGER DEFAULT 0,
    total_sales DECIMAL(12,2) DEFAULT 0,
    avg_delivery_days FLOAT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 5: Product category analytics
CREATE TABLE IF NOT EXISTS category_analytics (
    id SERIAL PRIMARY KEY,
    category_name VARCHAR(150) NOT NULL,
    total_shipments INTEGER DEFAULT 0,
    total_sales DECIMAL(12,2) DEFAULT 0,
    avg_quantity FLOAT DEFAULT 0,
    avg_price DECIMAL(10,2) DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 6: Delayed/problematic shipments
CREATE TABLE IF NOT EXISTS delayed_shipments (
    id SERIAL PRIMARY KEY,
    shipment_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100),
    customer_name VARCHAR(200),
    customer_city VARCHAR(100),
    carrier VARCHAR(100),
    shipping_mode VARCHAR(50),
    delay_days INTEGER,
    status VARCHAR(50),
    late_delivery_risk INTEGER,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 7: Hourly shipment volume (for time-series chart)
CREATE TABLE IF NOT EXISTS hourly_shipments (
    id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP NOT NULL,
    shipment_count INTEGER DEFAULT 0,
    total_sales DECIMAL(12,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 8: Shipping mode distribution
CREATE TABLE IF NOT EXISTS shipping_mode_stats (
    id SERIAL PRIMARY KEY,
    shipping_mode VARCHAR(50) NOT NULL,
    total_shipments INTEGER DEFAULT 0,
    avg_delivery_days FLOAT DEFAULT 0,
    total_sales DECIMAL(12,2) DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_shipment_stats_timestamp ON shipment_stats(timestamp);
CREATE INDEX IF NOT EXISTS idx_hourly_timestamp ON hourly_shipments(hour_timestamp);
CREATE INDEX IF NOT EXISTS idx_delayed_shipments_detected ON delayed_shipments(detected_at);
CREATE INDEX IF NOT EXISTS idx_carrier_name ON carrier_performance(carrier_name);
CREATE INDEX IF NOT EXISTS idx_category_name ON category_analytics(category_name);
CREATE INDEX IF NOT EXISTS idx_city_name ON city_stats(city_name);
CREATE INDEX IF NOT EXISTS idx_shipping_mode ON shipping_mode_stats(shipping_mode);

-- Insert initial data (optional - for testing)
INSERT INTO shipment_stats (timestamp, total_shipments, delivered, in_transit, delayed, avg_delivery_days, on_time_delivery_rate)
VALUES (CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, 0);

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Supply Chain database schema created successfully!';
END $$;