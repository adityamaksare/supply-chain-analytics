from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from config import Config
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)
CORS(app)  # Enable CORS for React frontend

# Database connections
postgres_conn = None
mongo_client = None
mongo_db = None

def get_postgres_connection():
    """Get PostgreSQL connection"""
    global postgres_conn
    try:
        if postgres_conn is None or postgres_conn.closed:
            postgres_conn = psycopg2.connect(
                host=Config.POSTGRES_HOST,
                port=Config.POSTGRES_PORT,
                database=Config.POSTGRES_DB,
                user=Config.POSTGRES_USER,
                password=Config.POSTGRES_PASSWORD
            )
            logger.info("✅ Connected to PostgreSQL")
        return postgres_conn
    except Exception as e:
        logger.error(f"❌ PostgreSQL connection error: {e}")
        return None

def get_mongo_db():
    """Get MongoDB database"""
    global mongo_client, mongo_db
    try:
        if mongo_client is None:
            mongo_uri = f"mongodb://{Config.MONGO_USER}:{Config.MONGO_PASSWORD}@{Config.MONGO_HOST}:{Config.MONGO_PORT}/{Config.MONGO_DB}?authSource=admin"
            mongo_client = MongoClient(mongo_uri)
            mongo_db = mongo_client[Config.MONGO_DB]
            logger.info("✅ Connected to MongoDB")
        return mongo_db
    except Exception as e:
        logger.error(f"❌ MongoDB connection error: {e}")
        return None

# ========== API Endpoints ==========

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'Supply Chain Analytics API'
    }), 200

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get overall KPI metrics"""
    try:
        conn = get_postgres_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get latest shipment stats
        cursor.execute("""
            SELECT 
                total_shipments,
                delivered,
                in_transit,
                delayed,
                avg_delivery_days,
                on_time_delivery_rate,
                timestamp
            FROM shipment_stats
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        
        stats = cursor.fetchone()
        cursor.close()
        
        if stats:
            return jsonify({
                'success': True,
                'data': dict(stats)
            }), 200
        else:
            return jsonify({
                'success': True,
                'data': {
                    'total_shipments': 0,
                    'delivered': 0,
                    'in_transit': 0,
                    'delayed': 0,
                    'avg_delivery_days': 0,
                    'on_time_delivery_rate': 0
                }
            }), 200
            
    except Exception as e:
        logger.error(f"❌ Error in /api/stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/carriers', methods=['GET'])
def get_carriers():
    """Get carrier performance metrics"""
    try:
        conn = get_postgres_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get carrier performance
        cursor.execute("""
            SELECT 
                carrier_name,
                SUM(total_shipments) as total_shipments,
                AVG(avg_delivery_days) as avg_delivery_days,
                AVG(on_time_rate) as on_time_rate,
                SUM(delayed_shipments) as delayed_shipments
            FROM carrier_performance
            GROUP BY carrier_name
            ORDER BY total_shipments DESC
        """)
        
        carriers = cursor.fetchall()
        cursor.close()
        
        return jsonify({
            'success': True,
            'data': [dict(row) for row in carriers]
        }), 200
            
    except Exception as e:
        logger.error(f"❌ Error in /api/carriers: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get category analytics"""
    try:
        conn = get_postgres_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get category analytics
        cursor.execute("""
            SELECT 
                category_name,
                SUM(total_shipments) as total_shipments,
                SUM(total_sales) as total_sales,
                AVG(avg_quantity) as avg_quantity,
                AVG(avg_price) as avg_price
            FROM category_analytics
            GROUP BY category_name
            ORDER BY total_sales DESC
            LIMIT 20
        """)
        
        categories = cursor.fetchall()
        cursor.close()
        
        return jsonify({
            'success': True,
            'data': [dict(row) for row in categories]
        }), 200
            
    except Exception as e:
        logger.error(f"❌ Error in /api/categories: {e}")
        return jsonify({'error': str(e)}), 500

# ========== Error Handlers ==========

@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({'error': 'Internal server error'}), 500

# ========== Main ==========

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("🚀 SUPPLY CHAIN ANALYTICS API STARTING")
    logger.info("=" * 60)
    logger.info(f"Flask running on {Config.HOST}:{Config.PORT}")
    logger.info(f"PostgreSQL: {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}")
    logger.info(f"MongoDB: {Config.MONGO_HOST}:{Config.MONGO_PORT}")
    logger.info("=" * 60)
    
    app.run(
        host=Config.HOST,
        port=Config.PORT,
        debug=Config.DEBUG
    )