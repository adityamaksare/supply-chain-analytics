import os

class Config:
    """Configuration for Flask API"""
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'supplychain')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')
    
    # MongoDB Configuration
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = int(os.getenv('MONGO_PORT', '27017'))
    MONGO_DB = os.getenv('MONGO_DB', 'supplychain')
    MONGO_USER = os.getenv('MONGO_USER', 'admin')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')
    
    # Flask Configuration
    DEBUG = os.getenv('FLASK_DEBUG', 'False') == 'True'
    HOST = '0.0.0.0'
    PORT = 5000