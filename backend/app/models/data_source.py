from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, JSON
from datetime import datetime
from enum import Enum
from app.db.base import Base


class DataSourceType(str, Enum):
    # Relational Databases
    MYSQL = "mysql"
    POSTGRESQL = "postgresql" 
    SQLITE = "sqlite"
    MSSQL = "mssql"
    ORACLE = "oracle"
    
    # NoSQL Databases
    MONGODB = "mongodb"
    REDIS = "redis"
    ELASTICSEARCH = "elasticsearch"
    CASSANDRA = "cassandra"
    HBASE = "hbase"
    
    # Message Queues
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
    
    # APIs
    REST_API = "rest_api"
    GRAPHQL = "graphql"
    
    # Files
    CSV = "csv"
    EXCEL = "excel"
    JSON_FILE = "json_file"
    PARQUET = "parquet"


class ExtractionMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    REAL_TIME = "real_time"
    CHUNKED = "chunked"


class DataSource(Base):
    __tablename__ = "data_sources"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    type = Column(String(50), nullable=False)  # DataSourceType
    connection_config = Column(JSON, nullable=False)  # Connection parameters
    extraction_config = Column(JSON, nullable=True)  # Extraction settings
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_sync_at = Column(DateTime, nullable=True)
    description = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<DataSource(name='{self.name}', type='{self.type}')>"


class ExtractionJob(Base):
    __tablename__ = "extraction_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    data_source_id = Column(Integer, nullable=False)
    job_name = Column(String(255), nullable=False)
    extraction_mode = Column(String(50), nullable=False)  # ExtractionMode
    source_query = Column(Text, nullable=True)  # SQL query or collection name
    target_table = Column(String(255), nullable=False)
    status = Column(String(50), default="pending")  # pending, running, completed, failed
    records_processed = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    config = Column(JSON, nullable=True)  # Job-specific configuration
    
    def __repr__(self):
        return f"<ExtractionJob(name='{self.job_name}', status='{self.status}')>"