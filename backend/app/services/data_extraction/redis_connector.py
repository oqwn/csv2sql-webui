import pandas as pd
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging

logger = logging.getLogger(__name__)

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class RedisConnector(DataSourceConnector):
    """Connector for Redis databases"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.client = None
        
        if not REDIS_AVAILABLE:
            raise ImportError("redis is required for Redis connections. Install with: pip install redis")
    
    async def connect(self) -> bool:
        """Establish connection to Redis"""
        try:
            self.client = redis.Redis(
                host=self.connection_config.get('host', 'localhost'),
                port=self.connection_config.get('port', 6379),
                db=self.connection_config.get('database', 0),
                password=self.connection_config.get('password'),
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            
            # Test the connection
            self.client.ping()
            
            logger.info("Successfully connected to Redis")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close Redis connection"""
        try:
            if self.client:
                self.client.close()
                self.client = None
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect from Redis: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Redis connection and return metadata"""
        try:
            if not self.client:
                await self.connect()
                
            # Get Redis info
            info = self.client.info()
            
            # Get database size
            db_size = self.client.dbsize()
            
            return {
                "status": "success",
                "database_type": "redis",
                "version": info.get("redis_version", "Unknown"),
                "key_count": db_size,
                "memory_usage": info.get("used_memory_human", "Unknown"),
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get Redis key patterns and their types"""
        try:
            if not self.client:
                await self.connect()
                
            # Get all keys (be careful with large databases)
            keys = self.client.keys("*")
            
            # Group by data type and pattern
            type_info = {}
            
            for key in keys[:1000]:  # Limit to first 1000 keys for performance
                key_type = self.client.type(key)
                
                if key_type not in type_info:
                    type_info[key_type] = {
                        "type": key_type,
                        "count": 0,
                        "sample_keys": []
                    }
                
                type_info[key_type]["count"] += 1
                if len(type_info[key_type]["sample_keys"]) < 10:
                    type_info[key_type]["sample_keys"].append(key)
            
            schema_info = []
            for data_type, info in type_info.items():
                schema_info.append({
                    "name": f"{data_type}_keys",
                    "type": data_type,
                    "key_count": info["count"],
                    "sample_keys": info["sample_keys"]
                })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get Redis schema info: {str(e)}")
            return []
    
    async def extract_data(
        self,
        source: str,
        extraction_config: Dict[str, Any],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data from Redis"""
        try:
            if not self.client:
                await self.connect()
                
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            # Get keys matching the pattern
            pattern = source if source else "*"
            keys = self.client.keys(pattern)
            
            data_rows = []
            
            for i, key in enumerate(keys):
                try:
                    key_type = self.client.type(key)
                    ttl = self.client.ttl(key)
                    
                    # Extract value based on type
                    if key_type == "string":
                        value = self.client.get(key)
                        data_rows.append({
                            "key": key,
                            "type": key_type,
                            "value": value,
                            "ttl": ttl,
                            "size": len(str(value)) if value else 0
                        })
                    
                    elif key_type == "hash":
                        hash_data = self.client.hgetall(key)
                        # Flatten hash data
                        for field, value in hash_data.items():
                            data_rows.append({
                                "key": key,
                                "type": key_type,
                                "field": field,
                                "value": value,
                                "ttl": ttl,
                                "size": len(str(value)) if value else 0
                            })
                    
                    elif key_type == "list":
                        list_data = self.client.lrange(key, 0, -1)
                        for index, value in enumerate(list_data):
                            data_rows.append({
                                "key": key,
                                "type": key_type,
                                "index": index,
                                "value": value,
                                "ttl": ttl,
                                "size": len(str(value)) if value else 0
                            })
                    
                    elif key_type == "set":
                        set_data = self.client.smembers(key)
                        for value in set_data:
                            data_rows.append({
                                "key": key,
                                "type": key_type,
                                "value": value,
                                "ttl": ttl,
                                "size": len(str(value)) if value else 0
                            })
                    
                    elif key_type == "zset":
                        zset_data = self.client.zrange(key, 0, -1, withscores=True)
                        for value, score in zset_data:
                            data_rows.append({
                                "key": key,
                                "type": key_type,
                                "value": value,
                                "score": score,
                                "ttl": ttl,
                                "size": len(str(value)) if value else 0
                            })
                    
                    # Yield chunk when it reaches the specified size
                    if len(data_rows) >= chunk_size:
                        yield pd.DataFrame(data_rows)
                        data_rows = []
                        
                except Exception as e:
                    logger.warning(f"Failed to extract data for key {key}: {str(e)}")
                    continue
            
            # Yield remaining data
            if data_rows:
                yield pd.DataFrame(data_rows)
                
        except Exception as e:
            logger.error(f"Failed to extract data from Redis: {str(e)}")
            raise
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get key count matching the pattern"""
        try:
            if not self.client:
                await self.connect()
                
            pattern = source if source else "*"
            keys = self.client.keys(pattern)
            return len(keys)
                
        except Exception as e:
            logger.error(f"Failed to get key count: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for Redis"""
        return ['host']
    
    async def supports_incremental_extraction(self) -> bool:
        """Redis doesn't traditionally support incremental extraction"""
        return False
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Redis doesn't have traditional incremental columns"""
        return []