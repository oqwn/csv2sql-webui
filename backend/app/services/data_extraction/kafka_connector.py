import pandas as pd
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
import json
import asyncio

logger = logging.getLogger(__name__)

try:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class KafkaConnector(DataSourceConnector):
    """Connector for Apache Kafka message streaming platform"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.bootstrap_servers = connection_config.get('bootstrap_servers', 'localhost:9092')
        self.security_protocol = connection_config.get('security_protocol', 'PLAINTEXT')
        self.sasl_mechanism = connection_config.get('sasl_mechanism', 'PLAIN')
        self.sasl_username = connection_config.get('sasl_username', '')
        self.sasl_password = connection_config.get('sasl_password', '')
        self.consumer_group = connection_config.get('consumer_group', 'csv2sql_consumer')
        self.auto_offset_reset = connection_config.get('auto_offset_reset', 'earliest')
        
        self.consumer = None
        self.producer = None
        
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python is required for Kafka connections. Install with: pip install kafka-python")
    
    async def connect(self) -> bool:
        """Establish connection to Kafka"""
        try:
            # Create a test consumer to verify connection
            test_consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_username=self.sasl_username if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_password=self.sasl_password if self.security_protocol != 'PLAINTEXT' else None,
                consumer_timeout_ms=5000,
                group_id=f"{self.consumer_group}_test"
            )
            
            # Get cluster metadata to verify connection
            topics = test_consumer.topics()
            test_consumer.close()
            
            logger.info(f"Successfully connected to Kafka cluster. Found {len(topics)} topics.")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close Kafka connections"""
        try:
            if self.consumer:
                self.consumer.close()
                self.consumer = None
            if self.producer:
                self.producer.close()
                self.producer = None
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect from Kafka: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Kafka connection and return metadata"""
        try:
            if not await self.connect():
                return {
                    "status": "error",
                    "error": "Failed to connect to Kafka"
                }
            
            # Get cluster info
            test_consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_username=self.sasl_username if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_password=self.sasl_password if self.security_protocol != 'PLAINTEXT' else None,
                consumer_timeout_ms=5000,
                group_id=f"{self.consumer_group}_test"
            )
            
            topics = test_consumer.topics()
            test_consumer.close()
            
            return {
                "status": "success",
                "database_type": "kafka",
                "topic_count": len(topics),
                "bootstrap_servers": self.bootstrap_servers,
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get available Kafka topics as schema information"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_username=self.sasl_username if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_password=self.sasl_password if self.security_protocol != 'PLAINTEXT' else None,
                consumer_timeout_ms=5000,
                group_id=f"{self.consumer_group}_schema"
            )
            
            topics = consumer.topics()
            schema_info = []
            
            for topic in topics:
                try:
                    # Get partition information
                    partitions = consumer.partitions_for_topic(topic)
                    
                    schema_info.append({
                        'name': topic,
                        'type': 'topic',
                        'partition_count': len(partitions) if partitions else 0,
                        'description': f'Kafka topic with {len(partitions) if partitions else 0} partitions'
                    })
                except Exception as e:
                    logger.warning(f"Could not get info for topic {topic}: {str(e)}")
                    schema_info.append({
                        'name': topic,
                        'type': 'topic',
                        'partition_count': 0,
                        'description': 'Kafka topic (info unavailable)'
                    })
            
            consumer.close()
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get Kafka schema: {str(e)}")
            return []
    
    async def preview_data(self, source_name: str, limit: int = 100) -> Dict[str, Any]:
        """Preview messages from a Kafka topic."""
        try:
            consumer = KafkaConsumer(
                source_name,
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_username=self.sasl_username if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_password=self.sasl_password if self.security_protocol != 'PLAINTEXT' else None,
                auto_offset_reset=self.auto_offset_reset,
                consumer_timeout_ms=10000,
                group_id=f"{self.consumer_group}_preview",
                value_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            
            messages = []
            message_count = 0
            
            for message in consumer:
                if message_count >= limit:
                    break
                
                try:
                    # Try to parse as JSON
                    if message.value:
                        try:
                            parsed_value = json.loads(message.value)
                        except json.JSONDecodeError:
                            parsed_value = message.value
                    else:
                        parsed_value = None
                    
                    msg_data = {
                        'offset': message.offset,
                        'partition': message.partition,
                        'timestamp': message.timestamp,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'value': parsed_value,
                        'headers': dict(message.headers) if message.headers else {}
                    }
                    
                    messages.append(msg_data)
                    message_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing message: {str(e)}")
                    continue
            
            consumer.close()
            
            if not messages:
                return {
                    'status': 'success',
                    'columns': [],
                    'sample_data': [],
                    'row_count': 0,
                    'message': 'No messages found in topic'
                }
            
            # Create DataFrame from messages for consistent format
            df = pd.json_normalize(messages)
            
            columns = [
                {
                    'name': col,
                    'sql_type': self._infer_sql_type(df[col])
                }
                for col in df.columns
            ]
            
            sample_data = df.head(limit).to_dict('records')
            
            return {
                'status': 'success',
                'columns': columns,
                'sample_data': sample_data,
                'row_count': len(messages)
            }
            
        except Exception as e:
            logger.error(f"Failed to preview Kafka topic {source_name}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'columns': [],
                'sample_data': [],
                'row_count': 0
            }
    
    async def extract_data(
        self,
        source: str,
        extraction_config: Dict[str, Any],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data from Kafka topic"""
        try:
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            consumer = KafkaConsumer(
                source,  # topic name
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_username=self.sasl_username if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_password=self.sasl_password if self.security_protocol != 'PLAINTEXT' else None,
                auto_offset_reset=self.auto_offset_reset,
                group_id=self.consumer_group,
                value_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            
            messages = []
            message_count = 0
            
            for message in consumer:
                try:
                    # Try to parse as JSON
                    if message.value:
                        try:
                            parsed_value = json.loads(message.value)
                        except json.JSONDecodeError:
                            parsed_value = message.value
                    else:
                        parsed_value = None
                    
                    msg_data = {
                        'offset': message.offset,
                        'partition': message.partition,
                        'timestamp': message.timestamp,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'value': parsed_value,
                        'headers': dict(message.headers) if message.headers else {}
                    }
                    
                    # If value is a dict, flatten it
                    if isinstance(parsed_value, dict):
                        for k, v in parsed_value.items():
                            msg_data[f'value_{k}'] = v
                    
                    messages.append(msg_data)
                    message_count += 1
                    
                    # Yield chunk when it reaches the specified size
                    if len(messages) >= chunk_size:
                        df = pd.json_normalize(messages)
                        yield df
                        messages = []
                        
                except Exception as e:
                    logger.warning(f"Error processing message: {str(e)}")
                    continue
            
            # Yield remaining messages
            if messages:
                df = pd.json_normalize(messages)
                yield df
            
            consumer.close()
            
        except Exception as e:
            logger.error(f"Failed to extract data from Kafka topic {source}: {str(e)}")
            raise
    
    def _infer_sql_type(self, series: pd.Series) -> str:
        """Infer SQL type from pandas series."""
        if series.dtype == 'object':
            # Check if it's JSON-like
            non_null = series.dropna()
            if len(non_null) > 0:
                sample = non_null.iloc[0]
                if isinstance(sample, (dict, list)):
                    return 'JSONB'
                return 'TEXT'
            return 'TEXT'
        elif pd.api.types.is_integer_dtype(series):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(series):
            return 'DOUBLE PRECISION'
        elif pd.api.types.is_bool_dtype(series):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(series):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get message count for a topic (approximate)"""
        try:
            consumer = KafkaConsumer(
                source,
                bootstrap_servers=self.bootstrap_servers.split(','),
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_username=self.sasl_username if self.security_protocol != 'PLAINTEXT' else None,
                sasl_plain_password=self.sasl_password if self.security_protocol != 'PLAINTEXT' else None,
                consumer_timeout_ms=5000,
                group_id=f"{self.consumer_group}_count"
            )
            
            # Get end offsets for all partitions
            partitions = consumer.partitions_for_topic(source)
            if not partitions:
                consumer.close()
                return 0
                
            topic_partitions = [TopicPartition(source, p) for p in partitions]
            end_offsets = consumer.end_offsets(topic_partitions)
            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            
            total_messages = sum(end_offsets[tp] - beginning_offsets[tp] for tp in topic_partitions)
            
            consumer.close()
            return total_messages
            
        except Exception as e:
            logger.error(f"Failed to get message count for topic {source}: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for Kafka"""
        return ['bootstrap_servers']
    
    async def supports_real_time_sync(self) -> bool:
        """Kafka supports real-time streaming"""
        return True