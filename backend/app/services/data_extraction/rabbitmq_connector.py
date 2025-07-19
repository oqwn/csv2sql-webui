import pandas as pd
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
import json
import asyncio

logger = logging.getLogger(__name__)

try:
    import pika
    from pika.exceptions import AMQPConnectionError, AMQPChannelError
    PIKA_AVAILABLE = True
except ImportError:
    PIKA_AVAILABLE = False


class RabbitMQConnector(DataSourceConnector):
    """Connector for RabbitMQ message broker"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.host = connection_config.get('host', 'localhost')
        self.port = connection_config.get('port', 5672)
        self.username = connection_config.get('username', 'guest')
        self.password = connection_config.get('password', 'guest')
        self.virtual_host = connection_config.get('virtual_host', '/')
        self.exchange = connection_config.get('exchange', '')
        self.queue_prefix = connection_config.get('queue_prefix', 'csv2sql')
        
        self.connection = None
        self.channel = None
        
        if not PIKA_AVAILABLE:
            raise ImportError("pika is required for RabbitMQ connections. Install with: pip install pika")
    
    async def connect(self) -> bool:
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2,
                socket_timeout=10
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Test basic operations
            server_properties = connection.server_properties
            
            channel.close()
            connection.close()
            
            logger.info(f"Successfully connected to RabbitMQ at {self.host}:{self.port}")
            return True
            
        except (AMQPConnectionError, AMQPChannelError, Exception) as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close RabbitMQ connections"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
                self.channel = None
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                self.connection = None
            return True
        except Exception as e:
            logger.error(f"Failed to disconnect from RabbitMQ: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test RabbitMQ connection and return metadata"""
        try:
            if not await self.connect():
                return {
                    "status": "error",
                    "error": "Failed to connect to RabbitMQ"
                }
            
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            server_properties = connection.server_properties
            
            channel.close()
            connection.close()
            
            return {
                "status": "success",
                "database_type": "rabbitmq",
                "server_version": server_properties.get("version", "Unknown"),
                "virtual_host": self.virtual_host,
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get available RabbitMQ queues and exchanges as schema information."""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            schema_info = []
            
            # Use management API if available, otherwise return limited info
            try:
                import requests
                
                # Try to get queue info via management API
                management_url = f"http://{self.host}:15672/api/queues/{self.virtual_host.replace('/', '%2F')}"
                response = requests.get(
                    management_url,
                    auth=(self.username, self.password),
                    timeout=5
                )
                
                if response.status_code == 200:
                    queues = response.json()
                    for queue in queues:
                        schema_info.append({
                            'name': queue['name'],
                            'type': 'queue',
                            'message_count': queue.get('messages', 0),
                            'consumers': queue.get('consumers', 0),
                            'description': f"RabbitMQ queue with {queue.get('messages', 0)} messages"
                        })
                else:
                    # Fallback to basic queue declaration test
                    test_queue = f"{self.queue_prefix}_test"
                    channel.queue_declare(queue=test_queue, passive=True)
                    schema_info.append({
                        'name': test_queue,
                        'type': 'queue',
                        'message_count': 0,
                        'description': 'RabbitMQ queue (limited info available)'
                    })
                    
            except (ImportError, requests.RequestException, Exception):
                # Basic fallback - just indicate RabbitMQ is available
                schema_info.append({
                    'name': 'default_queue',
                    'type': 'queue',
                    'message_count': 0,
                    'description': 'RabbitMQ queues available (use Management API for full listing)'
                })
            
            channel.close()
            connection.close()
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get RabbitMQ schema: {str(e)}")
            return []
    
    async def preview_data(self, source_name: str, limit: int = 100) -> Dict[str, Any]:
        """Preview messages from a RabbitMQ queue."""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Ensure queue exists
            channel.queue_declare(queue=source_name, durable=True)
            
            messages = []
            message_count = 0
            
            def callback(ch, method, properties, body):
                nonlocal message_count, messages
                
                if message_count >= limit:
                    ch.stop_consuming()
                    return
                
                try:
                    # Try to decode and parse message
                    body_str = body.decode('utf-8')
                    
                    try:
                        parsed_body = json.loads(body_str)
                    except json.JSONDecodeError:
                        parsed_body = body_str
                    
                    msg_data = {
                        'delivery_tag': method.delivery_tag,
                        'exchange': method.exchange,
                        'routing_key': method.routing_key,
                        'message_id': properties.message_id,
                        'timestamp': properties.timestamp,
                        'content_type': properties.content_type,
                        'headers': dict(properties.headers) if properties.headers else {},
                        'body': parsed_body
                    }
                    
                    # If body is a dict, flatten it
                    if isinstance(parsed_body, dict):
                        for k, v in parsed_body.items():
                            msg_data[f'body_{k}'] = v
                    
                    messages.append(msg_data)
                    message_count += 1
                    
                    # Don't acknowledge to keep messages in queue for preview
                    # ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    logger.warning(f"Error processing message: {str(e)}")
                
                if message_count >= limit:
                    ch.stop_consuming()
            
            # Set up consumer with manual acknowledgment
            channel.basic_qos(prefetch_count=limit)
            channel.basic_consume(queue=source_name, on_message_callback=callback, auto_ack=False)
            
            # Consume messages with timeout
            connection.add_timeout(10, lambda: channel.stop_consuming())
            channel.start_consuming()
            
            channel.close()
            connection.close()
            
            if not messages:
                return {
                    'status': 'success',
                    'columns': [],
                    'sample_data': [],
                    'row_count': 0,
                    'message': 'No messages found in queue'
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
            logger.error(f"Failed to preview RabbitMQ queue {source_name}: {str(e)}")
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
        """Extract data from RabbitMQ queue"""
        try:
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Ensure queue exists
            channel.queue_declare(queue=source, durable=True)
            
            messages = []
            
            def callback(ch, method, properties, body):
                nonlocal messages
                
                try:
                    # Try to decode and parse message
                    body_str = body.decode('utf-8')
                    
                    try:
                        parsed_body = json.loads(body_str)
                    except json.JSONDecodeError:
                        parsed_body = body_str
                    
                    msg_data = {
                        'delivery_tag': method.delivery_tag,
                        'exchange': method.exchange,
                        'routing_key': method.routing_key,
                        'message_id': properties.message_id,
                        'timestamp': properties.timestamp,
                        'content_type': properties.content_type,
                        'headers': dict(properties.headers) if properties.headers else {},
                        'body': parsed_body
                    }
                    
                    # If body is a dict, flatten it
                    if isinstance(parsed_body, dict):
                        for k, v in parsed_body.items():
                            msg_data[f'body_{k}'] = v
                    
                    messages.append(msg_data)
                    
                    # Acknowledge message after processing
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                    # Yield chunk when it reaches the specified size
                    if len(messages) >= chunk_size:
                        df = pd.json_normalize(messages)
                        messages = []  # Reset for next chunk
                        return df
                        
                except Exception as e:
                    logger.warning(f"Error processing message: {str(e)}")
                    # Reject message to avoid infinite loop
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set up consumer
            channel.basic_qos(prefetch_count=chunk_size)
            channel.basic_consume(queue=source, on_message_callback=callback, auto_ack=False)
            
            # Process messages in chunks
            while True:
                try:
                    connection.process_data_events(time_limit=30)  # 30 second timeout
                    
                    # If we have messages, yield them
                    if messages:
                        df = pd.json_normalize(messages)
                        yield df
                        messages = []
                    else:
                        # No more messages, break
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing messages: {str(e)}")
                    break
            
            # Yield remaining messages
            if messages:
                df = pd.json_normalize(messages)
                yield df
            
            channel.close()
            connection.close()
            
        except Exception as e:
            logger.error(f"Failed to extract data from RabbitMQ queue {source}: {str(e)}")
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
        """Get message count for a queue"""
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials
            )
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Get queue info
            queue_info = channel.queue_declare(queue=source, passive=True)
            message_count = queue_info.method.message_count
            
            channel.close()
            connection.close()
            
            return message_count
            
        except Exception as e:
            logger.error(f"Failed to get message count for queue {source}: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for RabbitMQ"""
        return ['host']
    
    async def supports_real_time_sync(self) -> bool:
        """RabbitMQ supports real-time messaging"""
        return True