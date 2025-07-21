import pandas as pd
import boto3
import io
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
from botocore.exceptions import ClientError, NoCredentialsError
import json

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    ClientError = Exception
    NoCredentialsError = Exception


class S3Connector(DataSourceConnector):
    """Connector for Amazon S3 data sources"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 is required for S3 support. Install with: pip install boto3")
        
        super().__init__(connection_config)
        self.s3_client = None
        self.bucket_name = None
        
    async def connect(self) -> bool:
        """Test connection to S3."""
        try:
            # Initialize S3 client with credentials
            aws_access_key_id = self.connection_config.get('aws_access_key_id')
            aws_secret_access_key = self.connection_config.get('aws_secret_access_key')
            region_name = self.connection_config.get('region_name', 'us-east-1')
            
            if aws_access_key_id and aws_secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name
                )
            else:
                # Try to use default credentials (IAM role, env vars, etc.)
                self.s3_client = boto3.client('s3', region_name=region_name)
            
            self.bucket_name = self.connection_config.get('bucket_name')
            if not self.bucket_name:
                logger.error("S3 bucket name not specified")
                return False
            
            # Test connection by listing bucket (with limit)
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            return True
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            return False
        except ClientError as e:
            logger.error(f"Failed to connect to S3: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to S3: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Clean up S3 connection"""
        self.s3_client = None
        return True
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test S3 connection and return metadata"""
        try:
            if not await self.connect():
                return {
                    "status": "error",
                    "error": "Failed to connect to S3"
                }
            
            # Get bucket info
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    MaxKeys=1000
                )
                
                object_count = response.get('KeyCount', 0)
                total_size = sum(obj.get('Size', 0) for obj in response.get('Contents', []))
                
                # Get bucket location
                try:
                    location = self.s3_client.get_bucket_location(Bucket=self.bucket_name)
                    region = location.get('LocationConstraint') or 'us-east-1'
                except:
                    region = 'unknown'
                
                return {
                    "status": "success",
                    "database_type": "s3",
                    "bucket_name": self.bucket_name,
                    "region": region,
                    "object_count": object_count,
                    "total_size_bytes": total_size,
                    "connection_info": self.get_connection_info()
                }
                
            except ClientError as e:
                return {
                    "status": "error",
                    "error": f"Cannot access bucket contents: {str(e)}"
                }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get available S3 objects/prefixes."""
        try:
            if not self.s3_client:
                await self.connect()
            
            schema_info = []
            prefix = self.connection_config.get('prefix', '')
            
            # List objects with the specified prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            file_types = {}
            
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    size = obj['Size']
                    
                    # Group by file extension
                    if '.' in key:
                        ext = key.split('.')[-1].lower()
                    else:
                        ext = 'no_extension'
                    
                    if ext not in file_types:
                        file_types[ext] = {
                            'count': 0,
                            'total_size': 0,
                            'sample_files': []
                        }
                    
                    file_types[ext]['count'] += 1
                    file_types[ext]['total_size'] += size
                    
                    if len(file_types[ext]['sample_files']) < 5:
                        file_types[ext]['sample_files'].append(key)
            
            # Create schema info for each file type
            for ext, info in file_types.items():
                schema_info.append({
                    'name': f"{ext}_files",
                    'type': 's3_objects',
                    'row_count': info['count'],  # Number of files
                    'column_count': 1,  # Files themselves
                    'description': f"{info['count']} {ext} files totaling {info['total_size']} bytes"
                })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get S3 schema: {str(e)}")
            return []
    
    async def preview_data(self, source_name: str, limit: int = 100) -> Dict[str, Any]:
        """Preview data from S3 objects."""
        try:
            if not self.s3_client:
                await self.connect()
            
            # For S3, source_name should be a specific object key or pattern
            object_key = self.connection_config.get('object_key', source_name)
            
            if not object_key:
                # If no specific object, list some objects
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    MaxKeys=limit
                )
                
                objects_data = []
                for obj in response.get('Contents', []):
                    objects_data.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'storage_class': obj.get('StorageClass', 'STANDARD')
                    })
                
                return {
                    'status': 'success',
                    'columns': [
                        {'name': 'key', 'sql_type': 'TEXT'},
                        {'name': 'size', 'sql_type': 'INTEGER'},
                        {'name': 'last_modified', 'sql_type': 'TIMESTAMP'},
                        {'name': 'storage_class', 'sql_type': 'TEXT'}
                    ],
                    'sample_data': objects_data[:limit],
                    'row_count': len(objects_data)
                }
            
            # Try to read and preview specific object content
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=object_key)
                content = response['Body'].read()
                
                # Try to determine file type and parse accordingly
                df = self._parse_s3_object_content(object_key, content, limit)
                
                if df.empty:
                    return {
                        'status': 'error',
                        'error': 'Unable to parse object content or empty file',
                        'columns': [],
                        'sample_data': [],
                        'row_count': 0
                    }
                
                # Get column information
                columns_info = []
                for col in df.columns:
                    columns_info.append({
                        'name': col,
                        'sql_type': self._infer_sql_type(df[col])
                    })
                
                return {
                    'status': 'success',
                    'columns': columns_info,
                    'sample_data': df.to_dict(orient='records'),
                    'row_count': len(df)
                }
                
            except ClientError as e:
                return {
                    'status': 'error',
                    'error': f'Cannot read object {object_key}: {str(e)}',
                    'columns': [],
                    'sample_data': [],
                    'row_count': 0
                }
            
        except Exception as e:
            logger.error(f"Failed to preview S3 data: {str(e)}")
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
        """Extract data from S3 objects"""
        try:
            if not self.s3_client:
                await self.connect()
            
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            # Get list of objects to process
            objects_to_process = await self._get_objects_list(source)
            
            for obj_key in objects_to_process:
                try:
                    # Read object content
                    response = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    content = response['Body'].read()
                    
                    # Parse content based on file type
                    df = self._parse_s3_object_content(obj_key, content)
                    
                    if df.empty:
                        continue
                    
                    # Apply filters if specified
                    if config.filters:
                        df = self._apply_filters(df, config.filters)
                    
                    # Select specific columns if specified
                    if config.columns:
                        available_columns = [col for col in config.columns if col in df.columns]
                        if available_columns:
                            df = df[available_columns]
                    
                    # Yield data in chunks
                    for i in range(0, len(df), chunk_size):
                        chunk = df.iloc[i:i + chunk_size].copy()
                        # Add metadata about source object
                        chunk['_s3_source_key'] = obj_key
                        yield chunk
                        
                except Exception as e:
                    logger.error(f"Failed to process S3 object {obj_key}: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"Failed to extract S3 data: {str(e)}")
            raise
    
    async def supports_incremental_extraction(self) -> bool:
        """S3 objects can support incremental extraction based on LastModified."""
        return True
    
    async def supports_real_time_sync(self) -> bool:
        """S3 doesn't support real-time sync, but can be combined with S3 events."""
        return False
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """S3 objects can use LastModified for incremental extraction."""
        return ['LastModified', '_s3_last_modified']
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get record count - for S3 this could be object count or total records in objects."""
        try:
            objects_list = await self._get_objects_list(source)
            
            if not filters:
                return len(objects_list)
            
            # For filtered count, would need to read all objects (expensive)
            total_count = 0
            for obj_key in objects_list:
                try:
                    response = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    content = response['Body'].read()
                    df = self._parse_s3_object_content(obj_key, content)
                    df = self._apply_filters(df, filters)
                    total_count += len(df)
                except Exception:
                    continue
            
            return total_count
            
        except Exception as e:
            logger.error(f"Failed to get record count: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for S3"""
        return ['bucket_name']
    
    async def _get_objects_list(self, pattern: str) -> List[str]:
        """Get list of S3 objects matching pattern"""
        objects = []
        prefix = self.connection_config.get('prefix', '')
        
        if pattern and pattern != prefix:
            # Use pattern as additional prefix
            full_prefix = f"{prefix}{pattern}" if prefix else pattern
        else:
            full_prefix = prefix
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=full_prefix)
        
        for page in page_iterator:
            for obj in page.get('Contents', []):
                objects.append(obj['Key'])
        
        return objects
    
    def _parse_s3_object_content(self, object_key: str, content: bytes, limit: Optional[int] = None) -> pd.DataFrame:
        """Parse S3 object content based on file extension"""
        try:
            # Determine file type from key
            if '.' in object_key:
                ext = object_key.split('.')[-1].lower()
            else:
                ext = 'txt'
            
            # Convert bytes to string for text formats
            if ext in ['csv', 'tsv', 'txt', 'json', 'jsonl']:
                content_str = content.decode('utf-8')
            
            if ext == 'csv':
                df = pd.read_csv(io.StringIO(content_str))
            elif ext == 'tsv':
                df = pd.read_csv(io.StringIO(content_str), sep='\t')
            elif ext == 'json':
                data = json.loads(content_str)
                if isinstance(data, list):
                    df = pd.json_normalize(data)
                else:
                    df = pd.json_normalize([data])
            elif ext == 'jsonl':
                # JSON Lines format
                lines = content_str.strip().split('\n')
                data = [json.loads(line) for line in lines if line.strip()]
                df = pd.json_normalize(data)
            elif ext in ['parquet', 'pqt']:
                # For parquet files in S3
                try:
                    import pyarrow.parquet as pq
                    table = pq.read_table(io.BytesIO(content))
                    df = table.to_pandas()
                except ImportError:
                    logger.error("PyArrow required for Parquet files")
                    return pd.DataFrame()
            else:
                # Fallback: treat as text file
                lines = content_str.split('\n')
                df = pd.DataFrame([{'line_number': i+1, 'content': line} 
                                  for i, line in enumerate(lines) if line.strip()])
            
            if limit:
                df = df.head(limit)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse S3 object {object_key}: {str(e)}")
            return pd.DataFrame()
    
    def _infer_sql_type(self, series: pd.Series) -> str:
        """Infer SQL type from pandas series"""
        if series.dtype == 'object':
            return 'TEXT'
        elif series.dtype in ['int64', 'int32']:
            return 'INTEGER'
        elif series.dtype in ['float64', 'float32']:
            return 'REAL'
        elif series.dtype == 'bool':
            return 'BOOLEAN'
        elif 'datetime' in str(series.dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
    
    def _apply_filters(self, df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """Apply basic filters to DataFrame"""
        for column, value in filters.items():
            if column in df.columns:
                df = df[df[column] == value]
        return df