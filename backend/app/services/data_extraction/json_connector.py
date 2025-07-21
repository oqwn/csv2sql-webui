import pandas as pd
import json
import requests
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class JSONConnector(DataSourceConnector):
    """Connector for JSON data sources (files, URLs, or raw JSON data)"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.data_cache = None
        
    async def connect(self) -> bool:
        """Test connection to JSON source."""
        try:
            source_type = self.connection_config.get('source_type', 'file')
            
            if source_type == 'file':
                file_path = self.connection_config.get('file_path')
                if not file_path or not Path(file_path).exists():
                    logger.error(f"JSON file not found: {file_path}")
                    return False
                    
                with open(file_path, 'r', encoding='utf-8') as f:
                    json.load(f)  # Validate JSON syntax
                    
            elif source_type == 'url':
                url = self.connection_config.get('url')
                headers = self.connection_config.get('headers', {})
                
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                response.json()  # Validate JSON response
                
            elif source_type == 'raw':
                raw_json = self.connection_config.get('raw_data')
                if not raw_json:
                    return False
                json.loads(raw_json)  # Validate JSON syntax
            
            logger.info(f"Successfully connected to JSON source: {source_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to JSON source: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Clean up JSON connection"""
        self.data_cache = None
        return True
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test JSON source connection and return metadata"""
        try:
            if not await self.connect():
                return {
                    "status": "error",
                    "error": "Failed to connect to JSON source"
                }
            
            # Try to load and analyze JSON structure
            data = await self._load_json_data()
            
            if isinstance(data, list):
                record_count = len(data)
                sample_record = data[0] if data else {}
            elif isinstance(data, dict):
                record_count = 1
                sample_record = data
            else:
                record_count = 1
                sample_record = {"value": data}
            
            return {
                "status": "success",
                "database_type": "json",
                "record_count": record_count,
                "data_type": type(data).__name__,
                "sample_keys": list(sample_record.keys()) if isinstance(sample_record, dict) else [],
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get available JSON data structures."""
        try:
            data = await self._load_json_data()
            schema_info = []
            
            source_type = self.connection_config.get('source_type', 'file')
            source_name = self._get_source_name()
            
            if isinstance(data, list) and data:
                # Array of objects
                sample_record = data[0] if isinstance(data[0], dict) else {}
                columns = list(sample_record.keys()) if sample_record else []
                
                schema_info.append({
                    'name': source_name,
                    'type': 'json_array',
                    'row_count': len(data),
                    'column_count': len(columns),
                    'description': f'JSON array with {len(data)} records and {len(columns)} fields'
                })
                
            elif isinstance(data, dict):
                # Single object or nested structure
                def count_nested_objects(obj, path=""):
                    count = 0
                    for key, value in obj.items():
                        current_path = f"{path}.{key}" if path else key
                        if isinstance(value, dict):
                            count += count_nested_objects(value, current_path)
                        elif isinstance(value, list) and value and isinstance(value[0], dict):
                            schema_info.append({
                                'name': current_path,
                                'type': 'json_nested_array',
                                'row_count': len(value),
                                'column_count': len(value[0].keys()) if value[0] else 0,
                                'description': f'Nested array at {current_path} with {len(value)} records'
                            })
                        count += 1
                    return count
                
                field_count = count_nested_objects(data)
                schema_info.append({
                    'name': source_name,
                    'type': 'json_object',
                    'row_count': 1,
                    'column_count': field_count,
                    'description': f'JSON object with {field_count} fields'
                })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get JSON schema: {str(e)}")
            return []
    
    async def preview_data(self, source_name: str, limit: int = 100) -> Dict[str, Any]:
        """Preview data from JSON source."""
        try:
            data = await self._load_json_data()
            
            # Normalize data to DataFrame
            df = self._normalize_to_dataframe(data, source_name, limit)
            
            if df.empty:
                return {
                    'status': 'error',
                    'error': 'No data found or unable to normalize JSON',
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
            
            # Convert to records for JSON serialization
            sample_data = df.head(limit).to_dict(orient='records')
            
            return {
                'status': 'success',
                'columns': columns_info,
                'sample_data': sample_data,
                'row_count': len(df)
            }
            
        except Exception as e:
            logger.error(f"Failed to preview JSON data: {str(e)}")
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
        """Extract data from JSON source"""
        try:
            data = await self._load_json_data()
            config = ExtractionConfig.from_dict(extraction_config)
            chunk_size = chunk_size or config.chunk_size
            
            # Normalize to DataFrame
            df = self._normalize_to_dataframe(data, source)
            
            if df.empty:
                return
            
            # Apply filtering if specified
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
                yield chunk
            
        except Exception as e:
            logger.error(f"Failed to extract JSON data: {str(e)}")
            raise
    
    async def supports_incremental_extraction(self) -> bool:
        """JSON sources don't typically support incremental extraction."""
        return False
    
    async def supports_real_time_sync(self) -> bool:
        """JSON sources don't support real-time sync."""
        return False
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get fields suitable for incremental extraction."""
        # For JSON, we could look for timestamp fields
        try:
            data = await self._load_json_data()
            df = self._normalize_to_dataframe(data, source)
            
            timestamp_fields = []
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['time', 'date', 'created', 'updated', 'timestamp']):
                    timestamp_fields.append(col)
            
            return timestamp_fields
            
        except Exception as e:
            logger.error(f"Failed to get incremental fields: {str(e)}")
            return []
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get record count for JSON source"""
        try:
            data = await self._load_json_data()
            df = self._normalize_to_dataframe(data, source)
            
            if filters:
                df = self._apply_filters(df, filters)
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to get record count: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for JSON"""
        return ['source_type']  # file, url, or raw
    
    async def _load_json_data(self) -> Any:
        """Load JSON data from source"""
        if self.data_cache is not None:
            return self.data_cache
        
        source_type = self.connection_config.get('source_type', 'file')
        
        if source_type == 'file':
            file_path = self.connection_config.get('file_path')
            encoding = self.connection_config.get('encoding', 'utf-8')
            
            with open(file_path, 'r', encoding=encoding) as f:
                self.data_cache = json.load(f)
                
        elif source_type == 'url':
            url = self.connection_config.get('url')
            headers = self.connection_config.get('headers', {})
            auth = self.connection_config.get('auth')
            
            response = requests.get(url, headers=headers, auth=auth, timeout=30)
            response.raise_for_status()
            self.data_cache = response.json()
            
        elif source_type == 'raw':
            raw_data = self.connection_config.get('raw_data')
            self.data_cache = json.loads(raw_data)
        
        return self.data_cache
    
    def _normalize_to_dataframe(self, data: Any, source_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Convert JSON data to pandas DataFrame"""
        try:
            if isinstance(data, list):
                # Array of objects
                df = pd.json_normalize(data)
            elif isinstance(data, dict):
                # Check if we're accessing a nested path
                if '.' in source_name:
                    # Navigate to nested data
                    nested_data = data
                    for key in source_name.split('.')[1:]:  # Skip root name
                        if isinstance(nested_data, dict) and key in nested_data:
                            nested_data = nested_data[key]
                        else:
                            break
                    
                    if isinstance(nested_data, list):
                        df = pd.json_normalize(nested_data)
                    else:
                        df = pd.json_normalize([nested_data])
                else:
                    # Flatten single object
                    df = pd.json_normalize([data])
            else:
                # Primitive value
                df = pd.DataFrame([{'value': data}])
            
            if limit:
                df = df.head(limit)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to normalize JSON to DataFrame: {str(e)}")
            return pd.DataFrame()
    
    def _get_source_name(self) -> str:
        """Get a friendly name for the JSON source"""
        source_type = self.connection_config.get('source_type', 'json')
        
        if source_type == 'file':
            file_path = self.connection_config.get('file_path', 'unknown.json')
            return Path(file_path).stem
        elif source_type == 'url':
            url = self.connection_config.get('url', 'unknown_url')
            return url.split('/')[-1] or 'json_api'
        else:
            return 'json_data'
    
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