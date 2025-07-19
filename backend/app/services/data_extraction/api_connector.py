import pandas as pd
from typing import Dict, Any, List, Optional, Iterator
from .base_connector import DataSourceConnector, ExtractionConfig
import logging
import json
import asyncio
from urllib.parse import urljoin, urlencode

logger = logging.getLogger(__name__)

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class APIConnector(DataSourceConnector):
    """Connector for REST API data sources"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.client = None
        
        if not HTTPX_AVAILABLE:
            raise ImportError("httpx is required for API connections. Install with: pip install httpx")
    
    async def connect(self) -> bool:
        """Establish HTTP client for API connections"""
        try:
            # Setup HTTP client with authentication
            headers = {"Content-Type": "application/json"}
            
            # Add authentication headers
            auth_type = self.connection_config.get('auth_type', 'none')
            if auth_type == 'bearer':
                headers['Authorization'] = f"Bearer {self.connection_config['token']}"
            elif auth_type == 'api_key':
                api_key_header = self.connection_config.get('api_key_header', 'X-API-Key')
                headers[api_key_header] = self.connection_config['api_key']
            
            # Create HTTP client
            self.client = httpx.AsyncClient(
                headers=headers,
                timeout=30.0,
                verify=self.connection_config.get('verify_ssl', True)
            )
            
            logger.info("Successfully created API client")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create API client: {str(e)}")
            return False
    
    async def disconnect(self) -> bool:
        """Close HTTP client"""
        try:
            if self.client:
                await self.client.aclose()
                self.client = None
            return True
        except Exception as e:
            logger.error(f"Failed to close API client: {str(e)}")
            return False
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test API connection"""
        try:
            if not self.client:
                await self.connect()
            
            # Use health endpoint if provided, otherwise use base URL
            test_url = self.connection_config.get('health_endpoint', self.connection_config['base_url'])
            
            response = await self.client.get(test_url)
            
            return {
                "status": "success" if response.status_code < 400 else "error",
                "status_code": response.status_code,
                "response_time": response.elapsed.total_seconds() if response.elapsed else 0,
                "base_url": self.connection_config['base_url'],
                "connection_info": self.get_connection_info()
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get available API endpoints"""
        try:
            schema_info = []
            
            # If OpenAPI/Swagger spec is available
            openapi_url = self.connection_config.get('openapi_url')
            if openapi_url:
                schema_info = await self._get_openapi_schema(openapi_url)
            else:
                # Use manually configured endpoints
                endpoints = self.connection_config.get('endpoints', [])
                for endpoint in endpoints:
                    schema_info.append({
                        "name": endpoint.get('name', endpoint['path']),
                        "type": "endpoint",
                        "path": endpoint['path'],
                        "method": endpoint.get('method', 'GET'),
                        "description": endpoint.get('description', ''),
                        "parameters": endpoint.get('parameters', [])
                    })
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to get API schema info: {str(e)}")
            return []
    
    async def extract_data(
        self,
        source: str,
        extraction_config: Dict[str, Any],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data from API endpoint"""
        try:
            if not self.client:
                await self.connect()
                
            config = ExtractionConfig.from_dict(extraction_config)
            
            # Handle pagination for chunked extraction
            if config.mode == "chunked" or chunk_size:
                async for chunk in self._extract_paginated_data(source, config, chunk_size):
                    yield chunk
            else:
                # Single request extraction
                data = await self._extract_single_request(source, config)
                if not data.empty:
                    yield data
                    
        except Exception as e:
            logger.error(f"Failed to extract data from API endpoint {source}: {str(e)}")
            raise
    
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get record count from API (if supported)"""
        try:
            if not self.client:
                await self.connect()
                
            # Try to get count from a dedicated count endpoint
            count_url = f"{source}/count"
            try:
                response = await self.client.get(count_url)
                if response.status_code == 200:
                    count_data = response.json()
                    if isinstance(count_data, dict) and 'count' in count_data:
                        return count_data['count']
                    elif isinstance(count_data, int):
                        return count_data
            except:
                pass
            
            # Fallback: make a request and count results
            response = await self.client.get(source)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    return len(data)
                elif isinstance(data, dict):
                    # Look for common pagination fields
                    for key in ['total', 'count', 'total_count']:
                        if key in data:
                            return data[key]
                    # If data is nested, count the main array
                    for value in data.values():
                        if isinstance(value, list):
                            return len(value)
            
            return 0
                
        except Exception as e:
            logger.error(f"Failed to get record count: {str(e)}")
            return 0
    
    def get_required_config_fields(self) -> List[str]:
        """Return required configuration fields for API"""
        return ['base_url']
    
    async def supports_incremental_extraction(self) -> bool:
        """API can support incremental extraction if properly configured"""
        return True
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get fields suitable for incremental extraction"""
        # These would typically be configured per endpoint
        return ['id', 'created_at', 'updated_at', 'timestamp']
    
    async def _extract_single_request(self, endpoint: str, config: ExtractionConfig) -> pd.DataFrame:
        """Extract data from a single API request"""
        # Build URL with query parameters
        url = urljoin(self.connection_config['base_url'], endpoint)
        
        params = {}
        if config.filters:
            params.update(config.filters)
            
        if config.incremental_column and config.last_value:
            params[f"{config.incremental_column}_after"] = config.last_value
        
        if params:
            url += "?" + urlencode(params)
        
        response = await self.client.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        # Convert to DataFrame
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            # Look for data in common response wrappers
            for key in ['data', 'results', 'items', 'records']:
                if key in data and isinstance(data[key], list):
                    return pd.DataFrame(data[key])
            # If it's a single object, wrap in list
            return pd.DataFrame([data])
        else:
            return pd.DataFrame()
    
    async def _extract_paginated_data(
        self, 
        endpoint: str, 
        config: ExtractionConfig, 
        chunk_size: Optional[int]
    ) -> Iterator[pd.DataFrame]:
        """Extract data with pagination support"""
        page = 1
        page_size = chunk_size or config.chunk_size
        
        while True:
            # Build URL with pagination parameters
            url = urljoin(self.connection_config['base_url'], endpoint)
            
            params = {
                'page': page,
                'limit': page_size
            }
            
            if config.filters:
                params.update(config.filters)
                
            if config.incremental_column and config.last_value:
                params[f"{config.incremental_column}_after"] = config.last_value
            
            url += "?" + urlencode(params)
            
            response = await self.client.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract data array
            data_array = []
            if isinstance(data, list):
                data_array = data
            elif isinstance(data, dict):
                for key in ['data', 'results', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        data_array = data[key]
                        break
            
            if not data_array:
                break
                
            yield pd.DataFrame(data_array)
            
            # Check if there are more pages
            if len(data_array) < page_size:
                break
                
            page += 1
    
    async def _get_openapi_schema(self, openapi_url: str) -> List[Dict[str, Any]]:
        """Parse OpenAPI specification to get available endpoints"""
        try:
            response = await self.client.get(openapi_url)
            response.raise_for_status()
            
            spec = response.json()
            endpoints = []
            
            paths = spec.get('paths', {})
            for path, methods in paths.items():
                for method, details in methods.items():
                    if method.upper() == 'GET':  # Only GET endpoints for data extraction
                        endpoints.append({
                            "name": details.get('operationId', f"{method}_{path}"),
                            "type": "endpoint",
                            "path": path,
                            "method": method.upper(),
                            "description": details.get('summary', ''),
                            "parameters": [
                                {
                                    "name": param.get('name'),
                                    "type": param.get('schema', {}).get('type', 'string'),
                                    "required": param.get('required', False),
                                    "description": param.get('description', '')
                                }
                                for param in details.get('parameters', [])
                            ]
                        })
            
            return endpoints
            
        except Exception as e:
            logger.error(f"Failed to parse OpenAPI schema: {str(e)}")
            return []