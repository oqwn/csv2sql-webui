from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Iterator, Tuple
import pandas as pd
from datetime import datetime


class DataSourceConnector(ABC):
    """Base class for all data source connectors"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.connection_config = connection_config
        self.connection = None
        
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the data source"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """Close connection to the data source"""
        pass
    
    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection and return status with metadata"""
        pass
    
    @abstractmethod
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get schema information (tables, collections, etc.)"""
        pass
    
    @abstractmethod
    async def extract_data(
        self,
        source: str,
        extraction_config: Dict[str, Any],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """Extract data from the source"""
        pass
    
    @abstractmethod
    async def get_record_count(self, source: str, filters: Optional[Dict] = None) -> int:
        """Get total record count for planning extractions"""
        pass
    
    async def validate_config(self) -> List[str]:
        """Validate connection configuration and return any errors"""
        errors = []
        required_fields = self.get_required_config_fields()
        
        for field in required_fields:
            if field not in self.connection_config:
                errors.append(f"Missing required field: {field}")
            elif not self.connection_config[field]:
                errors.append(f"Empty required field: {field}")
                
        return errors
    
    @abstractmethod
    def get_required_config_fields(self) -> List[str]:
        """Return list of required configuration fields"""
        pass
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get sanitized connection info (without passwords)"""
        safe_config = self.connection_config.copy()
        sensitive_fields = ['password', 'api_key', 'secret', 'token']
        
        for field in sensitive_fields:
            if field in safe_config:
                safe_config[field] = "***"
                
        return safe_config
    
    async def supports_incremental_extraction(self) -> bool:
        """Check if this connector supports incremental extraction"""
        return False
    
    async def supports_real_time_sync(self) -> bool:
        """Check if this connector supports real-time synchronization"""
        return False
    
    async def get_incremental_key_columns(self, source: str) -> List[str]:
        """Get available columns that can be used for incremental extraction"""
        return []


class ExtractionConfig:
    """Configuration for data extraction operations"""
    
    def __init__(
        self,
        mode: str = "full",
        chunk_size: int = 10000,
        incremental_column: Optional[str] = None,
        last_value: Optional[Any] = None,
        filters: Optional[Dict] = None,
        columns: Optional[List[str]] = None,
        order_by: Optional[str] = None
    ):
        self.mode = mode
        self.chunk_size = chunk_size
        self.incremental_column = incremental_column
        self.last_value = last_value
        self.filters = filters or {}
        self.columns = columns
        self.order_by = order_by
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "chunk_size": self.chunk_size,
            "incremental_column": self.incremental_column,
            "last_value": self.last_value,
            "filters": self.filters,
            "columns": self.columns,
            "order_by": self.order_by
        }
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "ExtractionConfig":
        return cls(**config)