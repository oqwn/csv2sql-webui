"""MySQL binlog connector for real-time data synchronization."""

import pandas as pd
from typing import Dict, Any, List, Optional, Callable
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)

try:
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )
    from pymysqlreplication.event import RotateEvent, FormatDescriptionEvent
    BINLOG_AVAILABLE = True
except ImportError:
    BINLOG_AVAILABLE = False


class MySQLBinlogConnector:
    """MySQL binlog connector for real-time sync capabilities."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize MySQL binlog connector."""
        self.connection_config = connection_config
        self.stream = None
        self.is_running = False
        
        if not BINLOG_AVAILABLE:
            raise ImportError(
                "pymysqlreplication is required for MySQL binlog. "
                "Install with: pip install mysql-replication"
            )
    
    async def start_real_time_sync(
        self,
        tables: List[str],
        callback: Callable,
        server_id: Optional[int] = None,
        log_file: Optional[str] = None,
        log_pos: Optional[int] = None
    ) -> Dict[str, Any]:
        """Start real-time sync using MySQL binlog."""
        try:
            # Configure binlog stream
            mysql_settings = {
                'host': self.connection_config['host'],
                'port': self.connection_config.get('port', 3306),
                'user': self.connection_config['username'],
                'passwd': self.connection_config['password'],
                'charset': 'utf8mb4'
            }
            
            stream_config = {
                **mysql_settings,
                'server_id': server_id or 100,  # Unique server ID for replication
                'only_events': [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                'only_tables': tables,
                'blocking': True,
                'resume_stream': log_file and log_pos,
                'log_file': log_file,
                'log_pos': log_pos,
                'fail_on_table_metadata_unavailable': True,
                'slave_heartbeat': 30
            }
            
            self.stream = BinLogStreamReader(**stream_config)
            self.is_running = True
            
            logger.info(f"Started MySQL binlog sync for tables: {tables}")
            
            # Process binlog events
            for binlog_event in self.stream:
                if not self.is_running:
                    break
                
                try:
                    # Skip non-data events
                    if isinstance(binlog_event, (RotateEvent, FormatDescriptionEvent)):
                        continue
                    
                    # Process data change events
                    if isinstance(binlog_event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                        changes = self._process_binlog_event(binlog_event)
                        
                        for change in changes:
                            df = pd.DataFrame([change])
                            await callback(df, change['_change_operation'])
                            
                except Exception as e:
                    logger.error(f"Error processing binlog event: {str(e)}")
                    continue
            
            return {
                'log_file': self.stream.log_file,
                'log_pos': self.stream.log_pos
            }
            
        except Exception as e:
            logger.error(f"Failed to start MySQL binlog sync: {str(e)}")
            raise
    
    def _process_binlog_event(self, binlog_event) -> List[Dict[str, Any]]:
        """Process a binlog event into DataFrame-compatible format."""
        changes = []
        
        try:
            table_name = f"{binlog_event.schema}.{binlog_event.table}"
            timestamp = datetime.fromtimestamp(binlog_event.timestamp)
            
            if isinstance(binlog_event, WriteRowsEvent):
                # INSERT events
                for row in binlog_event.rows:
                    change_data = dict(row['values'])
                    change_data.update({
                        '_change_operation': 'insert',
                        '_change_timestamp': timestamp,
                        '_table_name': table_name,
                        '_log_file': self.stream.log_file,
                        '_log_pos': self.stream.log_pos
                    })
                    changes.append(change_data)
                    
            elif isinstance(binlog_event, UpdateRowsEvent):
                # UPDATE events
                for row in binlog_event.rows:
                    # Combine old and new values
                    change_data = dict(row['after_values'])
                    change_data.update({
                        '_change_operation': 'update',
                        '_change_timestamp': timestamp,
                        '_table_name': table_name,
                        '_log_file': self.stream.log_file,
                        '_log_pos': self.stream.log_pos,
                        '_before_values': json.dumps(row['before_values'], default=str)
                    })
                    changes.append(change_data)
                    
            elif isinstance(binlog_event, DeleteRowsEvent):
                # DELETE events
                for row in binlog_event.rows:
                    change_data = dict(row['values'])
                    change_data.update({
                        '_change_operation': 'delete',
                        '_change_timestamp': timestamp,
                        '_table_name': table_name,
                        '_log_file': self.stream.log_file,
                        '_log_pos': self.stream.log_pos
                    })
                    changes.append(change_data)
            
            return changes
            
        except Exception as e:
            logger.error(f"Error processing binlog event: {str(e)}")
            return []
    
    async def get_current_binlog_position(self) -> Dict[str, Any]:
        """Get current MySQL binlog position."""
        try:
            import pymysql
            
            connection = pymysql.connect(
                host=self.connection_config['host'],
                port=self.connection_config.get('port', 3306),
                user=self.connection_config['username'],
                password=self.connection_config['password'],
                charset='utf8mb4'
            )
            
            with connection.cursor() as cursor:
                cursor.execute("SHOW MASTER STATUS")
                result = cursor.fetchone()
                
                if result:
                    return {
                        'log_file': result[0],
                        'log_pos': result[1]
                    }
                else:
                    raise Exception("Could not get master status")
                    
            connection.close()
            
        except Exception as e:
            logger.error(f"Failed to get binlog position: {str(e)}")
            raise
    
    async def validate_binlog_configuration(self) -> Dict[str, Any]:
        """Validate MySQL binlog configuration for real-time sync."""
        try:
            import pymysql
            
            connection = pymysql.connect(
                host=self.connection_config['host'],
                port=self.connection_config.get('port', 3306),
                user=self.connection_config['username'],
                password=self.connection_config['password'],
                charset='utf8mb4'
            )
            
            issues = []
            
            with connection.cursor() as cursor:
                # Check if binlog is enabled
                cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
                log_bin = cursor.fetchone()
                if not log_bin or log_bin[1].lower() != 'on':
                    issues.append("Binary logging is not enabled")
                
                # Check binlog format
                cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
                binlog_format = cursor.fetchone()
                if binlog_format and binlog_format[1].upper() not in ['ROW', 'MIXED']:
                    issues.append(f"Binlog format is {binlog_format[1]}, should be ROW or MIXED")
                
                # Check server ID
                cursor.execute("SHOW VARIABLES LIKE 'server_id'")
                server_id = cursor.fetchone()
                if not server_id or int(server_id[1]) == 0:
                    issues.append("Server ID is 0, should be a unique positive integer")
                
                # Check user privileges
                cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
                grants = [row[0] for row in cursor.fetchall()]
                has_replication = any('REPLICATION SLAVE' in grant or 'ALL PRIVILEGES' in grant for grant in grants)
                if not has_replication:
                    issues.append("User does not have REPLICATION SLAVE privilege")
            
            connection.close()
            
            return {
                'valid': len(issues) == 0,
                'issues': issues
            }
            
        except Exception as e:
            logger.error(f"Failed to validate binlog configuration: {str(e)}")
            return {
                'valid': False,
                'issues': [f"Configuration check failed: {str(e)}"]
            }
    
    async def stop_real_time_sync(self):
        """Stop real-time synchronization."""
        self.is_running = False
        if self.stream:
            self.stream.close()
            self.stream = None
        logger.info("Stopped MySQL binlog sync")
    
    def get_supported_tables(self) -> List[str]:
        """Get list of tables that support binlog replication."""
        try:
            import pymysql
            
            connection = pymysql.connect(
                host=self.connection_config['host'],
                port=self.connection_config.get('port', 3306),
                user=self.connection_config['username'],
                password=self.connection_config['password'],
                database=self.connection_config.get('database'),
                charset='utf8mb4'
            )
            
            tables = []
            with connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                for (table_name,) in cursor.fetchall():
                    # Check if table uses a supported storage engine
                    cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                    create_table = cursor.fetchone()[1]
                    
                    # InnoDB and MyISAM support binlog
                    if 'ENGINE=InnoDB' in create_table or 'ENGINE=MyISAM' in create_table:
                        tables.append(table_name)
            
            connection.close()
            return tables
            
        except Exception as e:
            logger.error(f"Failed to get supported tables: {str(e)}")
            return []