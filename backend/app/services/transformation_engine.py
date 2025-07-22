import pandas as pd
import numpy as np
import re
import ast
import json
from typing import List, Dict, Any, Optional, Tuple, TYPE_CHECKING
from datetime import datetime
import sqlparse

from app.models.transformation_types import (
    TransformationType, TransformationStep, FilterRule, FilterOperator,
    AggregationConfig, ColumnSplitConfig, ColumnMergeConfig, 
    TypeConversionConfig, CleaningRule
)

if TYPE_CHECKING:
    from app.services.transaction_manager import TransactionContext, TransactionManager


class TransformationEngine:
    """Engine for applying data transformations"""
    
    def __init__(self, transaction_manager: Optional['TransactionManager'] = None):
        self.transaction_manager = transaction_manager
        self.supported_transformations = {
            TransformationType.FILTER: self._apply_filter,
            TransformationType.CLEAN: self._apply_cleaning,
            TransformationType.AGGREGATE: self._apply_aggregation,
            TransformationType.SPLIT_COLUMN: self._apply_column_split,
            TransformationType.MERGE_COLUMN: self._apply_column_merge,
            TransformationType.TYPE_CONVERSION: self._apply_type_conversion,
            TransformationType.RENAME: self._apply_rename,
            TransformationType.DROP: self._apply_drop,
            TransformationType.FILL_NULL: self._apply_fill_null,
            TransformationType.CUSTOM_SQL: self._apply_custom_sql,
            TransformationType.CUSTOM_PYTHON: self._apply_custom_python,
        }
    
    async def apply_transformations(self, df: pd.DataFrame, steps: List[TransformationStep], 
                                   transaction_context: Optional['TransactionContext'] = None) -> pd.DataFrame:
        """Apply a series of transformation steps to a DataFrame"""
        result_df = df.copy()
        
        for i, step in enumerate(steps):
            if step.type not in self.supported_transformations:
                raise ValueError(f"Unsupported transformation type: {step.type}")
            
            # Create checkpoint before each transformation step
            if transaction_context and self.transaction_manager:
                checkpoint_id = self.transaction_manager.create_checkpoint(
                    transaction_context.transaction_id,
                    f"Step_{i}_{step.name or step.type.value}",
                    result_df
                )
                
                self.transaction_manager._log_operation(
                    transaction_context.transaction_id,
                    "TRANSFORMATION_STEP",
                    "started",
                    f"Starting transformation step: {step.name or step.type.value}",
                    metadata={"step_index": i, "step_type": step.type.value}
                )
            
            try:
                transform_func = self.supported_transformations[step.type]
                result_df = await transform_func(result_df, step.config, transaction_context)
                
                # Validate result
                if result_df is None or result_df.empty:
                    raise ValueError(f"Transformation '{step.name}' resulted in empty dataset")
                
                # Log successful completion
                if transaction_context and self.transaction_manager:
                    self.transaction_manager._log_operation(
                        transaction_context.transaction_id,
                        "TRANSFORMATION_STEP",
                        "completed",
                        f"Completed transformation step: {step.name or step.type.value}",
                        metadata={"step_index": i, "step_type": step.type.value, "result_rows": len(result_df)}
                    )
                    
            except Exception as e:
                # Handle errors and dirty data
                if transaction_context and self.transaction_manager:
                    # Log error
                    self.transaction_manager._log_operation(
                        transaction_context.transaction_id,
                        "TRANSFORMATION_STEP",
                        "failed",
                        f"Failed transformation step: {step.name or step.type.value}",
                        error=str(e),
                        metadata={"step_index": i, "step_type": step.type.value}
                    )
                    
                    # If we have checkpoint capability, we could rollback to previous checkpoint
                    # For now, we'll just re-raise the error
                    raise
                else:
                    raise
        
        return result_df
    
    async def _apply_filter(self, df: pd.DataFrame, config: Dict[str, Any], 
                           transaction_context: Optional['TransactionContext'] = None) -> pd.DataFrame:
        """Apply filtering rules to DataFrame"""
        result_df = df.copy()
        
        # Parse filter rules
        if 'rules' in config:
            rules = [FilterRule(**rule) for rule in config['rules']]
        else:
            # Single rule
            rules = [FilterRule(**config)]
        
        # Apply filters
        for rule in rules:
            if rule.column not in result_df.columns:
                raise ValueError(f"Column '{rule.column}' not found in data")
            
            col_data = result_df[rule.column]
            
            if rule.operator == FilterOperator.EQUALS:
                mask = col_data == rule.value
            elif rule.operator == FilterOperator.NOT_EQUALS:
                mask = col_data != rule.value
            elif rule.operator == FilterOperator.GREATER_THAN:
                mask = col_data > rule.value
            elif rule.operator == FilterOperator.LESS_THAN:
                mask = col_data < rule.value
            elif rule.operator == FilterOperator.GREATER_EQUAL:
                mask = col_data >= rule.value
            elif rule.operator == FilterOperator.LESS_EQUAL:
                mask = col_data <= rule.value
            elif rule.operator == FilterOperator.IN:
                mask = col_data.isin(rule.value)
            elif rule.operator == FilterOperator.NOT_IN:
                mask = ~col_data.isin(rule.value)
            elif rule.operator == FilterOperator.CONTAINS:
                mask = col_data.astype(str).str.contains(rule.value, case=not rule.case_sensitive, na=False)
            elif rule.operator == FilterOperator.NOT_CONTAINS:
                mask = ~col_data.astype(str).str.contains(rule.value, case=not rule.case_sensitive, na=False)
            elif rule.operator == FilterOperator.STARTS_WITH:
                mask = col_data.astype(str).str.startswith(rule.value, na=False)
            elif rule.operator == FilterOperator.ENDS_WITH:
                mask = col_data.astype(str).str.endswith(rule.value, na=False)
            elif rule.operator == FilterOperator.IS_NULL:
                mask = col_data.isna()
            elif rule.operator == FilterOperator.NOT_NULL:
                mask = col_data.notna()
            else:
                raise ValueError(f"Unknown filter operator: {rule.operator}")
            
            # Apply filter based on logical operator
            if config.get('logical_operator', 'AND') == 'AND':
                result_df = result_df[mask]
            else:  # OR
                if len(result_df) == len(df):  # First filter
                    result_df = result_df[mask]
                else:
                    result_df = pd.concat([result_df, df[mask]]).drop_duplicates()
        
        return result_df
    
    async def _apply_cleaning(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Apply cleaning rules to DataFrame"""
        result_df = df.copy()
        
        # Parse cleaning rules
        if 'rules' in config:
            rules = [CleaningRule(**rule) for rule in config['rules']]
        else:
            # Single rule
            rules = [CleaningRule(**config)]
        
        for rule in rules:
            if rule.column not in result_df.columns:
                raise ValueError(f"Column '{rule.column}' not found in data")
            
            col = rule.column
            
            if rule.rule_type == "trim":
                result_df[col] = result_df[col].astype(str).str.strip()
            
            elif rule.rule_type == "remove_special":
                pattern = rule.parameters.get('pattern', r'[^a-zA-Z0-9\s]') if rule.parameters else r'[^a-zA-Z0-9\s]'
                result_df[col] = result_df[col].astype(str).str.replace(pattern, '', regex=True)
            
            elif rule.rule_type == "lowercase":
                result_df[col] = result_df[col].astype(str).str.lower()
            
            elif rule.rule_type == "uppercase":
                result_df[col] = result_df[col].astype(str).str.upper()
            
            elif rule.rule_type == "remove_numbers":
                result_df[col] = result_df[col].astype(str).str.replace(r'\d+', '', regex=True)
            
            elif rule.rule_type == "remove_spaces":
                result_df[col] = result_df[col].astype(str).str.replace(r'\s+', '', regex=True)
            
            elif rule.rule_type == "normalize_whitespace":
                result_df[col] = result_df[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
            
            elif rule.rule_type == "remove_punctuation":
                result_df[col] = result_df[col].astype(str).str.replace(r'[^\w\s]', '', regex=True)
            
            elif rule.rule_type == "remove_html":
                result_df[col] = result_df[col].astype(str).str.replace(r'<[^>]+>', '', regex=True)
            
            elif rule.rule_type == "remove_urls":
                result_df[col] = result_df[col].astype(str).str.replace(
                    r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                    '', regex=True
                )
            
            elif rule.rule_type == "custom_regex":
                if rule.parameters and 'pattern' in rule.parameters:
                    replacement = rule.parameters.get('replacement', '')
                    result_df[col] = result_df[col].astype(str).str.replace(
                        rule.parameters['pattern'], replacement, regex=True
                    )
            
            else:
                raise ValueError(f"Unknown cleaning rule: {rule.rule_type}")
        
        return result_df
    
    async def _apply_aggregation(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Apply aggregation operations"""
        agg_config = AggregationConfig(**config)
        
        # Validate columns
        for col in agg_config.group_by:
            if col not in df.columns:
                raise ValueError(f"Group by column '{col}' not found in data")
        
        # Build aggregation dictionary
        agg_dict = {}
        for agg in agg_config.aggregations:
            col = agg['column']
            func = agg['function']
            alias = agg.get('alias', f"{col}_{func}")
            
            if col not in df.columns:
                raise ValueError(f"Aggregation column '{col}' not found in data")
            
            # Map to pandas aggregation functions
            if func == 'count_distinct':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='nunique')
            elif func == 'median':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='median')
            elif func == 'std':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='std')
            elif func == 'var':
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc='var')
            else:
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc=func)
        
        # Perform aggregation
        if agg_config.group_by:
            result_df = df.groupby(agg_config.group_by).agg(**agg_dict).reset_index()
        else:
            # Aggregate without grouping
            result_dict = {}
            for alias, named_agg in agg_dict.items():
                result_dict[alias] = [df[named_agg.column].agg(named_agg.aggfunc)]
            result_df = pd.DataFrame(result_dict)
        
        # Apply HAVING clause if specified
        if agg_config.having:
            for rule in agg_config.having:
                # Apply filter rules to aggregated data
                result_df = await self._apply_filter(result_df, rule.dict(), transaction_context)
        
        return result_df
    
    async def _apply_column_split(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Split a column into multiple columns"""
        split_config = ColumnSplitConfig(**config)
        result_df = df.copy()
        
        if split_config.column not in result_df.columns:
            raise ValueError(f"Column '{split_config.column}' not found in data")
        
        # Perform split
        if split_config.delimiter:
            # Split by delimiter
            split_data = result_df[split_config.column].astype(str).str.split(
                split_config.delimiter, expand=True, n=len(split_config.new_columns)-1
            )
        elif split_config.pattern:
            # Split by regex pattern
            split_data = result_df[split_config.column].astype(str).str.extract(
                split_config.pattern, expand=True
            )
        else:
            raise ValueError("Either delimiter or pattern must be specified for column split")
        
        # Assign to new columns
        for i, new_col in enumerate(split_config.new_columns):
            if i < split_data.shape[1]:
                result_df[new_col] = split_data[i]
            else:
                result_df[new_col] = None
        
        # Drop original column if requested
        if not split_config.keep_original:
            result_df = result_df.drop(columns=[split_config.column])
        
        return result_df
    
    async def _apply_column_merge(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Merge multiple columns into one"""
        merge_config = ColumnMergeConfig(**config)
        result_df = df.copy()
        
        # Validate columns
        for col in merge_config.columns:
            if col not in result_df.columns:
                raise ValueError(f"Column '{col}' not found in data")
        
        # Merge columns
        result_df[merge_config.new_column] = result_df[merge_config.columns].astype(str).apply(
            lambda x: merge_config.separator.join(x.dropna()), axis=1
        )
        
        # Drop original columns if requested
        if merge_config.drop_original:
            result_df = result_df.drop(columns=merge_config.columns)
        
        return result_df
    
    async def _apply_type_conversion(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Convert column data types"""
        conv_config = TypeConversionConfig(**config)
        result_df = df.copy()
        
        if conv_config.column not in result_df.columns:
            raise ValueError(f"Column '{conv_config.column}' not found in data")
        
        col = conv_config.column
        target_type = conv_config.target_type
        
        try:
            if target_type == "integer":
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('Int64')
            
            elif target_type == "float":
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
            
            elif target_type == "string":
                result_df[col] = result_df[col].astype(str)
            
            elif target_type == "date":
                result_df[col] = pd.to_datetime(
                    result_df[col], 
                    format=conv_config.format, 
                    errors='coerce'
                ).dt.date
            
            elif target_type == "datetime":
                result_df[col] = pd.to_datetime(
                    result_df[col], 
                    format=conv_config.format, 
                    errors='coerce'
                )
            
            elif target_type == "boolean":
                # Handle various boolean representations
                true_values = ['true', 'True', 'TRUE', '1', 't', 'T', 'yes', 'Yes', 'YES']
                result_df[col] = result_df[col].astype(str).isin(true_values)
            
            elif target_type == "json":
                def safe_json_parse(x):
                    try:
                        return json.loads(x) if pd.notna(x) else None
                    except:
                        return None
                result_df[col] = result_df[col].apply(safe_json_parse)
            
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
            
            # Handle default values for failed conversions
            if conv_config.default_value is not None:
                result_df[col] = result_df[col].fillna(conv_config.default_value)
            
        except Exception as e:
            raise ValueError(f"Type conversion failed: {str(e)}")
        
        return result_df
    
    async def _apply_rename(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Rename columns"""
        result_df = df.copy()
        rename_map = config.get('rename_map', {})
        
        # Validate columns
        for old_name in rename_map:
            if old_name not in result_df.columns:
                raise ValueError(f"Column '{old_name}' not found in data")
        
        result_df = result_df.rename(columns=rename_map)
        return result_df
    
    async def _apply_drop(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Drop columns"""
        result_df = df.copy()
        columns_to_drop = config.get('columns', [])
        
        # Validate columns
        for col in columns_to_drop:
            if col not in result_df.columns:
                raise ValueError(f"Column '{col}' not found in data")
        
        result_df = result_df.drop(columns=columns_to_drop)
        return result_df
    
    async def _apply_fill_null(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Fill null values"""
        result_df = df.copy()
        
        if 'column' in config:
            # Fill specific column
            column = config['column']
            if column not in result_df.columns:
                raise ValueError(f"Column '{column}' not found in data")
            
            method = config.get('method', 'value')
            
            if method == 'value':
                result_df[column] = result_df[column].fillna(config.get('value', ''))
            elif method == 'forward':
                result_df[column] = result_df[column].fillna(method='ffill')
            elif method == 'backward':
                result_df[column] = result_df[column].fillna(method='bfill')
            elif method == 'mean':
                if pd.api.types.is_numeric_dtype(result_df[column]):
                    result_df[column] = result_df[column].fillna(result_df[column].mean())
            elif method == 'median':
                if pd.api.types.is_numeric_dtype(result_df[column]):
                    result_df[column] = result_df[column].fillna(result_df[column].median())
            elif method == 'mode':
                mode_value = result_df[column].mode()
                if len(mode_value) > 0:
                    result_df[column] = result_df[column].fillna(mode_value[0])
        else:
            # Fill all columns
            method = config.get('method', 'value')
            value = config.get('value', '')
            
            if method == 'value':
                result_df = result_df.fillna(value)
            elif method == 'forward':
                result_df = result_df.fillna(method='ffill')
            elif method == 'backward':
                result_df = result_df.fillna(method='bfill')
        
        return result_df
    
    async def _apply_custom_sql(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Apply custom SQL transformation using pandas SQL capabilities"""
        import sqlite3
        import tempfile
        
        sql_query = config.get('script', '')
        if not sql_query:
            raise ValueError("SQL script is required")
        
        # Create temporary SQLite database
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            conn = sqlite3.connect(tmp.name)
            
            try:
                # Write DataFrame to SQLite
                df.to_sql('source_data', conn, index=False, if_exists='replace')
                
                # Execute SQL query
                result_df = pd.read_sql_query(sql_query, conn)
                
                return result_df
            finally:
                conn.close()
    
    async def _apply_custom_python(self, df: pd.DataFrame, config: Dict[str, Any], transaction_context: Optional["TransactionContext"] = None) -> pd.DataFrame:
        """Apply custom Python transformation"""
        script = config.get('script', '')
        if not script:
            raise ValueError("Python script is required")
        
        # Validate script first
        is_valid, error = await self.validate_python_script(script)
        if not is_valid:
            raise ValueError(f"Invalid Python script: {error}")
        
        # Create execution context
        context = {
            'df': df.copy(),
            'pd': pd,
            'np': np,
            'datetime': datetime,
            're': re,
            'json': json,
            'result': None
        }
        
        # Add parameters if provided
        if 'parameters' in config:
            context.update(config['parameters'])
        
        try:
            # Execute script
            exec(script, context)
            
            # Get result
            if 'result' in context and isinstance(context['result'], pd.DataFrame):
                return context['result']
            elif isinstance(context['df'], pd.DataFrame):
                return context['df']
            else:
                raise ValueError("Script must return a DataFrame as 'result' or modify 'df'")
                
        except Exception as e:
            raise ValueError(f"Script execution failed: {str(e)}")
    
    async def validate_python_script(self, script: str) -> Tuple[bool, Optional[str]]:
        """Validate a Python transformation script"""
        try:
            # Parse the script to check syntax
            ast.parse(script)
            
            # Check for dangerous operations
            dangerous_keywords = [
                'exec', 'eval', '__import__', 'open', 'file',
                'compile', 'globals', 'locals', 'vars',
                'os.', 'sys.', 'subprocess', 'socket'
            ]
            
            for keyword in dangerous_keywords:
                if keyword in script:
                    return False, f"Dangerous operation '{keyword}' not allowed"
            
            # Check that it's not trying to import dangerous modules
            tree = ast.parse(script)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in ['os', 'sys', 'subprocess', 'socket']:
                            return False, f"Import of '{alias.name}' not allowed"
                elif isinstance(node, ast.ImportFrom):
                    if node.module in ['os', 'sys', 'subprocess', 'socket']:
                        return False, f"Import from '{node.module}' not allowed"
            
            return True, None
            
        except SyntaxError as e:
            return False, f"Syntax error: {str(e)}"
        except Exception as e:
            return False, str(e)
    
    async def validate_sql_script(self, script: str) -> Tuple[bool, Optional[str]]:
        """Validate a SQL transformation script"""
        try:
            # Parse SQL to check syntax
            parsed = sqlparse.parse(script)
            if not parsed:
                return False, "Invalid SQL syntax"
            
            # Check for dangerous operations
            dangerous_keywords = [
                'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE',
                'INSERT', 'UPDATE', 'GRANT', 'REVOKE'
            ]
            
            formatted = sqlparse.format(script, keyword_case='upper')
            for keyword in dangerous_keywords:
                if keyword in formatted:
                    return False, f"Operation '{keyword}' not allowed in transformation SQL"
            
            # Ensure it's a SELECT statement
            if not formatted.strip().upper().startswith('SELECT'):
                return False, "Only SELECT statements are allowed"
            
            return True, None
            
        except Exception as e:
            return False, str(e)
    
    async def save_to_table(self, df: pd.DataFrame, table_name: str, executor, if_exists: str = 'replace', primary_key_columns: Optional[List[str]] = None):
        """Save DataFrame to database table with various modes"""
        from app.services.import_utils import prepare_dataframe_for_import
        from app.services.type_detection import detect_column_type
        
        # Check if table exists
        table_exists = await self._check_table_exists(executor, table_name)
        
        if if_exists == 'replace':
            if table_exists:
                # Drop table if exists
                drop_query = f'DROP TABLE IF EXISTS "{table_name}"'
                await executor.execute_query(drop_query)
                table_exists = False
        elif if_exists == 'fail' and table_exists:
            raise Exception(f"Table '{table_name}' already exists and if_exists='fail'")
        
        # Detect column types
        column_types = {}
        for col in df.columns:
            sql_type, _ = detect_column_type(df[col])
            column_types[col] = sql_type
        
        if not table_exists:
            # Create table if it doesn't exist
            create_table_sql, insert_columns, _, _ = prepare_dataframe_for_import(
                df, table_name, column_types
            )
            
            result = await executor.execute_query(create_table_sql)
            if result['error']:
                raise Exception(f"Failed to create table: {result['error']}")
        else:
            # Table exists, validate columns match for append/upsert/merge modes
            existing_columns = await self._get_table_columns(executor, table_name)
            df_columns = set(df.columns)
            
            if not df_columns.issubset(set(existing_columns)):
                missing_cols = df_columns - set(existing_columns)
                raise Exception(f"DataFrame contains columns not in existing table: {missing_cols}")
        
        # Handle different loading modes
        if if_exists == 'append':
            await self._append_data(df, table_name, executor)
        elif if_exists == 'upsert':
            await self._upsert_data(df, table_name, executor, primary_key_columns)
        elif if_exists == 'merge':
            await self._merge_data(df, table_name, executor, primary_key_columns)
        else:  # replace or new table
            await self._insert_data(df, table_name, executor)

    async def _check_table_exists(self, executor, table_name: str) -> bool:
        """Check if table exists in database"""
        try:
            check_query = f"SELECT 1 FROM \"{table_name}\" LIMIT 1"
            result = await executor.execute_query(check_query)
            return not result['error']
        except:
            return False
    
    async def _get_table_columns(self, executor, table_name: str) -> List[str]:
        """Get list of column names from existing table"""
        try:
            # This query works for most SQL databases
            info_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
            """
            result = await executor.execute_query(info_query)
            if result['error']:
                # Fallback: try to select from table and get columns
                fallback_query = f'SELECT * FROM "{table_name}" LIMIT 0'
                result = await executor.execute_query(fallback_query)
                return result['columns'] if not result['error'] else []
            else:
                return [row[0] for row in result['rows']]
        except:
            return []
    
    async def _insert_data(self, df: pd.DataFrame, table_name: str, executor):
        """Insert data using standard INSERT statements"""
        from app.services.import_utils import generate_insert_sql
        
        # Get column info for insert
        column_names = list(df.columns)
        insert_statements = generate_insert_sql(df, table_name, column_names)
        
        for insert_sql in insert_statements:
            result = await executor.execute_query(insert_sql)
            if result['error']:
                raise Exception(f"Failed to insert data: {result['error']}")
    
    async def _append_data(self, df: pd.DataFrame, table_name: str, executor):
        """Append data to existing table"""
        await self._insert_data(df, table_name, executor)
    
    async def _upsert_data(self, df: pd.DataFrame, table_name: str, executor, primary_key_columns: Optional[List[str]]):
        """Upsert data (INSERT ... ON CONFLICT DO UPDATE for PostgreSQL, REPLACE for MySQL)"""
        if not primary_key_columns:
            # If no primary key specified, try to detect from table
            primary_key_columns = await self._detect_primary_key(executor, table_name)
            
        if not primary_key_columns:
            # Fallback to append if no primary key available
            await self._append_data(df, table_name, executor)
            return
        
        # Generate upsert statements based on database type
        db_type = executor.data_source_type
        
        for _, row in df.iterrows():
            if db_type == 'postgresql':
                await self._postgresql_upsert(row, table_name, executor, primary_key_columns)
            elif db_type == 'mysql':
                await self._mysql_upsert(row, table_name, executor, primary_key_columns)
            else:
                # Generic approach: DELETE then INSERT
                await self._generic_upsert(row, table_name, executor, primary_key_columns)
    
    async def _merge_data(self, df: pd.DataFrame, table_name: str, executor, primary_key_columns: Optional[List[str]]):
        """Advanced merge with custom logic (currently same as upsert)"""
        # For now, merge is the same as upsert
        # Could be extended for more complex scenarios like:
        # - Different update vs insert logic
        # - Conditional merging based on data values
        # - Soft deletes for records not in new dataset
        await self._upsert_data(df, table_name, executor, primary_key_columns)
    
    async def _detect_primary_key(self, executor, table_name: str) -> List[str]:
        """Detect primary key columns from table schema"""
        try:
            # Try PostgreSQL approach
            pk_query = f"""
            SELECT column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}' 
            AND tc.constraint_type = 'PRIMARY KEY'
            """
            result = await executor.execute_query(pk_query)
            if not result['error'] and result['rows']:
                return [row[0] for row in result['rows']]
                
            # Try MySQL approach
            mysql_pk_query = f"""
            SELECT column_name 
            FROM information_schema.key_column_usage 
            WHERE table_name = '{table_name}' 
            AND constraint_name = 'PRIMARY'
            """
            result = await executor.execute_query(mysql_pk_query)
            if not result['error'] and result['rows']:
                return [row[0] for row in result['rows']]
                
        except:
            pass
        return []
    
    async def _postgresql_upsert(self, row: pd.Series, table_name: str, executor, primary_key_columns: List[str]):
        """PostgreSQL-specific upsert using ON CONFLICT"""
        columns = list(row.index)
        values = [f"'{str(val).replace(chr(39), chr(39)+chr(39))}'" if pd.notna(val) else 'NULL' for val in row.values]
        
        # Build INSERT statement
        columns_str = ", ".join(f'"{col}"' for col in columns)
        insert_part = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({", ".join(values)})'
        
        # Build ON CONFLICT part
        pk_columns_str = ", ".join(f'"{col}"' for col in primary_key_columns)
        update_columns = [col for col in columns if col not in primary_key_columns]
        
        if update_columns:
            update_part = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)
            upsert_query = f'{insert_part} ON CONFLICT ({pk_columns_str}) DO UPDATE SET {update_part}'
        else:
            upsert_query = f'{insert_part} ON CONFLICT ({pk_columns_str}) DO NOTHING'
        
        result = await executor.execute_query(upsert_query)
        if result['error']:
            raise Exception(f"Failed to upsert data: {result['error']}")
    
    async def _mysql_upsert(self, row: pd.Series, table_name: str, executor, primary_key_columns: List[str]):
        """MySQL-specific upsert using REPLACE or ON DUPLICATE KEY UPDATE"""
        columns = list(row.index)
        values = [f"'{str(val).replace(chr(39), chr(39)+chr(39))}'" if pd.notna(val) else 'NULL' for val in row.values]
        
        # Use REPLACE for simpler syntax
        columns_str = ", ".join(f"`{col}`" for col in columns)
        replace_query = f'REPLACE INTO `{table_name}` ({columns_str}) VALUES ({", ".join(values)})'
        
        result = await executor.execute_query(replace_query)
        if result['error']:
            raise Exception(f"Failed to replace data: {result['error']}")
    
    async def _generic_upsert(self, row: pd.Series, table_name: str, executor, primary_key_columns: List[str]):
        """Generic upsert using DELETE then INSERT"""
        # Build WHERE clause for primary key
        pk_conditions = []
        for col in primary_key_columns:
            val = row[col]
            if pd.notna(val):
                pk_conditions.append(f'"{col}" = \'{str(val).replace(chr(39), chr(39)+chr(39))}\'')
            else:
                pk_conditions.append(f'"{col}" IS NULL')
        
        where_clause = " AND ".join(pk_conditions)
        
        # Delete existing record
        delete_query = f'DELETE FROM "{table_name}" WHERE {where_clause}'
        await executor.execute_query(delete_query)  # Ignore errors - record might not exist
        
        # Insert new record
        columns = list(row.index)
        values = [f"'{str(val).replace(chr(39), chr(39)+chr(39))}'" if pd.notna(val) else 'NULL' for val in row.values]
        
        columns_str = ", ".join(f'"{col}"' for col in columns)
        insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({", ".join(values)})'
        
        result = await executor.execute_query(insert_query)
        if result['error']:
            raise Exception(f"Failed to insert data: {result['error']}")
    
    async def export_data(self, df: pd.DataFrame, format: str, filename: Optional[str] = None) -> str:
        """Export DataFrame to file"""
        import tempfile
        import os
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"transformation_result_{timestamp}"
        
        # Create temp directory if not exists
        temp_dir = os.path.join(tempfile.gettempdir(), 'csv2sql_exports')
        os.makedirs(temp_dir, exist_ok=True)
        
        if format == 'csv':
            filepath = os.path.join(temp_dir, f"{filename}.csv")
            df.to_csv(filepath, index=False)
        elif format == 'excel':
            filepath = os.path.join(temp_dir, f"{filename}.xlsx")
            df.to_excel(filepath, index=False)
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        return filepath