"""
SQL Parser and Analyzer for Code Generation
Parses SQL queries and extracts structure for generating code in multiple languages
"""

import re
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import sqlparse
from sqlparse.sql import Statement, Token, Identifier, IdentifierList, Parenthesis, Function
from sqlparse.tokens import DML, Keyword, Name, Punctuation


class QueryType(Enum):
    """Types of SQL queries"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE_TABLE = "CREATE_TABLE"
    ALTER_TABLE = "ALTER_TABLE"
    DROP_TABLE = "DROP_TABLE"
    UNKNOWN = "UNKNOWN"


class JoinType(Enum):
    """Types of SQL joins"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


@dataclass
class ColumnReference:
    """Represents a column reference in SQL"""
    name: str
    table_alias: Optional[str] = None
    table_name: Optional[str] = None
    alias: Optional[str] = None
    is_aggregate: bool = False
    aggregate_function: Optional[str] = None


@dataclass
class TableReference:
    """Represents a table reference in SQL"""
    name: str
    alias: Optional[str] = None
    schema: Optional[str] = None


@dataclass
class JoinClause:
    """Represents a JOIN clause in SQL"""
    join_type: JoinType
    table: TableReference
    condition: str


@dataclass
class WhereCondition:
    """Represents a WHERE condition"""
    column: str
    operator: str
    value: Union[str, int, float, List]
    table_alias: Optional[str] = None


@dataclass
class OrderByClause:
    """Represents an ORDER BY clause"""
    column: str
    direction: str = "ASC"
    table_alias: Optional[str] = None


@dataclass
class GroupByClause:
    """Represents a GROUP BY clause"""
    columns: List[str]


@dataclass
class ParsedQuery:
    """Represents a fully parsed SQL query"""
    query_type: QueryType
    original_sql: str
    
    # SELECT specific
    columns: List[ColumnReference] = field(default_factory=list)
    tables: List[TableReference] = field(default_factory=list)
    joins: List[JoinClause] = field(default_factory=list)
    where_conditions: List[WhereCondition] = field(default_factory=list)
    group_by: Optional[GroupByClause] = None
    having_conditions: List[WhereCondition] = field(default_factory=list)
    order_by: List[OrderByClause] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    
    # INSERT specific
    insert_table: Optional[TableReference] = None
    insert_columns: List[str] = field(default_factory=list)
    insert_values: List[Dict[str, Any]] = field(default_factory=list)
    
    # UPDATE specific
    update_table: Optional[TableReference] = None
    update_assignments: List[Tuple[str, Any]] = field(default_factory=list)
    
    # DELETE specific
    delete_table: Optional[TableReference] = None
    
    # Additional metadata
    is_parameterized: bool = False
    parameters: List[str] = field(default_factory=list)
    complexity_score: int = 0


class SQLParser:
    """Advanced SQL parser for code generation"""
    
    def __init__(self):
        self.aggregate_functions = {
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 
            'STRING_AGG', 'STDDEV', 'VARIANCE', 'DISTINCT'
        }
        
    def parse(self, sql: str) -> ParsedQuery:
        """Parse SQL query and return structured representation"""
        # Clean and normalize SQL
        sql = sql.strip()
        if not sql:
            return ParsedQuery(QueryType.UNKNOWN, sql)
        
        # Parse with sqlparse
        try:
            parsed = sqlparse.parse(sql)[0]
        except Exception:
            return ParsedQuery(QueryType.UNKNOWN, sql)
        
        # Determine query type
        query_type = self._determine_query_type(parsed)
        
        # Create base parsed query
        parsed_query = ParsedQuery(query_type, sql)
        
        # Parse based on query type
        if query_type == QueryType.SELECT:
            self._parse_select(parsed, parsed_query)
        elif query_type == QueryType.INSERT:
            self._parse_insert(parsed, parsed_query)
        elif query_type == QueryType.UPDATE:
            self._parse_update(parsed, parsed_query)
        elif query_type == QueryType.DELETE:
            self._parse_delete(parsed, parsed_query)
        
        # Calculate complexity score
        parsed_query.complexity_score = self._calculate_complexity(parsed_query)
        
        return parsed_query
    
    def _determine_query_type(self, parsed: Statement) -> QueryType:
        """Determine the type of SQL query"""
        first_token = None
        for token in parsed.tokens:
            if token.ttype is not None or str(token).strip():
                first_token = str(token).strip().upper()
                break
        
        if first_token:
            if first_token.startswith('SELECT'):
                return QueryType.SELECT
            elif first_token.startswith('INSERT'):
                return QueryType.INSERT
            elif first_token.startswith('UPDATE'):
                return QueryType.UPDATE
            elif first_token.startswith('DELETE'):
                return QueryType.DELETE
            elif first_token.startswith('CREATE TABLE'):
                return QueryType.CREATE_TABLE
            elif first_token.startswith('ALTER TABLE'):
                return QueryType.ALTER_TABLE
            elif first_token.startswith('DROP TABLE'):
                return QueryType.DROP_TABLE
        
        return QueryType.UNKNOWN
    
    def _parse_select(self, parsed: Statement, query: ParsedQuery):
        """Parse SELECT statement"""
        tokens = list(parsed.flatten())
        i = 0
        
        while i < len(tokens):
            token = tokens[i]
            token_value = str(token).upper().strip()
            
            if token_value == 'SELECT':
                i = self._parse_select_columns(tokens, i + 1, query)
            elif token_value == 'FROM':
                i = self._parse_from_clause(tokens, i + 1, query)
            elif token_value in ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS']:
                i = self._parse_join_clause(tokens, i, query)
            elif token_value == 'WHERE':
                i = self._parse_where_clause(tokens, i + 1, query)
            elif token_value == 'GROUP':
                if i + 1 < len(tokens) and str(tokens[i + 1]).upper().strip() == 'BY':
                    i = self._parse_group_by(tokens, i + 2, query)
            elif token_value == 'HAVING':
                i = self._parse_having_clause(tokens, i + 1, query)
            elif token_value == 'ORDER':
                if i + 1 < len(tokens) and str(tokens[i + 1]).upper().strip() == 'BY':
                    i = self._parse_order_by(tokens, i + 2, query)
            elif token_value == 'LIMIT':
                i = self._parse_limit(tokens, i + 1, query)
            elif token_value == 'OFFSET':
                i = self._parse_offset(tokens, i + 1, query)
            else:
                i += 1
    
    def _parse_select_columns(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse SELECT column list"""
        i = start_idx
        current_column = ""
        
        while i < len(tokens):
            token = tokens[i]
            token_value = str(token).strip()
            
            if str(token).upper().strip() == 'FROM':
                break
            
            if token.ttype is Punctuation and token_value == ',':
                if current_column.strip():
                    column = self._parse_column_reference(current_column.strip())
                    query.columns.append(column)
                current_column = ""
            else:
                current_column += token_value + " "
            
            i += 1
        
        # Add last column
        if current_column.strip():
            column = self._parse_column_reference(current_column.strip())
            query.columns.append(column)
        
        return i
    
    def _parse_column_reference(self, column_text: str) -> ColumnReference:
        """Parse individual column reference"""
        column_text = column_text.strip()
        
        # Check for alias (AS keyword or space separation)
        alias = None
        if ' AS ' in column_text.upper():
            parts = column_text.upper().split(' AS ')
            column_text = parts[0].strip()
            alias = parts[1].strip()
        elif ' ' in column_text and not any(func in column_text.upper() for func in self.aggregate_functions):
            parts = column_text.rsplit(' ', 1)
            if not parts[1].upper() in ['ASC', 'DESC', 'NULLS', 'FIRST', 'LAST']:
                column_text = parts[0].strip()
                alias = parts[1].strip()
        
        # Check for table prefix
        table_alias = None
        column_name = column_text
        if '.' in column_text:
            parts = column_text.split('.')
            if len(parts) == 2:
                table_alias = parts[0].strip()
                column_name = parts[1].strip()
        
        # Check for aggregate function
        is_aggregate = False
        aggregate_function = None
        for func in self.aggregate_functions:
            if func in column_text.upper():
                is_aggregate = True
                aggregate_function = func
                break
        
        return ColumnReference(
            name=column_name,
            table_alias=table_alias,
            alias=alias,
            is_aggregate=is_aggregate,
            aggregate_function=aggregate_function
        )
    
    def _parse_from_clause(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse FROM clause"""
        i = start_idx
        table_text = ""
        
        while i < len(tokens):
            token = tokens[i]
            token_value = str(token).upper().strip()
            
            if token_value in ['WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS']:
                break
            
            table_text += str(token) + " "
            i += 1
        
        if table_text.strip():
            table = self._parse_table_reference(table_text.strip())
            query.tables.append(table)
        
        return i
    
    def _parse_table_reference(self, table_text: str) -> TableReference:
        """Parse table reference with optional alias"""
        table_text = table_text.strip()
        
        # Check for alias
        alias = None
        if ' AS ' in table_text.upper():
            parts = table_text.upper().split(' AS ')
            table_text = parts[0].strip()
            alias = parts[1].strip()
        elif ' ' in table_text:
            parts = table_text.rsplit(' ', 1)
            table_text = parts[0].strip()
            alias = parts[1].strip()
        
        # Check for schema
        schema = None
        table_name = table_text
        if '.' in table_text:
            parts = table_text.split('.')
            if len(parts) == 2:
                schema = parts[0].strip()
                table_name = parts[1].strip()
        
        return TableReference(name=table_name, alias=alias, schema=schema)
    
    def _parse_join_clause(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse JOIN clause"""
        # This is a simplified implementation
        # In a full implementation, you'd parse the entire JOIN syntax
        return start_idx + 1
    
    def _parse_where_clause(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse WHERE clause"""
        # This is a simplified implementation
        # In a full implementation, you'd parse complex WHERE conditions
        return start_idx + 1
    
    def _parse_group_by(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse GROUP BY clause"""
        # Simplified implementation
        return start_idx + 1
    
    def _parse_having_clause(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse HAVING clause"""
        return start_idx + 1
    
    def _parse_order_by(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse ORDER BY clause"""
        return start_idx + 1
    
    def _parse_limit(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse LIMIT clause"""
        if start_idx < len(tokens):
            try:
                query.limit = int(str(tokens[start_idx]).strip())
            except ValueError:
                pass
        return start_idx + 1
    
    def _parse_offset(self, tokens: List[Token], start_idx: int, query: ParsedQuery) -> int:
        """Parse OFFSET clause"""
        if start_idx < len(tokens):
            try:
                query.offset = int(str(tokens[start_idx]).strip())
            except ValueError:
                pass
        return start_idx + 1
    
    def _parse_insert(self, parsed: Statement, query: ParsedQuery):
        """Parse INSERT statement"""
        # Simplified INSERT parsing
        pass
    
    def _parse_update(self, parsed: Statement, query: ParsedQuery):
        """Parse UPDATE statement"""
        # Simplified UPDATE parsing
        pass
    
    def _parse_delete(self, parsed: Statement, query: ParsedQuery):
        """Parse DELETE statement"""
        # Simplified DELETE parsing
        pass
    
    def _calculate_complexity(self, query: ParsedQuery) -> int:
        """Calculate query complexity score"""
        score = 0
        
        # Base score for query type
        if query.query_type == QueryType.SELECT:
            score += 1
        elif query.query_type in [QueryType.INSERT, QueryType.UPDATE, QueryType.DELETE]:
            score += 2
        
        # Add score for joins
        score += len(query.joins) * 2
        
        # Add score for columns
        score += len(query.columns)
        
        # Add score for conditions
        score += len(query.where_conditions)
        score += len(query.having_conditions)
        
        # Add score for grouping and ordering
        if query.group_by:
            score += 1
        score += len(query.order_by)
        
        return score


# Global parser instance
sql_parser = SQLParser()