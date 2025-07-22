"""
Python Code Generator
Generates Python code from SQL queries using various frameworks
"""

from typing import Dict, List, Any, Optional
from app.services.sql_parser import ParsedQuery, QueryType, ColumnReference


class PythonCodeGenerator:
    """Generates Python code from parsed SQL queries"""
    
    def generate_all_frameworks(self, query: ParsedQuery) -> Dict[str, str]:
        """Generate code for all Python frameworks"""
        return {
            "raw_python": self.generate_raw_python(query),
            "sqlalchemy_core": self.generate_sqlalchemy_core(query),
            "sqlalchemy_orm": self.generate_sqlalchemy_orm(query),
            "django_orm": self.generate_django_orm(query),
            "peewee_orm": self.generate_peewee_orm(query),
            "pandas": self.generate_pandas(query)
        }
    
    def generate_raw_python(self, query: ParsedQuery) -> str:
        """Generate raw Python with psycopg2/mysql-connector"""
        if query.query_type == QueryType.SELECT:
            return self._generate_raw_select(query)
        elif query.query_type == QueryType.INSERT:
            return self._generate_raw_insert(query)
        elif query.query_type == QueryType.UPDATE:
            return self._generate_raw_update(query)
        elif query.query_type == QueryType.DELETE:
            return self._generate_raw_delete(query)
        else:
            return f"# Unsupported query type: {query.query_type.value}"
    
    def _generate_raw_select(self, query: ParsedQuery) -> str:
        """Generate raw Python SELECT code"""
        code = []
        code.append("import psycopg2")
        code.append("from psycopg2.extras import RealDictCursor")
        code.append("")
        code.append("def execute_query(connection_params):")
        code.append("    conn = psycopg2.connect(**connection_params)")
        code.append("    try:")
        code.append("        with conn.cursor(cursor_factory=RealDictCursor) as cur:")
        
        # Add parameterized query
        sql_with_params = self._parameterize_sql(query.original_sql)
        code.append(f'            sql = """{sql_with_params}"""')
        
        if query.is_parameterized:
            code.append("            params = {}")
            for param in query.parameters:
                code.append(f"            # params['{param}'] = your_value")
            code.append("            cur.execute(sql, params)")
        else:
            code.append("            cur.execute(sql)")
        
        code.append("            rows = cur.fetchall()")
        code.append("            return [dict(row) for row in rows]")
        code.append("    finally:")
        code.append("        conn.close()")
        code.append("")
        code.append("# Usage:")
        code.append("# connection_params = {")
        code.append("#     'host': 'localhost',")
        code.append("#     'database': 'your_db',")
        code.append("#     'user': 'your_user',")
        code.append("#     'password': 'your_password'")
        code.append("# }")
        code.append("# result = execute_query(connection_params)")
        
        return "\n".join(code)
    
    def _generate_raw_insert(self, query: ParsedQuery) -> str:
        """Generate raw Python INSERT code"""
        code = []
        code.append("import psycopg2")
        code.append("")
        code.append("def insert_data(connection_params, data):")
        code.append("    conn = psycopg2.connect(**connection_params)")
        code.append("    try:")
        code.append("        with conn.cursor() as cur:")
        code.append(f'            sql = """{query.original_sql}"""')
        code.append("            cur.execute(sql, data)")
        code.append("            conn.commit()")
        code.append("            return cur.rowcount")
        code.append("    except Exception as e:")
        code.append("        conn.rollback()")
        code.append("        raise")
        code.append("    finally:")
        code.append("        conn.close()")
        
        return "\n".join(code)
    
    def _generate_raw_update(self, query: ParsedQuery) -> str:
        """Generate raw Python UPDATE code"""
        code = []
        code.append("import psycopg2")
        code.append("")
        code.append("def update_data(connection_params, data):")
        code.append("    conn = psycopg2.connect(**connection_params)")
        code.append("    try:")
        code.append("        with conn.cursor() as cur:")
        code.append(f'            sql = """{query.original_sql}"""')
        code.append("            cur.execute(sql, data)")
        code.append("            conn.commit()")
        code.append("            return cur.rowcount")
        code.append("    except Exception as e:")
        code.append("        conn.rollback()")
        code.append("        raise")
        code.append("    finally:")
        code.append("        conn.close()")
        
        return "\n".join(code)
    
    def _generate_raw_delete(self, query: ParsedQuery) -> str:
        """Generate raw Python DELETE code"""
        code = []
        code.append("import psycopg2")
        code.append("")
        code.append("def delete_data(connection_params, data):")
        code.append("    conn = psycopg2.connect(**connection_params)")
        code.append("    try:")
        code.append("        with conn.cursor() as cur:")
        code.append(f'            sql = """{query.original_sql}"""')
        code.append("            cur.execute(sql, data)")
        code.append("            conn.commit()")
        code.append("            return cur.rowcount")
        code.append("    except Exception as e:")
        code.append("        conn.rollback()")
        code.append("        raise")
        code.append("    finally:")
        code.append("        conn.close()")
        
        return "\n".join(code)
    
    def generate_sqlalchemy_core(self, query: ParsedQuery) -> str:
        """Generate SQLAlchemy Core code"""
        if query.query_type == QueryType.SELECT:
            return self._generate_sqlalchemy_core_select(query)
        else:
            return self._generate_sqlalchemy_core_dml(query)
    
    def _generate_sqlalchemy_core_select(self, query: ParsedQuery) -> str:
        """Generate SQLAlchemy Core SELECT code"""
        code = []
        code.append("from sqlalchemy import create_engine, text, MetaData, Table")
        code.append("from sqlalchemy.sql import select")
        code.append("")
        code.append("# Method 1: Using text() for complex queries")
        code.append("def execute_query_text(engine):")
        code.append(f'    sql = text("""{query.original_sql}""")')
        code.append("    with engine.connect() as conn:")
        code.append("        result = conn.execute(sql)")
        code.append("        return [dict(row._mapping) for row in result]")
        code.append("")
        
        if query.tables and len(query.tables) == 1:  # Simple single table query
            table_name = query.tables[0].name
            code.append("# Method 2: Using SQLAlchemy constructs")
            code.append("def execute_query_construct(engine):")
            code.append("    metadata = MetaData()")
            code.append(f"    {table_name}_table = Table('{table_name}', metadata, autoload_with=engine)")
            code.append("")
            
            if query.columns:
                columns = []
                for col in query.columns:
                    if col.name == "*":
                        columns.append(f"{table_name}_table")
                        break
                    else:
                        columns.append(f"{table_name}_table.c.{col.name}")
                
                code.append(f"    stmt = select({', '.join(columns)})")
            else:
                code.append(f"    stmt = select({table_name}_table)")
            
            # Add WHERE conditions if simple
            if query.where_conditions:
                code.append("    # Add WHERE conditions")
                for condition in query.where_conditions:
                    code.append(f"    stmt = stmt.where({table_name}_table.c.{condition.column} {condition.operator} :param)")
            
            # Add ORDER BY if present
            if query.order_by:
                code.append("    # Add ORDER BY")
                for order in query.order_by:
                    direction = "desc()" if order.direction.upper() == "DESC" else ""
                    code.append(f"    stmt = stmt.order_by({table_name}_table.c.{order.column}{f'.{direction}' if direction else ''})")
            
            # Add LIMIT if present
            if query.limit:
                code.append(f"    stmt = stmt.limit({query.limit})")
            
            code.append("")
            code.append("    with engine.connect() as conn:")
            code.append("        result = conn.execute(stmt)")
            code.append("        return [dict(row._mapping) for row in result]")
        
        code.append("")
        code.append("# Usage:")
        code.append("# engine = create_engine('postgresql://user:password@localhost/dbname')")
        code.append("# result = execute_query_text(engine)")
        
        return "\n".join(code)
    
    def _generate_sqlalchemy_core_dml(self, query: ParsedQuery) -> str:
        """Generate SQLAlchemy Core DML code"""
        code = []
        code.append("from sqlalchemy import create_engine, text")
        code.append("")
        code.append("def execute_dml(engine, params=None):")
        code.append(f'    sql = text("""{query.original_sql}""")')
        code.append("    with engine.connect() as conn:")
        code.append("        result = conn.execute(sql, params or {})")
        code.append("        conn.commit()")
        code.append("        return result.rowcount")
        
        return "\n".join(code)
    
    def generate_sqlalchemy_orm(self, query: ParsedQuery) -> str:
        """Generate SQLAlchemy ORM code"""
        if not query.tables:
            return "# Cannot generate ORM code without table information"
        
        code = []
        code.append("from sqlalchemy import create_engine, Column, Integer, String, DateTime")
        code.append("from sqlalchemy.ext.declarative import declarative_base")
        code.append("from sqlalchemy.orm import sessionmaker")
        code.append("")
        code.append("Base = declarative_base()")
        code.append("")
        
        # Generate model class for primary table
        primary_table = query.tables[0]
        class_name = self._to_pascal_case(primary_table.name)
        
        code.append(f"class {class_name}(Base):")
        code.append(f"    __tablename__ = '{primary_table.name}'")
        code.append("    ")
        code.append("    id = Column(Integer, primary_key=True)")
        
        # Add columns based on SELECT columns
        if query.columns:
            for col in query.columns:
                if col.name != "*" and not col.is_aggregate:
                    col_name = col.name
                    code.append(f"    {col_name} = Column(String)  # Adjust type as needed")
        
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("def query_data(session):")
            code.append(f"    query = session.query({class_name})")
            
            # Add WHERE conditions
            if query.where_conditions:
                for condition in query.where_conditions:
                    code.append(f"    query = query.filter({class_name}.{condition.column} {condition.operator} :param)")
            
            # Add ORDER BY
            if query.order_by:
                for order in query.order_by:
                    if order.direction.upper() == "DESC":
                        code.append(f"    query = query.order_by({class_name}.{order.column}.desc())")
                    else:
                        code.append(f"    query = query.order_by({class_name}.{order.column})")
            
            # Add LIMIT
            if query.limit:
                code.append(f"    query = query.limit({query.limit})")
            
            code.append("    return query.all()")
        
        code.append("")
        code.append("# Usage:")
        code.append("# engine = create_engine('postgresql://user:password@localhost/dbname')")
        code.append("# Session = sessionmaker(bind=engine)")
        code.append("# session = Session()")
        code.append("# result = query_data(session)")
        code.append("# session.close()")
        
        return "\n".join(code)
    
    def generate_django_orm(self, query: ParsedQuery) -> str:
        """Generate Django ORM code"""
        if not query.tables:
            return "# Cannot generate Django ORM code without table information"
        
        primary_table = query.tables[0]
        model_name = self._to_pascal_case(primary_table.name)
        
        code = []
        code.append("# models.py")
        code.append("from django.db import models")
        code.append("")
        code.append(f"class {model_name}(models.Model):")
        
        if query.columns:
            for col in query.columns:
                if col.name != "*" and not col.is_aggregate:
                    col_name = col.name
                    code.append(f"    {col_name} = models.CharField(max_length=255)  # Adjust field type as needed")
        else:
            code.append("    # Define your fields here")
            code.append("    name = models.CharField(max_length=255)")
        
        code.append("")
        code.append(f"    class Meta:")
        code.append(f"        db_table = '{primary_table.name}'")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("# views.py or wherever you query")
            code.append(f"def get_{primary_table.name}_data():")
            code.append(f"    queryset = {model_name}.objects")
            
            # Add filtering
            if query.where_conditions:
                filters = []
                for condition in query.where_conditions:
                    if condition.operator == "=":
                        filters.append(f"{condition.column}=value")
                    elif condition.operator == "LIKE":
                        filters.append(f"{condition.column}__icontains=value")
                    elif condition.operator == ">":
                        filters.append(f"{condition.column}__gt=value")
                    elif condition.operator == "<":
                        filters.append(f"{condition.column}__lt=value")
                
                if filters:
                    code.append(f"    queryset = queryset.filter({', '.join(filters)})")
            
            # Add ordering
            if query.order_by:
                order_fields = []
                for order in query.order_by:
                    field = order.column if order.direction.upper() == "ASC" else f"-{order.column}"
                    order_fields.append(f"'{field}'")
                code.append(f"    queryset = queryset.order_by({', '.join(order_fields)})")
            
            # Add limit
            if query.limit:
                code.append(f"    queryset = queryset[:{query.limit}]")
            
            code.append("    return list(queryset)")
        
        return "\n".join(code)
    
    def generate_peewee_orm(self, query: ParsedQuery) -> str:
        """Generate Peewee ORM code"""
        if not query.tables:
            return "# Cannot generate Peewee ORM code without table information"
        
        primary_table = query.tables[0]
        model_name = self._to_pascal_case(primary_table.name)
        
        code = []
        code.append("from peewee import *")
        code.append("")
        code.append("# Database connection")
        code.append("db = PostgresqlDatabase('your_database', user='user', password='password', host='localhost', port=5432)")
        code.append("")
        code.append("class BaseModel(Model):")
        code.append("    class Meta:")
        code.append("        database = db")
        code.append("")
        code.append(f"class {model_name}(BaseModel):")
        
        if query.columns:
            for col in query.columns:
                if col.name != "*" and not col.is_aggregate:
                    col_name = col.name
                    code.append(f"    {col_name} = CharField()  # Adjust field type as needed")
        else:
            code.append("    # Define your fields here")
        
        code.append("")
        code.append("    class Meta:")
        code.append(f"        table_name = '{primary_table.name}'")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append(f"def query_{primary_table.name}_data():")
            code.append(f"    query = {model_name}.select()")
            
            # Add WHERE conditions
            if query.where_conditions:
                for condition in query.where_conditions:
                    code.append(f"    query = query.where({model_name}.{condition.column} {condition.operator} value)")
            
            # Add ORDER BY
            if query.order_by:
                for order in query.order_by:
                    if order.direction.upper() == "DESC":
                        code.append(f"    query = query.order_by({model_name}.{order.column}.desc())")
                    else:
                        code.append(f"    query = query.order_by({model_name}.{order.column})")
            
            # Add LIMIT
            if query.limit:
                code.append(f"    query = query.limit({query.limit})")
            
            code.append("    return list(query)")
        
        code.append("")
        code.append("# Usage:")
        code.append("# db.connect()")
        code.append(f"# result = query_{primary_table.name}_data()")
        code.append("# db.close()")
        
        return "\n".join(code)
    
    def generate_pandas(self, query: ParsedQuery) -> str:
        """Generate Pandas code for data analysis"""
        code = []
        code.append("import pandas as pd")
        code.append("from sqlalchemy import create_engine")
        code.append("")
        code.append("def query_to_dataframe(connection_string):")
        code.append("    engine = create_engine(connection_string)")
        code.append(f'    sql = """{query.original_sql}"""')
        code.append("    df = pd.read_sql(sql, engine)")
        code.append("    return df")
        code.append("")
        code.append("# Usage:")
        code.append("# connection_string = 'postgresql://user:password@localhost/dbname'")
        code.append("# df = query_to_dataframe(connection_string)")
        code.append("# print(df.head())")
        code.append("")
        code.append("# Data analysis examples:")
        code.append("# df.describe()  # Statistical summary")
        code.append("# df.info()      # Data types and null counts")
        code.append("# df.groupby('column').sum()  # Grouping operations")
        
        return "\n".join(code)
    
    def _parameterize_sql(self, sql: str) -> str:
        """Convert SQL to use parameters (simplified)"""
        # This is a simplified version - in practice, you'd need more sophisticated parsing
        return sql
    
    def _to_pascal_case(self, snake_str: str) -> str:
        """Convert snake_case to PascalCase"""
        components = snake_str.split('_')
        return ''.join(word.capitalize() for word in components)


# Global instance
python_generator = PythonCodeGenerator()