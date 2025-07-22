"""
Code Generation API Endpoints
Generates code in multiple programming languages from SQL queries
"""

from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.sql_parser import sql_parser
from app.services.code_generators.python_generator import python_generator
from app.services.code_generators.java_generator import java_generator
from app.services.code_generators.go_generator import go_generator
from app.services.code_generators.nodejs_generator import nodejs_generator

router = APIRouter()


class CodeGenerationRequest(BaseModel):
    """Request model for code generation"""
    sql: str
    languages: Optional[List[str]] = None  # If None, generate for all languages
    frameworks: Optional[Dict[str, List[str]]] = None  # Optional framework filtering


class LanguageCode(BaseModel):
    """Generated code for a specific language and framework"""
    framework: str
    code: str
    description: str


class GeneratedCode(BaseModel):
    """Generated code response model"""
    language: str
    frameworks: List[LanguageCode]


class CodeGenerationResponse(BaseModel):
    """Response model for code generation"""
    sql: str
    query_type: str
    complexity_score: int
    generated_code: List[GeneratedCode]
    parsing_error: Optional[str] = None


@router.post("/generate", response_model=CodeGenerationResponse)
async def generate_code(request: CodeGenerationRequest) -> CodeGenerationResponse:
    """
    Generate code in multiple programming languages from SQL query
    
    Supports:
    - Python: raw psycopg2, SQLAlchemy Core/ORM, Django ORM, Peewee, Pandas
    - Java: JDBC, MyBatis, JPA/Hibernate, Spring Data JPA, jOOQ
    - Go: database/sql, sqlx, GORM, Squirrel, Ent
    - Node.js: raw pg/mysql2, Sequelize, TypeORM, Knex, Prisma, Mongoose
    """
    if not request.sql or not request.sql.strip():
        raise HTTPException(status_code=400, detail="SQL query cannot be empty")
    
    try:
        # Parse the SQL query
        parsed_query = sql_parser.parse(request.sql.strip())
        
        # Determine which languages to generate
        target_languages = request.languages or ["python", "java", "go", "nodejs"]
        
        generated_code = []
        
        # Generate code for each requested language
        for language in target_languages:
            if language.lower() == "python":
                frameworks = _generate_python_code(parsed_query, request.frameworks)
                generated_code.append(GeneratedCode(
                    language="Python",
                    frameworks=frameworks
                ))
            
            elif language.lower() == "java":
                frameworks = _generate_java_code(parsed_query, request.frameworks)
                generated_code.append(GeneratedCode(
                    language="Java",
                    frameworks=frameworks
                ))
            
            elif language.lower() == "go":
                frameworks = _generate_go_code(parsed_query, request.frameworks)
                generated_code.append(GeneratedCode(
                    language="Go",
                    frameworks=frameworks
                ))
            
            elif language.lower() in ["nodejs", "node", "javascript", "typescript"]:
                frameworks = _generate_nodejs_code(parsed_query, request.frameworks)
                generated_code.append(GeneratedCode(
                    language="Node.js",
                    frameworks=frameworks
                ))
        
        return CodeGenerationResponse(
            sql=request.sql,
            query_type=parsed_query.query_type.value,
            complexity_score=parsed_query.complexity_score,
            generated_code=generated_code
        )
    
    except Exception as e:
        return CodeGenerationResponse(
            sql=request.sql,
            query_type="UNKNOWN",
            complexity_score=0,
            generated_code=[],
            parsing_error=str(e)
        )


@router.get("/languages")
async def get_supported_languages() -> Dict[str, Any]:
    """Get list of supported languages and their frameworks"""
    return {
        "languages": {
            "python": {
                "name": "Python",
                "frameworks": [
                    {"id": "raw_python", "name": "Raw Python (psycopg2)", "description": "Direct database connection using psycopg2"},
                    {"id": "sqlalchemy_core", "name": "SQLAlchemy Core", "description": "SQLAlchemy core expressions"},
                    {"id": "sqlalchemy_orm", "name": "SQLAlchemy ORM", "description": "SQLAlchemy declarative ORM"},
                    {"id": "django_orm", "name": "Django ORM", "description": "Django model-based ORM"},
                    {"id": "peewee_orm", "name": "Peewee ORM", "description": "Lightweight Python ORM"},
                    {"id": "pandas", "name": "Pandas", "description": "Data analysis with pandas DataFrames"}
                ]
            },
            "java": {
                "name": "Java",
                "frameworks": [
                    {"id": "jdbc", "name": "JDBC", "description": "Plain JDBC with PreparedStatement"},
                    {"id": "mybatis", "name": "MyBatis", "description": "MyBatis mapper with XML configuration"},
                    {"id": "jpa_hibernate", "name": "JPA/Hibernate", "description": "JPA entities with Hibernate"},
                    {"id": "spring_data_jpa", "name": "Spring Data JPA", "description": "Spring Data repositories"},
                    {"id": "jooq", "name": "jOOQ", "description": "Type-safe SQL builder"}
                ]
            },
            "go": {
                "name": "Go",
                "frameworks": [
                    {"id": "database_sql", "name": "database/sql", "description": "Standard Go database package"},
                    {"id": "sqlx", "name": "sqlx", "description": "Extensions for database/sql"},
                    {"id": "gorm", "name": "GORM", "description": "Go ORM library"},
                    {"id": "squirrel", "name": "Squirrel", "description": "SQL query builder"},
                    {"id": "ent", "name": "Ent", "description": "Entity framework for Go"}
                ]
            },
            "nodejs": {
                "name": "Node.js",
                "frameworks": [
                    {"id": "raw_nodejs", "name": "Raw Node.js", "description": "Direct database connection with pg/mysql2"},
                    {"id": "sequelize", "name": "Sequelize", "description": "Promise-based Node.js ORM"},
                    {"id": "typeorm", "name": "TypeORM", "description": "TypeScript and JavaScript ORM"},
                    {"id": "knex", "name": "Knex.js", "description": "SQL query builder for Node.js"},
                    {"id": "prisma", "name": "Prisma", "description": "Modern database toolkit"},
                    {"id": "mongoose", "name": "Mongoose", "description": "MongoDB object modeling (NoSQL alternative)"}
                ]
            }
        }
    }


@router.post("/parse")
async def parse_sql(request: Dict[str, str]) -> Dict[str, Any]:
    """Parse SQL query and return structure information"""
    sql = request.get("sql", "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL query cannot be empty")
    
    try:
        parsed_query = sql_parser.parse(sql)
        
        return {
            "query_type": parsed_query.query_type.value,
            "complexity_score": parsed_query.complexity_score,
            "tables": [{"name": t.name, "alias": t.alias, "schema": t.schema} for t in parsed_query.tables],
            "columns": [{"name": c.name, "alias": c.alias, "is_aggregate": c.is_aggregate} for c in parsed_query.columns],
            "has_joins": len(parsed_query.joins) > 0,
            "has_where": len(parsed_query.where_conditions) > 0,
            "has_group_by": parsed_query.group_by is not None,
            "has_order_by": len(parsed_query.order_by) > 0,
            "limit": parsed_query.limit,
            "offset": parsed_query.offset
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse SQL: {str(e)}")


def _generate_python_code(parsed_query, framework_filter) -> List[LanguageCode]:
    """Generate Python code for all frameworks"""
    frameworks_data = python_generator.generate_all_frameworks(parsed_query)
    
    framework_descriptions = {
        "raw_python": "Direct database connection using psycopg2 with proper error handling",
        "sqlalchemy_core": "SQLAlchemy Core expressions for database-agnostic queries",
        "sqlalchemy_orm": "SQLAlchemy declarative ORM with model classes",
        "django_orm": "Django model-based ORM with queryset operations",
        "peewee_orm": "Lightweight Peewee ORM with model definitions",
        "pandas": "Data analysis using pandas DataFrames with SQL queries"
    }
    
    frameworks = []
    for framework_id, code in frameworks_data.items():
        if not framework_filter or "python" not in framework_filter or framework_id in framework_filter.get("python", []):
            frameworks.append(LanguageCode(
                framework=framework_id.replace("_", " ").title(),
                code=code,
                description=framework_descriptions.get(framework_id, "")
            ))
    
    return frameworks


def _generate_java_code(parsed_query, framework_filter) -> List[LanguageCode]:
    """Generate Java code for all frameworks"""
    frameworks_data = java_generator.generate_all_frameworks(parsed_query)
    
    framework_descriptions = {
        "jdbc": "Plain JDBC with PreparedStatement and proper resource management",
        "mybatis": "MyBatis mapper interface with XML configuration",
        "jpa_hibernate": "JPA entities with Hibernate implementation",
        "spring_data_jpa": "Spring Data repositories with query methods",
        "jooq": "Type-safe SQL builder with compile-time verification"
    }
    
    frameworks = []
    for framework_id, code in frameworks_data.items():
        if not framework_filter or "java" not in framework_filter or framework_id in framework_filter.get("java", []):
            frameworks.append(LanguageCode(
                framework=framework_id.replace("_", " ").title(),
                code=code,
                description=framework_descriptions.get(framework_id, "")
            ))
    
    return frameworks


def _generate_go_code(parsed_query, framework_filter) -> List[LanguageCode]:
    """Generate Go code for all frameworks"""
    frameworks_data = go_generator.generate_all_frameworks(parsed_query)
    
    framework_descriptions = {
        "database_sql": "Standard Go database/sql package with proper error handling",
        "sqlx": "Extensions to database/sql with struct scanning",
        "gorm": "Feature-rich ORM library for Go",
        "squirrel": "Fluent SQL query builder with parameter binding",
        "ent": "Entity framework with code generation"
    }
    
    frameworks = []
    for framework_id, code in frameworks_data.items():
        if not framework_filter or "go" not in framework_filter or framework_id in framework_filter.get("go", []):
            framework_name = framework_id.replace("_", "/") if framework_id == "database_sql" else framework_id.title()
            frameworks.append(LanguageCode(
                framework=framework_name,
                code=code,
                description=framework_descriptions.get(framework_id, "")
            ))
    
    return frameworks


def _generate_nodejs_code(parsed_query, framework_filter) -> List[LanguageCode]:
    """Generate Node.js code for all frameworks"""
    frameworks_data = nodejs_generator.generate_all_frameworks(parsed_query)
    
    framework_descriptions = {
        "raw_nodejs": "Direct database connection with pg/mysql2 and async/await",
        "sequelize": "Promise-based ORM with model definitions and associations",
        "typeorm": "TypeScript/JavaScript ORM with decorators and repositories",
        "knex": "SQL query builder with migrations and schema building",
        "prisma": "Modern database toolkit with type-safe client generation",
        "mongoose": "MongoDB object modeling for Node.js (NoSQL alternative)"
    }
    
    frameworks = []
    for framework_id, code in frameworks_data.items():
        if not framework_filter or "nodejs" not in framework_filter or framework_id in framework_filter.get("nodejs", []):
            framework_name = framework_id.replace("_", " ").title().replace("Nodejs", "Node.js")
            frameworks.append(LanguageCode(
                framework=framework_name,
                code=code,
                description=framework_descriptions.get(framework_id, "")
            ))
    
    return frameworks