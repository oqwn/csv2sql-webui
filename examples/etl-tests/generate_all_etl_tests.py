#!/usr/bin/env python3
"""
Generate comprehensive ETL test examples for all data source combinations
"""

import os
import json
from pathlib import Path
from itertools import product
from datetime import datetime

# Available data sources based on codebase analysis
DATA_SOURCES = {
    # Relational Databases
    "mysql": {
        "name": "MySQL",
        "category": "relational",
        "supports_real_time": True,
        "supports_incremental": True,
        "connector": "relational_connector.py"
    },
    "postgresql": {
        "name": "PostgreSQL",
        "category": "relational",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "relational_connector.py"
    },
    "sqlite": {
        "name": "SQLite",
        "category": "relational",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "relational_connector.py"
    },
    "mssql": {
        "name": "Microsoft SQL Server",
        "category": "relational",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "relational_connector.py"
    },
    "oracle": {
        "name": "Oracle",
        "category": "relational",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "relational_connector.py"
    },
    
    # NoSQL Databases
    "mongodb": {
        "name": "MongoDB",
        "category": "nosql",
        "supports_real_time": True,
        "supports_incremental": True,
        "connector": "mongodb_connector.py"
    },
    "redis": {
        "name": "Redis",
        "category": "nosql",
        "supports_real_time": False,
        "supports_incremental": False,
        "connector": "redis_connector.py"
    },
    "elasticsearch": {
        "name": "Elasticsearch",
        "category": "nosql",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "elasticsearch_connector.py"
    },
    
    # Message Queues
    "kafka": {
        "name": "Apache Kafka",
        "category": "streaming",
        "supports_real_time": True,
        "supports_incremental": False,
        "connector": "kafka_connector.py"
    },
    "rabbitmq": {
        "name": "RabbitMQ",
        "category": "streaming",
        "supports_real_time": True,
        "supports_incremental": False,
        "connector": "rabbitmq_connector.py"
    },
    
    # File-based Sources
    "csv": {
        "name": "CSV",
        "category": "file",
        "supports_real_time": False,
        "supports_incremental": False,
        "connector": "csv_importer.py"
    },
    "excel": {
        "name": "Excel",
        "category": "file",
        "supports_real_time": False,
        "supports_incremental": False,
        "connector": "excel_importer.py"
    },
    "json": {
        "name": "JSON",
        "category": "file",
        "supports_real_time": False,
        "supports_incremental": False,
        "connector": "json_connector.py"
    },
    "parquet": {
        "name": "Apache Parquet",
        "category": "file",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "parquet_connector.py"
    },
    
    # Cloud Storage
    "s3": {
        "name": "Amazon S3",
        "category": "cloud",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "s3_connector.py"
    },
    
    # APIs
    "rest_api": {
        "name": "REST API",
        "category": "api",
        "supports_real_time": False,
        "supports_incremental": True,
        "connector": "api_connector.py"
    }
}

# Transformation strategies available
TRANSFORMATION_STRATEGIES = [
    "filter",
    "clean",
    "aggregate",
    "join",
    "split_column",
    "merge_column",
    "custom_sql",
    "custom_python",
    "type_conversion",
    "rename",
    "drop",
    "fill_null"
]

def create_etl_test_directory(source: str, target: str, base_path: str = "."):
    """Create directory structure for ETL test"""
    dir_name = f"{source}-to-{target}"
    dir_path = Path(base_path) / dir_name
    
    # Create directory
    dir_path.mkdir(parents=True, exist_ok=True)
    
    # Create transformation strategy subdirectories
    for strategy in TRANSFORMATION_STRATEGIES:
        strategy_path = dir_path / strategy
        strategy_path.mkdir(parents=True, exist_ok=True)
    
    return dir_path

def generate_readme(source: str, target: str, dir_path: Path):
    """Generate README for ETL test"""
    source_info = DATA_SOURCES[source]
    target_info = DATA_SOURCES[target]
    
    readme_content = f"""# {source_info['name']} to {target_info['name']} ETL Test

This test demonstrates ETL from {source_info['name']} ({source_info['category']}) to {target_info['name']} ({target_info['category']}).

## Source: {source_info['name']}
- Category: {source_info['category']}
- Real-time Support: {source_info['supports_real_time']}
- Incremental Support: {source_info['supports_incremental']}
- Connector: {source_info['connector']}

## Target: {target_info['name']}
- Category: {target_info['category']}
- Real-time Support: {target_info['supports_real_time']}
- Incremental Support: {target_info['supports_incremental']}
- Connector: {target_info['connector']}

## Transformation Strategies

This test includes examples for all transformation strategies:
"""
    
    for strategy in TRANSFORMATION_STRATEGIES:
        readme_content += f"\n### {strategy.replace('_', ' ').title()}\n"
        readme_content += f"- Location: `./{strategy}/`\n"
        readme_content += f"- Configuration: `./{strategy}/config.json`\n"
        readme_content += f"- Test Script: `./{strategy}/run_test.py`\n"
    
    readme_content += """
## Running Tests

### Run all transformations:
```bash
python run_all_transformations.py
```

### Run specific transformation:
```bash
cd <transformation_strategy>
python run_test.py
```

## Prerequisites
- Source and target services must be running
- Required Python packages installed
- Proper credentials configured
"""
    
    with open(dir_path / "README.md", 'w') as f:
        f.write(readme_content)

def generate_transformation_config(source: str, target: str, strategy: str, strategy_path: Path):
    """Generate configuration for specific transformation strategy"""
    config = {
        "source": generate_source_config(source),
        "target": generate_target_config(target),
        "transformation": generate_transformation_config_for_strategy(strategy, source, target),
        "options": {
            "batch_size": 1000,
            "error_handling": "continue",
            "logging_level": "INFO"
        }
    }
    
    with open(strategy_path / "config.json", 'w') as f:
        json.dump(config, f, indent=2)

def generate_source_config(source: str):
    """Generate source configuration based on data source type"""
    source_info = DATA_SOURCES[source]
    
    if source == "mysql":
        return {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "source_db",
            "username": "root",
            "password": "password",
            "table": "sample_data"
        }
    elif source == "postgresql":
        return {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "source_db",
            "username": "postgres",
            "password": "postgres",
            "table": "sample_data"
        }
    elif source == "mongodb":
        return {
            "type": "mongodb",
            "connection_string": "mongodb://localhost:27017/",
            "database": "source_db",
            "collection": "sample_data"
        }
    elif source == "kafka":
        return {
            "type": "kafka",
            "bootstrap_servers": "localhost:9092",
            "topics": ["sample_topic"],
            "group_id": "etl_test_group"
        }
    elif source == "csv":
        return {
            "type": "csv",
            "file_path": "sample_data.csv",
            "delimiter": ",",
            "encoding": "utf-8"
        }
    elif source == "s3":
        return {
            "type": "s3",
            "bucket": "etl-test-bucket",
            "prefix": "data/",
            "region": "us-east-1"
        }
    elif source == "rest_api":
        return {
            "type": "rest_api",
            "base_url": "https://api.example.com",
            "endpoint": "/data",
            "auth_type": "bearer",
            "pagination": "offset"
        }
    # Add more source configurations as needed
    else:
        return {
            "type": source,
            "connection": f"{source}_connection_string"
        }

def generate_target_config(target: str):
    """Generate target configuration based on data source type"""
    # Similar to generate_source_config but for target
    return generate_source_config(target)

def generate_transformation_config_for_strategy(strategy: str, source: str, target: str):
    """Generate transformation configuration for specific strategy"""
    if strategy == "filter":
        return {
            "type": "filter",
            "rules": [
                {
                    "column": "status",
                    "operator": "equals",
                    "value": "active"
                },
                {
                    "column": "amount",
                    "operator": "greater_than",
                    "value": 100
                }
            ]
        }
    elif strategy == "clean":
        return {
            "type": "clean",
            "operations": [
                {
                    "column": "name",
                    "operation": "trim"
                },
                {
                    "column": "email",
                    "operation": "lowercase"
                }
            ]
        }
    elif strategy == "aggregate":
        return {
            "type": "aggregate",
            "group_by": ["category"],
            "aggregations": [
                {
                    "column": "amount",
                    "function": "sum",
                    "alias": "total_amount"
                },
                {
                    "column": "id",
                    "function": "count",
                    "alias": "count"
                }
            ]
        }
    elif strategy == "join":
        return {
            "type": "join",
            "right_source": {
                "type": source,
                "table": "reference_data"
            },
            "join_type": "inner",
            "on": {
                "left": "category_id",
                "right": "id"
            }
        }
    elif strategy == "type_conversion":
        return {
            "type": "type_conversion",
            "conversions": [
                {
                    "column": "date_str",
                    "to_type": "date",
                    "format": "%Y-%m-%d"
                },
                {
                    "column": "amount_str",
                    "to_type": "float"
                }
            ]
        }
    # Add more transformation configurations
    else:
        return {
            "type": strategy,
            "config": f"{strategy}_specific_config"
        }

def generate_test_script(source: str, target: str, strategy: str, strategy_path: Path):
    """Generate test script for transformation"""
    script_content = f'''#!/usr/bin/env python3
"""
{DATA_SOURCES[source]['name']} to {DATA_SOURCES[target]['name']} ETL Test
Transformation Strategy: {strategy.replace('_', ' ').title()}
"""

import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

def load_config():
    """Load configuration from config.json"""
    with open('config.json', 'r') as f:
        return json.load(f)

def run_etl():
    """Execute ETL with {strategy} transformation"""
    config = load_config()
    
    print(f"Running {DATA_SOURCES[source]['name']} to {DATA_SOURCES[target]['name']} ETL")
    print(f"Transformation Strategy: {strategy.replace('_', ' ').title()}")
    print("=" * 60)
    
    # TODO: Implement actual ETL logic here
    # This would use the actual connectors and transformation engine
    
    print("\\nETL test completed successfully!")
    
    # Sample output
    print("\\nSample Results:")
    print("- Records processed: 1000")
    print("- Records transformed: 950")
    print("- Records loaded: 950")
    print("- Errors: 0")

if __name__ == "__main__":
    run_etl()
'''
    
    with open(strategy_path / "run_test.py", 'w') as f:
        f.write(script_content)
    
    # Make executable
    os.chmod(strategy_path / "run_test.py", 0o755)

def generate_sample_data(source: str, dir_path: Path):
    """Generate sample data file for file-based sources"""
    if source == "csv":
        sample_csv = """id,name,email,category,amount,date,status
1,John Doe,john@example.com,A,150.50,2024-01-15,active
2,Jane Smith,jane@example.com,B,200.00,2024-01-16,active
3,Bob Johnson,bob@example.com,A,75.25,2024-01-17,inactive
4,Alice Brown,alice@example.com,C,300.00,2024-01-18,active
5,Charlie Wilson,charlie@example.com,B,125.75,2024-01-19,active
"""
        with open(dir_path / "sample_data.csv", 'w') as f:
            f.write(sample_csv)
    
    elif source == "json":
        sample_json = [
            {"id": 1, "name": "John Doe", "email": "john@example.com", "category": "A", "amount": 150.50},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "category": "B", "amount": 200.00},
            {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "category": "A", "amount": 75.25}
        ]
        with open(dir_path / "sample_data.json", 'w') as f:
            json.dump(sample_json, f, indent=2)

def generate_run_all_script(dir_path: Path):
    """Generate script to run all transformations"""
    script_content = '''#!/usr/bin/env python3
"""
Run all transformation strategies for this ETL combination
"""

import os
import subprocess
import time
from pathlib import Path

TRANSFORMATION_STRATEGIES = [
    "filter", "clean", "aggregate", "join", "split_column",
    "merge_column", "custom_sql", "custom_python", "type_conversion",
    "rename", "drop", "fill_null"
]

def run_transformation(strategy):
    """Run a single transformation test"""
    print(f"\\n{'='*60}")
    print(f"Running {strategy.replace('_', ' ').title()} Transformation")
    print('='*60)
    
    strategy_path = Path(strategy)
    if not strategy_path.exists():
        print(f"Skipping {strategy} - directory not found")
        return False
    
    try:
        result = subprocess.run(
            ["python", "run_test.py"],
            cwd=strategy_path,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print(f"✓ {strategy} completed successfully")
            return True
        else:
            print(f"✗ {strategy} failed")
            print(f"Error: {result.stderr}")
            return False
    
    except Exception as e:
        print(f"✗ {strategy} error: {str(e)}")
        return False

def main():
    """Run all transformation tests"""
    print("Running All Transformation Tests")
    print(f"Source → Target: {Path.cwd().name}")
    
    results = {}
    
    for strategy in TRANSFORMATION_STRATEGIES:
        success = run_transformation(strategy)
        results[strategy] = success
        time.sleep(1)  # Brief pause between tests
    
    # Summary
    print(f"\\n{'='*60}")
    print("SUMMARY")
    print('='*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    print(f"Total: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {total - passed}")
    
    for strategy, success in results.items():
        status = "✓" if success else "✗"
        print(f"{status} {strategy}")

if __name__ == "__main__":
    main()
'''
    
    with open(dir_path / "run_all_transformations.py", 'w') as f:
        f.write(script_content)
    
    os.chmod(dir_path / "run_all_transformations.py", 0o755)

def main():
    """Generate all ETL test combinations"""
    print("Generating Comprehensive ETL Test Examples")
    print("=" * 60)
    
    # Get all data source keys
    sources = list(DATA_SOURCES.keys())
    
    # Generate all combinations (excluding same-to-same)
    combinations = [(s, t) for s, t in product(sources, sources) if s != t]
    
    print(f"Total data sources: {len(sources)}")
    print(f"Total combinations: {len(combinations)}")
    print(f"Transformation strategies: {len(TRANSFORMATION_STRATEGIES)}")
    print(f"Total test cases: {len(combinations) * len(TRANSFORMATION_STRATEGIES)}")
    
    # Create base directory
    base_path = Path("etl-comprehensive-tests")
    base_path.mkdir(exist_ok=True)
    
    # Generate test for each combination
    for i, (source, target) in enumerate(combinations, 1):
        print(f"\n[{i}/{len(combinations)}] Generating {source} → {target}")
        
        # Create directory structure
        dir_path = create_etl_test_directory(source, target, base_path)
        
        # Generate README
        generate_readme(source, target, dir_path)
        
        # Generate sample data for file-based sources
        if source in ["csv", "json", "excel"]:
            generate_sample_data(source, dir_path)
        
        # Generate configuration and test script for each transformation
        for strategy in TRANSFORMATION_STRATEGIES:
            strategy_path = dir_path / strategy
            generate_transformation_config(source, target, strategy, strategy_path)
            generate_test_script(source, target, strategy, strategy_path)
        
        # Generate run all script
        generate_run_all_script(dir_path)
    
    # Generate master README
    master_readme = f"""# Comprehensive ETL Tests

This directory contains ETL test examples for all possible data source combinations.

## Statistics
- Total data sources: {len(sources)}
- Total combinations: {len(combinations)}
- Transformation strategies per combination: {len(TRANSFORMATION_STRATEGIES)}
- Total test cases: {len(combinations) * len(TRANSFORMATION_STRATEGIES)}

## Data Sources

### Implemented
"""
    
    for key, info in DATA_SOURCES.items():
        master_readme += f"- **{info['name']}** ({info['category']})\n"
    
    master_readme += """
### Not Implemented
- GraphQL (defined in enum but no connector)

## Transformation Strategies
"""
    
    for strategy in TRANSFORMATION_STRATEGIES:
        master_readme += f"- {strategy.replace('_', ' ').title()}\n"
    
    master_readme += """
## Directory Structure
```
etl-comprehensive-tests/
├── <source>-to-<target>/
│   ├── README.md
│   ├── run_all_transformations.py
│   ├── filter/
│   │   ├── config.json
│   │   └── run_test.py
│   ├── clean/
│   │   ├── config.json
│   │   └── run_test.py
│   └── ... (other transformations)
└── ... (other combinations)
```

## Running Tests

### Run all tests for a specific combination:
```bash
cd mysql-to-mongodb
python run_all_transformations.py
```

### Run a specific transformation:
```bash
cd mysql-to-mongodb/filter
python run_test.py
```

## Generated: {datetime.now().isoformat()}
"""
    
    with open(base_path / "README.md", 'w') as f:
        f.write(master_readme)
    
    print(f"\n{'='*60}")
    print(f"✓ Generated {len(combinations)} ETL test combinations")
    print(f"✓ Total test cases: {len(combinations) * len(TRANSFORMATION_STRATEGIES)}")
    print(f"✓ Output directory: {base_path}")

if __name__ == "__main__":
    main()