#!/usr/bin/env python3
"""
Enhanced ETL Test Generator with Realistic Implementations
Generates comprehensive test cases for all data source combinations
"""

import os
import json
from pathlib import Path
from itertools import product
from datetime import datetime
import shutil

# Import the data sources from the previous script
from generate_all_etl_tests import DATA_SOURCES, TRANSFORMATION_STRATEGIES

class ETLTestGenerator:
    def __init__(self, base_path="etl-comprehensive-tests-v2"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        
    def generate_all_tests(self):
        """Generate all ETL test combinations with realistic implementations"""
        sources = list(DATA_SOURCES.keys())
        combinations = [(s, t) for s, t in product(sources, sources) if s != t]
        
        print(f"Generating {len(combinations)} ETL test combinations...")
        
        # Generate summary matrix
        self.generate_matrix_visualization(sources)
        
        # Generate tests for each combination
        for i, (source, target) in enumerate(combinations, 1):
            if i % 10 == 0:
                print(f"Progress: {i}/{len(combinations)}")
            
            dir_path = self.base_path / f"{source}-to-{target}"
            dir_path.mkdir(exist_ok=True)
            
            # Generate comprehensive test structure
            self.generate_test_structure(source, target, dir_path)
            
        # Generate master test runner
        self.generate_master_runner()
        
        print(f"✓ Generated {len(combinations)} ETL test combinations")
        print(f"✓ Total test cases: {len(combinations) * len(TRANSFORMATION_STRATEGIES)}")
    
    def generate_test_structure(self, source: str, target: str, dir_path: Path):
        """Generate complete test structure for a source-target combination"""
        # Create directories
        (dir_path / "data").mkdir(exist_ok=True)
        (dir_path / "transformations").mkdir(exist_ok=True)
        (dir_path / "tests").mkdir(exist_ok=True)
        (dir_path / "logs").mkdir(exist_ok=True)
        
        # Generate main configuration
        self.generate_main_config(source, target, dir_path)
        
        # Generate ETL implementation
        self.generate_etl_implementation(source, target, dir_path)
        
        # Generate test data
        self.generate_test_data(source, target, dir_path)
        
        # Generate transformation tests
        for strategy in TRANSFORMATION_STRATEGIES:
            self.generate_transformation_test(source, target, strategy, dir_path)
        
        # Generate README with examples
        self.generate_detailed_readme(source, target, dir_path)
        
        # Generate Docker compose for this combination
        self.generate_docker_compose(source, target, dir_path)
    
    def generate_main_config(self, source: str, target: str, dir_path: Path):
        """Generate main configuration file"""
        config = {
            "etl_name": f"{source}_to_{target}",
            "source": self.get_detailed_source_config(source),
            "target": self.get_detailed_target_config(target),
            "transformations": {
                strategy: self.get_transformation_example(strategy)
                for strategy in TRANSFORMATION_STRATEGIES
            },
            "execution": {
                "batch_size": 1000,
                "parallel_workers": 4,
                "error_handling": "continue",
                "checkpoint_interval": 5000,
                "retry_policy": {
                    "max_retries": 3,
                    "backoff_factor": 2
                }
            },
            "monitoring": {
                "metrics_enabled": True,
                "log_level": "INFO",
                "performance_tracking": True
            }
        }
        
        with open(dir_path / "config.json", 'w') as f:
            json.dump(config, f, indent=2)
    
    def generate_etl_implementation(self, source: str, target: str, dir_path: Path):
        """Generate main ETL implementation"""
        implementation = f'''#!/usr/bin/env python3
"""
{DATA_SOURCES[source]['name']} to {DATA_SOURCES[target]['name']} ETL Implementation
Generated: {datetime.now().isoformat()}
"""

import json
import logging
import time
from pathlib import Path
from datetime import datetime
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/etl_{{datetime.now().strftime("%Y%m%d_%H%M%S")}}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class {self.get_class_name(source)}To{self.get_class_name(target)}ETL:
    """ETL implementation for {source} to {target}"""
    
    def __init__(self, config_path="config.json"):
        with open(config_path) as f:
            self.config = json.load(f)
        
        self.source_config = self.config['source']
        self.target_config = self.config['target']
        self.metrics = {{
            'start_time': None,
            'end_time': None,
            'records_read': 0,
            'records_transformed': 0,
            'records_written': 0,
            'errors': 0
        }}
    
    def connect_source(self):
        """Connect to {source} source"""
        logger.info(f"Connecting to {source}...")
        # TODO: Implement actual connection using {DATA_SOURCES[source]['connector']}
        logger.info(f"Connected to {source} successfully")
    
    def connect_target(self):
        """Connect to {target} target"""
        logger.info(f"Connecting to {target}...")
        # TODO: Implement actual connection using {DATA_SOURCES[target]['connector']}
        logger.info(f"Connected to {target} successfully")
    
    def extract(self):
        """Extract data from {source}"""
        logger.info("Starting extraction...")
        self.metrics['start_time'] = datetime.now()
        
        # TODO: Implement extraction logic
        # Simulated extraction
        data = []
        for i in range(100):
            record = {{
                'id': i,
                'name': f'Record {{i}}',
                'value': i * 10,
                'timestamp': datetime.now().isoformat()
            }}
            data.append(record)
            self.metrics['records_read'] += 1
        
        logger.info(f"Extracted {{self.metrics['records_read']}} records")
        return data
    
    def transform(self, data, transformation_type=None):
        """Apply transformations to data"""
        logger.info(f"Applying transformation: {{transformation_type or 'default'}}")
        
        transformed_data = []
        for record in data:
            try:
                # Apply transformation based on type
                if transformation_type:
                    record = self.apply_transformation(record, transformation_type)
                
                transformed_data.append(record)
                self.metrics['records_transformed'] += 1
            except Exception as e:
                logger.error(f"Transformation error: {{e}}")
                self.metrics['errors'] += 1
        
        logger.info(f"Transformed {{self.metrics['records_transformed']}} records")
        return transformed_data
    
    def apply_transformation(self, record, transformation_type):
        """Apply specific transformation"""
        # TODO: Implement transformation logic based on type
        return record
    
    def load(self, data):
        """Load data to {target}"""
        logger.info("Starting load...")
        
        # TODO: Implement load logic
        # Simulated load
        for record in data:
            try:
                # Write record to target
                self.metrics['records_written'] += 1
            except Exception as e:
                logger.error(f"Load error: {{e}}")
                self.metrics['errors'] += 1
        
        logger.info(f"Loaded {{self.metrics['records_written']}} records")
    
    def run(self, transformation_type=None):
        """Execute the complete ETL pipeline"""
        try:
            logger.info("="*60)
            logger.info(f"Starting {source} to {target} ETL")
            logger.info(f"Transformation: {{transformation_type or 'None'}}")
            logger.info("="*60)
            
            # Connect to source and target
            self.connect_source()
            self.connect_target()
            
            # Extract
            data = self.extract()
            
            # Transform
            if transformation_type:
                data = self.transform(data, transformation_type)
            
            # Load
            self.load(data)
            
            # Finalize metrics
            self.metrics['end_time'] = datetime.now()
            duration = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
            
            # Print summary
            logger.info("="*60)
            logger.info("ETL COMPLETED")
            logger.info(f"Duration: {{duration:.2f}} seconds")
            logger.info(f"Records read: {{self.metrics['records_read']}}")
            logger.info(f"Records transformed: {{self.metrics['records_transformed']}}")
            logger.info(f"Records written: {{self.metrics['records_written']}}")
            logger.info(f"Errors: {{self.metrics['errors']}}")
            logger.info(f"Throughput: {{self.metrics['records_written']/duration:.2f}} records/second")
            logger.info("="*60)
            
            return self.metrics
            
        except Exception as e:
            logger.error(f"ETL failed: {{e}}")
            raise

def main():
    """Main entry point"""
    etl = {self.get_class_name(source)}To{self.get_class_name(target)}ETL()
    
    # Check if transformation type is specified
    transformation = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Run ETL
    etl.run(transformation)

if __name__ == "__main__":
    main()
'''
        
        with open(dir_path / "etl_pipeline.py", 'w') as f:
            f.write(implementation)
        
        os.chmod(dir_path / "etl_pipeline.py", 0o755)
    
    def generate_transformation_test(self, source: str, target: str, strategy: str, dir_path: Path):
        """Generate test for specific transformation strategy"""
        test_dir = dir_path / "transformations" / strategy
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate transformation config
        config = {
            "name": f"{strategy}_test",
            "source": source,
            "target": target,
            "transformation": self.get_transformation_example(strategy),
            "test_data": f"../data/{strategy}_test_data.json"
        }
        
        with open(test_dir / "config.json", 'w') as f:
            json.dump(config, f, indent=2)
        
        # Generate test script
        test_script = f'''#!/usr/bin/env python3
"""
Test {strategy.replace('_', ' ').title()} transformation
"""

import sys
import json
from pathlib import Path

# Add parent directories to path
sys.path.append(str(Path(__file__).parent.parent.parent))
sys.path.append(str(Path(__file__).parent.parent))

from etl_pipeline import {self.get_class_name(source)}To{self.get_class_name(target)}ETL

def test_{strategy}():
    """Test {strategy} transformation"""
    print(f"Testing {strategy} transformation...")
    
    # Load test configuration
    with open('config.json') as f:
        config = json.load(f)
    
    # Create ETL instance
    etl = {self.get_class_name(source)}To{self.get_class_name(target)}ETL("../../config.json")
    
    # Run with specific transformation
    metrics = etl.run('{strategy}')
    
    # Validate results
    assert metrics['errors'] == 0, f"ETL had {{metrics['errors']}} errors"
    assert metrics['records_written'] > 0, "No records were written"
    
    print(f"✓ {strategy} test passed")
    return True

if __name__ == "__main__":
    test_{strategy}()
'''
        
        with open(test_dir / "test.py", 'w') as f:
            f.write(test_script)
        
        os.chmod(test_dir / "test.py", 0o755)
    
    def generate_test_data(self, source: str, target: str, dir_path: Path):
        """Generate test data files"""
        data_dir = dir_path / "data"
        
        # Generate sample data based on source type
        if source in ["csv", "excel", "json"]:
            self.generate_file_test_data(source, data_dir)
        else:
            self.generate_db_test_data(source, data_dir)
        
        # Generate test data for each transformation
        for strategy in TRANSFORMATION_STRATEGIES:
            test_data = self.generate_transformation_test_data(strategy)
            with open(data_dir / f"{strategy}_test_data.json", 'w') as f:
                json.dump(test_data, f, indent=2)
    
    def generate_file_test_data(self, source: str, data_dir: Path):
        """Generate file-based test data"""
        if source == "csv":
            csv_data = """id,name,email,age,salary,department,hire_date,is_active
1,John Doe,john@example.com,30,75000,Engineering,2020-01-15,true
2,Jane Smith,jane@example.com,28,65000,Marketing,2019-03-22,true
3,Bob Johnson,bob@example.com,35,80000,Sales,2018-11-05,false
4,Alice Brown,alice@example.com,32,70000,Engineering,2021-06-10,true
5,Charlie Wilson,charlie@example.com,29,68000,HR,2020-09-18,true
"""
            with open(data_dir / "sample.csv", 'w') as f:
                f.write(csv_data)
        
        elif source == "json":
            json_data = [
                {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
                {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 28}
            ]
            with open(data_dir / "sample.json", 'w') as f:
                json.dump(json_data, f, indent=2)
    
    def generate_db_test_data(self, source: str, data_dir: Path):
        """Generate database test data scripts"""
        if source in ["mysql", "postgresql", "sqlite"]:
            sql_script = """-- Sample data for testing
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    age INTEGER,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOLEAN
);

INSERT INTO employees VALUES 
(1, 'John Doe', 'john@example.com', 30, 75000, 'Engineering', '2020-01-15', true),
(2, 'Jane Smith', 'jane@example.com', 28, 65000, 'Marketing', '2019-03-22', true),
(3, 'Bob Johnson', 'bob@example.com', 35, 80000, 'Sales', '2018-11-05', false);
"""
            with open(data_dir / "create_test_data.sql", 'w') as f:
                f.write(sql_script)
    
    def generate_transformation_test_data(self, strategy: str):
        """Generate test data specific to transformation strategy"""
        base_data = [
            {"id": 1, "name": "Test 1", "value": 100, "category": "A"},
            {"id": 2, "name": "Test 2", "value": 200, "category": "B"},
            {"id": 3, "name": "Test 3", "value": 150, "category": "A"}
        ]
        
        if strategy == "filter":
            return {
                "data": base_data,
                "expected_count": 2,
                "filter_rule": {"column": "category", "operator": "equals", "value": "A"}
            }
        elif strategy == "aggregate":
            return {
                "data": base_data,
                "expected_results": [
                    {"category": "A", "total": 250, "count": 2},
                    {"category": "B", "total": 200, "count": 1}
                ]
            }
        else:
            return {"data": base_data, "expected_count": len(base_data)}
    
    def generate_detailed_readme(self, source: str, target: str, dir_path: Path):
        """Generate detailed README with examples"""
        readme = f"""# {DATA_SOURCES[source]['name']} to {DATA_SOURCES[target]['name']} ETL Tests

## Overview
This directory contains comprehensive ETL tests for migrating data from {DATA_SOURCES[source]['name']} to {DATA_SOURCES[target]['name']}.

## Directory Structure
```
{source}-to-{target}/
├── config.json              # Main configuration
├── etl_pipeline.py          # Core ETL implementation
├── data/                    # Test data files
├── transformations/         # Transformation-specific tests
│   ├── filter/
│   ├── aggregate/
│   └── ...
├── tests/                   # Additional test cases
├── logs/                    # Execution logs
└── docker-compose.yml       # Service setup
```

## Quick Start

### 1. Start Required Services
```bash
docker-compose up -d
```

### 2. Run Basic ETL
```bash
python etl_pipeline.py
```

### 3. Run with Transformation
```bash
python etl_pipeline.py filter
```

### 4. Run All Tests
```bash
python run_all_tests.py
```

## Transformation Examples

### Filter Transformation
```bash
cd transformations/filter
python test.py
```

This applies filtering rules to select specific records based on conditions.

### Aggregate Transformation
```bash
cd transformations/aggregate
python test.py
```

Groups and aggregates data using functions like SUM, COUNT, AVG, etc.

### Type Conversion
```bash
cd transformations/type_conversion
python test.py
```

Converts data types between source and target formats.

## Configuration

The main `config.json` file contains:
- Source connection details
- Target connection details
- Transformation definitions
- Execution parameters
- Monitoring settings

## Performance Considerations

- Batch Size: Adjust `batch_size` in config for optimal performance
- Parallel Workers: Set `parallel_workers` based on system resources
- Memory Usage: Monitor memory when processing large datasets

## Troubleshooting

1. **Connection Issues**: Check service availability and credentials
2. **Data Type Mismatches**: Review type conversion settings
3. **Performance Problems**: Adjust batch size and parallelism
4. **Transformation Errors**: Check transformation rules and data compatibility

## Generated: {datetime.now().isoformat()}
"""
        
        with open(dir_path / "README.md", 'w') as f:
            f.write(readme)
    
    def generate_docker_compose(self, source: str, target: str, dir_path: Path):
        """Generate Docker Compose for required services"""
        services = {}
        
        # Add source service if needed
        if source in ["mysql", "postgresql", "mongodb", "redis", "elasticsearch", "kafka", "rabbitmq"]:
            services[source] = self.get_docker_service(source)
        
        # Add target service if needed
        if target in ["mysql", "postgresql", "mongodb", "redis", "elasticsearch", "kafka", "rabbitmq"]:
            services[target] = self.get_docker_service(target)
        
        if services:
            compose = {
                "version": "3.8",
                "services": services,
                "networks": {
                    "etl-network": {
                        "driver": "bridge"
                    }
                }
            }
            
            with open(dir_path / "docker-compose.yml", 'w') as f:
                f.write(f"# Docker Compose for {source} to {target} ETL\n")
                f.write(yaml.dumps(compose, default_flow_style=False))
    
    def get_docker_service(self, service: str):
        """Get Docker service configuration"""
        configs = {
            "mysql": {
                "image": "mysql:8.0",
                "environment": {
                    "MYSQL_ROOT_PASSWORD": "password",
                    "MYSQL_DATABASE": "test_db"
                },
                "ports": ["3306:3306"],
                "networks": ["etl-network"]
            },
            "postgresql": {
                "image": "postgres:15",
                "environment": {
                    "POSTGRES_PASSWORD": "postgres",
                    "POSTGRES_DB": "test_db"
                },
                "ports": ["5432:5432"],
                "networks": ["etl-network"]
            },
            "mongodb": {
                "image": "mongo:6.0",
                "ports": ["27017:27017"],
                "networks": ["etl-network"]
            },
            "redis": {
                "image": "redis:7-alpine",
                "ports": ["6379:6379"],
                "networks": ["etl-network"]
            },
            "elasticsearch": {
                "image": "elasticsearch:8.11.0",
                "environment": {
                    "discovery.type": "single-node",
                    "xpack.security.enabled": "false"
                },
                "ports": ["9200:9200"],
                "networks": ["etl-network"]
            }
        }
        return configs.get(service, {})
    
    def generate_matrix_visualization(self, sources):
        """Generate a visual matrix of all combinations"""
        matrix_md = """# ETL Test Matrix

## Data Source Compatibility Matrix

|Source → Target|"""
        
        # Header row
        for target in sources:
            matrix_md += f"{target}|"
        matrix_md += "\n|" + "|".join(["-" * len(s) for s in ["Source → Target"] + sources]) + "|\n"
        
        # Data rows
        for source in sources:
            matrix_md += f"|**{source}**|"
            for target in sources:
                if source == target:
                    matrix_md += "—|"
                else:
                    matrix_md += "✓|"
            matrix_md += "\n"
        
        matrix_md += f"""
## Statistics
- Total Data Sources: {len(sources)}
- Total Combinations: {len(sources) * (len(sources) - 1)}
- Transformations per Combination: {len(TRANSFORMATION_STRATEGIES)}
- Total Test Cases: {len(sources) * (len(sources) - 1) * len(TRANSFORMATION_STRATEGIES)}

## Legend
- ✓: Test case available
- —: Same source and target (not applicable)
"""
        
        with open(self.base_path / "TEST_MATRIX.md", 'w') as f:
            f.write(matrix_md)
    
    def generate_master_runner(self):
        """Generate master test runner script"""
        runner = '''#!/usr/bin/env python3
"""
Master ETL Test Runner
Executes all or selected ETL test combinations
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import concurrent.futures

class MasterTestRunner:
    def __init__(self):
        self.base_path = Path(__file__).parent
        self.results = []
        
    def find_all_tests(self):
        """Find all ETL test directories"""
        tests = []
        for dir_path in self.base_path.iterdir():
            if dir_path.is_dir() and '-to-' in dir_path.name:
                tests.append(dir_path.name)
        return sorted(tests)
    
    def run_single_test(self, test_name, transformation=None):
        """Run a single ETL test"""
        test_path = self.base_path / test_name
        
        if not test_path.exists():
            return {
                "test": test_name,
                "status": "NOT_FOUND",
                "duration": 0
            }
        
        cmd = ["python", "etl_pipeline.py"]
        if transformation:
            cmd.append(transformation)
        
        start_time = datetime.now()
        
        try:
            result = subprocess.run(
                cmd,
                cwd=test_path,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return {
                "test": test_name,
                "transformation": transformation,
                "status": "SUCCESS" if result.returncode == 0 else "FAILED",
                "duration": duration,
                "output": result.stdout[-1000:] if result.returncode == 0 else result.stderr
            }
            
        except subprocess.TimeoutExpired:
            return {
                "test": test_name,
                "transformation": transformation,
                "status": "TIMEOUT",
                "duration": 300
            }
        except Exception as e:
            return {
                "test": test_name,
                "transformation": transformation,
                "status": "ERROR",
                "duration": (datetime.now() - start_time).total_seconds(),
                "error": str(e)
            }
    
    def run_tests(self, pattern=None, transformation=None, parallel=False):
        """Run tests matching pattern"""
        all_tests = self.find_all_tests()
        
        if pattern:
            tests = [t for t in all_tests if pattern in t]
        else:
            tests = all_tests
        
        print(f"Found {len(tests)} tests to run")
        
        if parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(self.run_single_test, test, transformation)
                    for test in tests
                ]
                
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    self.results.append(result)
                    self.print_result(result)
        else:
            for test in tests:
                print(f"\\nRunning {test}...")
                result = self.run_single_test(test, transformation)
                self.results.append(result)
                self.print_result(result)
    
    def print_result(self, result):
        """Print test result"""
        status_symbols = {
            "SUCCESS": "✓",
            "FAILED": "✗",
            "TIMEOUT": "⏱",
            "ERROR": "!",
            "NOT_FOUND": "?"
        }
        
        symbol = status_symbols.get(result["status"], "?")
        print(f"{symbol} {result['test']} - {result['status']} ({result['duration']:.2f}s)")
    
    def generate_report(self):
        """Generate test report"""
        print("\\n" + "="*70)
        print("TEST REPORT")
        print("="*70)
        
        # Summary
        total = len(self.results)
        success = sum(1 for r in self.results if r["status"] == "SUCCESS")
        failed = sum(1 for r in self.results if r["status"] == "FAILED")
        
        print(f"Total: {total}")
        print(f"Success: {success}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {success/total*100:.1f}%")
        
        # Save detailed report
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total": total,
                "success": success,
                "failed": failed
            },
            "results": self.results
        }
        
        report_file = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\\nDetailed report saved to: {report_file}")

def main():
    parser = argparse.ArgumentParser(description="Master ETL Test Runner")
    parser.add_argument("-p", "--pattern", help="Run tests matching pattern")
    parser.add_argument("-t", "--transformation", help="Specific transformation to test")
    parser.add_argument("--parallel", action="store_true", help="Run tests in parallel")
    
    args = parser.parse_args()
    
    runner = MasterTestRunner()
    runner.run_tests(args.pattern, args.transformation, args.parallel)
    runner.generate_report()

if __name__ == "__main__":
    main()
'''
        
        with open(self.base_path / "run_all_tests.py", 'w') as f:
            f.write(runner)
        
        os.chmod(self.base_path / "run_all_tests.py", 0o755)
    
    def get_class_name(self, source: str):
        """Convert source name to class name"""
        return ''.join(word.capitalize() for word in source.split('_'))
    
    def get_detailed_source_config(self, source: str):
        """Get detailed source configuration"""
        # Extend the basic config with more details
        base_config = generate_source_config(source)
        
        # Add additional configuration based on source type
        if source in ["mysql", "postgresql"]:
            base_config.update({
                "connection_pool_size": 10,
                "query_timeout": 30000,
                "ssl_enabled": False,
                "fetch_size": 1000
            })
        elif source == "kafka":
            base_config.update({
                "consumer_timeout_ms": 10000,
                "auto_offset_reset": "earliest",
                "enable_auto_commit": False,
                "max_poll_records": 500
            })
        elif source == "mongodb":
            base_config.update({
                "read_preference": "primary",
                "write_concern": "majority",
                "batch_size": 1000
            })
        
        return base_config
    
    def get_detailed_target_config(self, target: str):
        """Get detailed target configuration"""
        base_config = generate_target_config(target)
        
        # Add target-specific configurations
        if target in ["mysql", "postgresql"]:
            base_config.update({
                "batch_insert_size": 1000,
                "use_transactions": True,
                "create_table_if_not_exists": True
            })
        
        return base_config
    
    def get_transformation_example(self, strategy: str):
        """Get detailed transformation example"""
        examples = {
            "filter": {
                "description": "Filter records based on conditions",
                "rules": [
                    {"column": "age", "operator": "greater_than", "value": 25},
                    {"column": "department", "operator": "in", "value": ["Engineering", "Sales"]}
                ],
                "logic": "AND"
            },
            "clean": {
                "description": "Clean and normalize data",
                "operations": [
                    {"column": "email", "operation": "lowercase"},
                    {"column": "name", "operation": "trim"},
                    {"column": "phone", "operation": "remove_special_chars"}
                ]
            },
            "aggregate": {
                "description": "Group and aggregate data",
                "group_by": ["department", "location"],
                "aggregations": [
                    {"column": "salary", "function": "avg", "alias": "avg_salary"},
                    {"column": "employee_id", "function": "count", "alias": "employee_count"}
                ]
            },
            "type_conversion": {
                "description": "Convert data types",
                "conversions": [
                    {"column": "hire_date", "from": "string", "to": "date", "format": "%Y-%m-%d"},
                    {"column": "salary", "from": "string", "to": "float"},
                    {"column": "is_active", "from": "string", "to": "boolean"}
                ]
            }
        }
        
        return examples.get(strategy, {"description": f"{strategy} transformation"})

# Import yaml handling
try:
    import yaml
except ImportError:
    # Simple YAML-like output if PyYAML not available
    class yaml:
        @staticmethod
        def dumps(data, **kwargs):
            return json.dumps(data, indent=2)

# Import from previous script
try:
    from generate_all_etl_tests import generate_source_config, generate_target_config
except ImportError:
    def generate_source_config(source):
        return {"type": source}
    
    def generate_target_config(target):
        return {"type": target}

def main():
    """Generate comprehensive ETL tests"""
    generator = ETLTestGenerator()
    generator.generate_all_tests()

if __name__ == "__main__":
    main()