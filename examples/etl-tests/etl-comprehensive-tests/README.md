# Comprehensive ETL Tests

This directory contains ETL test examples for all possible data source combinations.

## Statistics
- Total data sources: 16
- Total combinations: 240
- Transformation strategies per combination: 12
- Total test cases: 2880

## Data Sources

### Implemented
- **MySQL** (relational)
- **PostgreSQL** (relational)
- **SQLite** (relational)
- **Microsoft SQL Server** (relational)
- **Oracle** (relational)
- **MongoDB** (nosql)
- **Redis** (nosql)
- **Elasticsearch** (nosql)
- **Apache Kafka** (streaming)
- **RabbitMQ** (streaming)
- **CSV** (file)
- **Excel** (file)
- **JSON** (file)
- **Apache Parquet** (file)
- **Amazon S3** (cloud)
- **REST API** (api)

### Not Implemented
- GraphQL (defined in enum but no connector)

## Transformation Strategies
- Filter
- Clean
- Aggregate
- Join
- Split Column
- Merge Column
- Custom Sql
- Custom Python
- Type Conversion
- Rename
- Drop
- Fill Null

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
