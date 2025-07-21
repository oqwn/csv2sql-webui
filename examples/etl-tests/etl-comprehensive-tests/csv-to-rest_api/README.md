# CSV to REST API ETL Test

This test demonstrates ETL from CSV (file) to REST API (api).

## Source: CSV
- Category: file
- Real-time Support: False
- Incremental Support: False
- Connector: csv_importer.py

## Target: REST API
- Category: api
- Real-time Support: False
- Incremental Support: True
- Connector: api_connector.py

## Transformation Strategies

This test includes examples for all transformation strategies:

### Filter
- Location: `./filter/`
- Configuration: `./filter/config.json`
- Test Script: `./filter/run_test.py`

### Clean
- Location: `./clean/`
- Configuration: `./clean/config.json`
- Test Script: `./clean/run_test.py`

### Aggregate
- Location: `./aggregate/`
- Configuration: `./aggregate/config.json`
- Test Script: `./aggregate/run_test.py`

### Join
- Location: `./join/`
- Configuration: `./join/config.json`
- Test Script: `./join/run_test.py`

### Split Column
- Location: `./split_column/`
- Configuration: `./split_column/config.json`
- Test Script: `./split_column/run_test.py`

### Merge Column
- Location: `./merge_column/`
- Configuration: `./merge_column/config.json`
- Test Script: `./merge_column/run_test.py`

### Custom Sql
- Location: `./custom_sql/`
- Configuration: `./custom_sql/config.json`
- Test Script: `./custom_sql/run_test.py`

### Custom Python
- Location: `./custom_python/`
- Configuration: `./custom_python/config.json`
- Test Script: `./custom_python/run_test.py`

### Type Conversion
- Location: `./type_conversion/`
- Configuration: `./type_conversion/config.json`
- Test Script: `./type_conversion/run_test.py`

### Rename
- Location: `./rename/`
- Configuration: `./rename/config.json`
- Test Script: `./rename/run_test.py`

### Drop
- Location: `./drop/`
- Configuration: `./drop/config.json`
- Test Script: `./drop/run_test.py`

### Fill Null
- Location: `./fill_null/`
- Configuration: `./fill_null/config.json`
- Test Script: `./fill_null/run_test.py`

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
