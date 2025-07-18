# Sample Files for Testing CSV2SQL WebUI

This directory contains various sample files to test the import functionality of the CSV2SQL Web UI application.

## 📁 Directory Structure

- **`csv/`** - CSV files for testing CSV import functionality
- **`excel/`** - Excel files (.xlsx) for testing Excel import functionality

## 📊 Excel Files

See `excel/README.md` for detailed information about Excel test files including:
- **employees.xlsx** - Basic employee data with mixed data types
- **sales_data.xlsx** - 500 sales records for performance testing  
- **inventory.xlsx** - Product inventory with supplier information
- **financial_report.xlsx** - Multi-sheet financial data (3 sheets)
- **data_types_test.xlsx** - Comprehensive data type testing
- **special_characters.xlsx** - Unicode and special character handling

## 📄 CSV Files

## Test Files

### 1. `users_with_valid_id.csv`
- **Purpose**: Test standard CSV import with valid numeric ID column
- **Expected behavior**: ID column should be used as BIGINT PRIMARY KEY
- **Features tested**: 
  - Numeric ID detection
  - Boolean value detection (true/false)
  - Timestamp parsing

### 2. `products_with_text_id.csv`
- **Purpose**: Test CSV with text-based ID column
- **Expected behavior**: Original 'id' column renamed to 'id_original', new BIGSERIAL id created
- **Features tested**:
  - Invalid ID column handling
  - Text ID preservation
  - Decimal/price column detection

### 3. `orders_no_id.csv`
- **Purpose**: Test CSV without any ID column
- **Expected behavior**: Auto-generate BIGSERIAL PRIMARY KEY named 'id'
- **Features tested**:
  - Auto ID generation
  - Address with commas in quotes
  - Date parsing

### 4. `employees_special_chars.csv`
- **Purpose**: Test column names with special characters and spaces
- **Expected behavior**: Column names sanitized (spaces → underscores, special chars removed)
- **Features tested**:
  - Special character handling in column names
  - Unicode character support in data
  - Currency symbols
  - Yes/No boolean detection

### 5. `data_types_test.csv`
- **Purpose**: Test various data type detection
- **Expected behavior**: Correct type detection for each column
- **Features tested**:
  - Integer types (SMALLINT, INTEGER, BIGINT)
  - Float/decimal detection
  - Boolean variations (true/false, TRUE/FALSE)
  - Date vs DateTime detection
  - JSON data handling
  - NULL value handling

### 6. `mixed_id_values.csv`
- **Purpose**: Test ID column with mixed valid/invalid values
- **Expected behavior**: ID column renamed to 'id_original', new BIGSERIAL created
- **Features tested**:
  - Partial invalid ID detection
  - Empty ID values
  - Mixed numeric/text IDs

### 7. `empty_columns.csv`
- **Purpose**: Test columns with many NULL values
- **Expected behavior**: Columns marked as nullable, proper type detection despite NULLs
- **Features tested**:
  - Nullable column detection
  - Type inference with sparse data

### 8. `large_numbers.csv`
- **Purpose**: Test large numeric values within BIGINT range
- **Expected behavior**: BIGINT for large IDs, proper numeric type for amounts
- **Features tested**:
  - BIGINT validation (max: 9,223,372,036,854,775,807)
  - Large transaction IDs
  - Decimal precision

### 9. `ultra_large_numbers.csv`
- **Purpose**: Test numeric values that exceed BIGINT range
- **Expected behavior**: NUMERIC type for values exceeding BIGINT limits
- **Features tested**:
  - Ultra-large number handling (> 9.22e18)
  - NUMERIC type detection
  - Precision preservation for huge numbers

## Usage

1. Navigate to the SQL Editor page
2. Go to the "Import Data" tab
3. Upload any of these CSV files
4. Test both automatic import and "Preview and edit CREATE TABLE statement" options
5. Verify the generated table structure matches expectations

## Expected CREATE TABLE Examples

### For `users_with_valid_id.csv`:
```sql
CREATE TABLE IF NOT EXISTS "users_with_valid_id" (
    "id" BIGINT PRIMARY KEY,
    "username" VARCHAR(255),
    "email" VARCHAR(255),
    "created_at" TIMESTAMP,
    "is_active" BOOLEAN
);
```

### For `products_with_text_id.csv`:
```sql
CREATE TABLE IF NOT EXISTS "products_with_text_id" (
    "id" BIGSERIAL PRIMARY KEY,
    "id_original" TEXT,
    "product_name" VARCHAR(255),
    "category" VARCHAR(255),
    "price" DOUBLE PRECISION,
    "stock_quantity" INTEGER,
    "description" TEXT
);
```

### For `orders_no_id.csv`:
```sql
CREATE TABLE IF NOT EXISTS "orders_no_id" (
    "id" BIGSERIAL PRIMARY KEY,
    "customer_name" VARCHAR(255),
    "order_date" DATE,
    "total_amount" DOUBLE PRECISION,
    "status" VARCHAR(255),
    "shipping_address" TEXT
);
```