"""
Go Code Generator
Generates Go code from SQL queries using various frameworks
"""

from typing import Dict, List, Any, Optional
from app.services.sql_parser import ParsedQuery, QueryType, ColumnReference


class GoCodeGenerator:
    """Generates Go code from parsed SQL queries"""
    
    def generate_all_frameworks(self, query: ParsedQuery) -> Dict[str, str]:
        """Generate code for all Go frameworks"""
        return {
            "database_sql": self.generate_database_sql(query),
            "sqlx": self.generate_sqlx(query),
            "gorm": self.generate_gorm(query),
            "squirrel": self.generate_squirrel(query),
            "ent": self.generate_ent(query)
        }
    
    def generate_database_sql(self, query: ParsedQuery) -> str:
        """Generate standard database/sql code"""
        code = []
        code.append("package main")
        code.append("")
        code.append("import (")
        code.append("    \"database/sql\"")
        code.append("    \"fmt\"")
        code.append("    \"log\"")
        code.append("    _ \"github.com/lib/pq\" // PostgreSQL driver")
        code.append(")")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.extend(self._generate_database_sql_select(query))
        elif query.query_type == QueryType.INSERT:
            code.extend(self._generate_database_sql_insert(query))
        elif query.query_type == QueryType.UPDATE:
            code.extend(self._generate_database_sql_update(query))
        elif query.query_type == QueryType.DELETE:
            code.extend(self._generate_database_sql_delete(query))
        
        return "\n".join(code)
    
    def _generate_database_sql_select(self, query: ParsedQuery) -> List[str]:
        """Generate database/sql SELECT code"""
        code = []
        
        # Generate struct based on columns
        if query.tables:
            struct_name = self._to_pascal_case(query.tables[0].name)
            code.append(f"type {struct_name} struct {{")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and not col.is_aggregate:
                        field_name = self._to_pascal_case(col.name)
                        code.append(f"    {field_name} string `json:\"{col.name}\" db:\"{col.name}\"`")
            else:
                code.append("    ID   int    `json:\"id\" db:\"id\"`")
                code.append("    Name string `json:\"name\" db:\"name\"`")
            
            code.append("}")
            code.append("")
            
            code.append(f"func query{struct_name}Data(db *sql.DB) ([]{struct_name}, error) {{")
            code.append(f"    query := `{query.original_sql}`")
            code.append("    ")
            code.append("    rows, err := db.Query(query)")
            code.append("    if err != nil {")
            code.append("        return nil, fmt.Errorf(\"query failed: %w\", err)")
            code.append("    }")
            code.append("    defer rows.Close()")
            code.append("    ")
            code.append(f"    var results []{struct_name}")
            code.append("    for rows.Next() {")
            code.append(f"        var item {struct_name}")
            
            # Generate scan parameters
            if query.columns:
                scan_fields = []
                for col in query.columns:
                    if col.name != "*" and not col.is_aggregate:
                        scan_fields.append(f"&item.{self._to_pascal_case(col.name)}")
                
                if scan_fields:
                    code.append(f"        err := rows.Scan({', '.join(scan_fields)})")
                else:
                    code.append("        err := rows.Scan(&item.ID, &item.Name)")
            else:
                code.append("        err := rows.Scan(&item.ID, &item.Name)")
            
            code.append("        if err != nil {")
            code.append("            return nil, fmt.Errorf(\"scan failed: %w\", err)")
            code.append("        }")
            code.append("        results = append(results, item)")
            code.append("    }")
            code.append("    ")
            code.append("    if err := rows.Err(); err != nil {")
            code.append("        return nil, fmt.Errorf(\"rows iteration failed: %w\", err)")
            code.append("    }")
            code.append("    ")
            code.append("    return results, nil")
            code.append("}")
        
        code.append("")
        code.append("func main() {")
        code.append("    db, err := sql.Open(\"postgres\", \"user=username dbname=mydb sslmode=disable\")")
        code.append("    if err != nil {")
        code.append("        log.Fatal(err)")
        code.append("    }")
        code.append("    defer db.Close()")
        code.append("    ")
        if query.tables:
            code.append(f"    results, err := query{self._to_pascal_case(query.tables[0].name)}Data(db)")
        else:
            code.append("    // results, err := queryData(db)")
        code.append("    if err != nil {")
        code.append("        log.Fatal(err)")
        code.append("    }")
        code.append("    ")
        code.append("    fmt.Printf(\"Found %d results\\n\", len(results))")
        code.append("}")
        
        return code
    
    def _generate_database_sql_insert(self, query: ParsedQuery) -> List[str]:
        """Generate database/sql INSERT code"""
        code = []
        code.append("func insertData(db *sql.DB, data ...interface{}) error {")
        code.append(f"    query := `{query.original_sql}`")
        code.append("    ")
        code.append("    _, err := db.Exec(query, data...)")
        code.append("    if err != nil {")
        code.append("        return fmt.Errorf(\"insert failed: %w\", err)")
        code.append("    }")
        code.append("    ")
        code.append("    return nil")
        code.append("}")
        
        return code
    
    def _generate_database_sql_update(self, query: ParsedQuery) -> List[str]:
        """Generate database/sql UPDATE code"""
        code = []
        code.append("func updateData(db *sql.DB, data ...interface{}) (int64, error) {")
        code.append(f"    query := `{query.original_sql}`")
        code.append("    ")
        code.append("    result, err := db.Exec(query, data...)")
        code.append("    if err != nil {")
        code.append("        return 0, fmt.Errorf(\"update failed: %w\", err)")
        code.append("    }")
        code.append("    ")
        code.append("    rowsAffected, err := result.RowsAffected()")
        code.append("    if err != nil {")
        code.append("        return 0, fmt.Errorf(\"failed to get rows affected: %w\", err)")
        code.append("    }")
        code.append("    ")
        code.append("    return rowsAffected, nil")
        code.append("}")
        
        return code
    
    def _generate_database_sql_delete(self, query: ParsedQuery) -> List[str]:
        """Generate database/sql DELETE code"""
        code = []
        code.append("func deleteData(db *sql.DB, data ...interface{}) (int64, error) {")
        code.append(f"    query := `{query.original_sql}`")
        code.append("    ")
        code.append("    result, err := db.Exec(query, data...)")
        code.append("    if err != nil {")
        code.append("        return 0, fmt.Errorf(\"delete failed: %w\", err)")
        code.append("    }")
        code.append("    ")
        code.append("    rowsAffected, err := result.RowsAffected()")
        code.append("    if err != nil {")
        code.append("        return 0, fmt.Errorf(\"failed to get rows affected: %w\", err)")
        code.append("    }")
        code.append("    ")
        code.append("    return rowsAffected, nil")
        code.append("}")
        
        return code
    
    def generate_sqlx(self, query: ParsedQuery) -> str:
        """Generate sqlx code"""
        code = []
        code.append("package main")
        code.append("")
        code.append("import (")
        code.append("    \"fmt\"")
        code.append("    \"log\"")
        code.append("    \"github.com/jmoiron/sqlx\"")
        code.append("    _ \"github.com/lib/pq\"")
        code.append(")")
        code.append("")
        
        if query.tables:
            struct_name = self._to_pascal_case(query.tables[0].name)
            code.append(f"type {struct_name} struct {{")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and not col.is_aggregate:
                        field_name = self._to_pascal_case(col.name)
                        code.append(f"    {field_name} string `db:\"{col.name}\" json:\"{col.name}\"`")
            else:
                code.append("    ID   int    `db:\"id\" json:\"id\"`")
                code.append("    Name string `db:\"name\" json:\"name\"`")
            
            code.append("}")
            code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("func queryData(db *sqlx.DB) ([]" + (struct_name if query.tables else "map[string]interface{}") + ", error) {")
            code.append(f"    query := `{query.original_sql}`")
            code.append("    ")
            
            if query.tables:
                code.append(f"    var results []{struct_name}")
                code.append("    err := db.Select(&results, query)")
            else:
                code.append("    var results []map[string]interface{}")
                code.append("    rows, err := db.Queryx(query)")
                code.append("    if err != nil {")
                code.append("        return nil, err")
                code.append("    }")
                code.append("    defer rows.Close()")
                code.append("    ")
                code.append("    for rows.Next() {")
                code.append("        row := make(map[string]interface{})")
                code.append("        err := rows.MapScan(row)")
                code.append("        if err != nil {")
                code.append("            return nil, err")
                code.append("        }")
                code.append("        results = append(results, row)")
                code.append("    }")
                code.append("    err = rows.Err()")
            
            code.append("    if err != nil {")
            code.append("        return nil, fmt.Errorf(\"query failed: %w\", err)")
            code.append("    }")
            code.append("    ")
            code.append("    return results, nil")
            code.append("}")
        
        elif query.query_type in [QueryType.INSERT, QueryType.UPDATE, QueryType.DELETE]:
            code.append("func executeQuery(db *sqlx.DB, args ...interface{}) (int64, error) {")
            code.append(f"    query := `{query.original_sql}`")
            code.append("    ")
            code.append("    result, err := db.Exec(query, args...)")
            code.append("    if err != nil {")
            code.append("        return 0, fmt.Errorf(\"execution failed: %w\", err)")
            code.append("    }")
            code.append("    ")
            code.append("    rowsAffected, err := result.RowsAffected()")
            code.append("    if err != nil {")
            code.append("        return 0, fmt.Errorf(\"failed to get rows affected: %w\", err)")
            code.append("    }")
            code.append("    ")
            code.append("    return rowsAffected, nil")
            code.append("}")
        
        code.append("")
        code.append("func main() {")
        code.append("    db, err := sqlx.Connect(\"postgres\", \"user=username dbname=mydb sslmode=disable\")")
        code.append("    if err != nil {")
        code.append("        log.Fatal(err)")
        code.append("    }")
        code.append("    defer db.Close()")
        code.append("    ")
        
        if query.query_type == QueryType.SELECT:
            code.append("    results, err := queryData(db)")
        else:
            code.append("    rowsAffected, err := executeQuery(db)")
        
        code.append("    if err != nil {")
        code.append("        log.Fatal(err)")
        code.append("    }")
        code.append("    ")
        
        if query.query_type == QueryType.SELECT:
            code.append("    fmt.Printf(\"Found %d results\\n\", len(results))")
        else:
            code.append("    fmt.Printf(\"Rows affected: %d\\n\", rowsAffected)")
        
        code.append("}")
        
        return "\n".join(code)
    
    def generate_gorm(self, query: ParsedQuery) -> str:
        """Generate GORM code"""
        code = []
        code.append("package main")
        code.append("")
        code.append("import (")
        code.append("    \"fmt\"")
        code.append("    \"log\"")
        code.append("    \"gorm.io/driver/postgres\"")
        code.append("    \"gorm.io/gorm\"")
        code.append(")")
        code.append("")
        
        if query.tables:
            struct_name = self._to_pascal_case(query.tables[0].name)
            code.append(f"type {struct_name} struct {{")
            code.append("    ID uint `gorm:\"primaryKey\" json:\"id\"`")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and col.name.lower() != "id" and not col.is_aggregate:
                        field_name = self._to_pascal_case(col.name)
                        code.append(f"    {field_name} string `gorm:\"column:{col.name}\" json:\"{col.name}\"`")
            else:
                code.append("    Name string `gorm:\"column:name\" json:\"name\"`")
            
            code.append("}")
            code.append("")
            code.append(f"func ({struct_name}) TableName() string {{")
            code.append(f"    return \"{query.tables[0].name}\"")
            code.append("}")
            code.append("")
        
        if query.query_type == QueryType.SELECT:
            if query.tables:
                code.append(f"func queryData(db *gorm.DB) ([]{struct_name}, error) {{")
                code.append(f"    var results []{struct_name}")
                code.append("    ")
                code.append("    // Method 1: Using Raw SQL")
                code.append(f"    err := db.Raw(`{query.original_sql}`).Scan(&results).Error")
                code.append("    if err != nil {")
                code.append("        return nil, err")
                code.append("    }")
                code.append("    ")
                code.append("    // Method 2: Using GORM methods (example)")
                code.append(f"    // var gormResults []{struct_name}")
                code.append(f"    // err = db.Model(&{struct_name}{{}}).Find(&gormResults).Error")
                
                # Add WHERE conditions if simple
                if query.where_conditions:
                    for condition in query.where_conditions:
                        code.append(f"    // err = db.Where(\"{condition.column} {condition.operator} ?\", value).Find(&gormResults).Error")
                
                # Add ORDER BY
                if query.order_by:
                    for order in query.order_by:
                        code.append(f"    // err = db.Order(\"{order.column} {order.direction}\").Find(&gormResults).Error")
                
                # Add LIMIT
                if query.limit:
                    code.append(f"    // err = db.Limit({query.limit}).Find(&gormResults).Error")
                
                code.append("    ")
                code.append("    return results, nil")
                code.append("}")
        
        elif query.query_type == QueryType.INSERT:
            if query.tables:
                code.append(f"func insertData(db *gorm.DB, data *{struct_name}) error {{")
                code.append("    return db.Create(data).Error")
                code.append("}")
        
        elif query.query_type == QueryType.UPDATE:
            if query.tables:
                code.append(f"func updateData(db *gorm.DB, data *{struct_name}) error {{")
                code.append("    return db.Save(data).Error")
                code.append("}")
        
        elif query.query_type == QueryType.DELETE:
            if query.tables:
                code.append(f"func deleteData(db *gorm.DB, id uint) error {{")
                code.append(f"    return db.Delete(&{struct_name}{{}}, id).Error")
                code.append("}")
        
        code.append("")
        code.append("func main() {")
        code.append("    dsn := \"host=localhost user=username password=password dbname=mydb port=5432 sslmode=disable\"")
        code.append("    db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})")
        code.append("    if err != nil {")
        code.append("        log.Fatal(err)")
        code.append("    }")
        code.append("    ")
        
        if query.query_type == QueryType.SELECT and query.tables:
            code.append("    results, err := queryData(db)")
            code.append("    if err != nil {")
            code.append("        log.Fatal(err)")
            code.append("    }")
            code.append("    ")
            code.append("    fmt.Printf(\"Found %d results\\n\", len(results))")
        
        code.append("}")
        
        return "\n".join(code)
    
    def generate_squirrel(self, query: ParsedQuery) -> str:
        """Generate Squirrel query builder code"""
        code = []
        code.append("package main")
        code.append("")
        code.append("import (")
        code.append("    \"database/sql\"")
        code.append("    \"fmt\"")
        code.append("    \"log\"")
        code.append("    \"github.com/Masterminds/squirrel\"")
        code.append("    _ \"github.com/lib/pq\"")
        code.append(")")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("func queryData(db *sql.DB) error {")
            code.append("    psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)")
            code.append("    ")
            
            if query.tables:
                table_name = query.tables[0].name
                code.append(f"    query := psql.Select(")
                
                if query.columns and len(query.columns) > 0:
                    select_columns = []
                    for col in query.columns:
                        if col.name == "*":
                            select_columns = ["\"*\""]
                            break
                        else:
                            select_columns.append(f"\"{col.name}\"")
                    code.append(f"        {', '.join(select_columns)},")
                else:
                    code.append("        \"*\",")
                
                code.append(f"    ).From(\"{table_name}\")")
                
                # Add WHERE conditions
                if query.where_conditions:
                    for condition in query.where_conditions:
                        code.append(f"    query = query.Where(squirrel.Eq{{\"{condition.column}\": \"value\"}})")
                
                # Add ORDER BY
                if query.order_by:
                    for order in query.order_by:
                        code.append(f"    query = query.OrderBy(\"{order.column} {order.direction}\")")
                
                # Add LIMIT
                if query.limit:
                    code.append(f"    query = query.Limit({query.limit})")
            
            code.append("    ")
            code.append("    sql, args, err := query.ToSql()")
            code.append("    if err != nil {")
            code.append("        return fmt.Errorf(\"failed to build query: %w\", err)")
            code.append("    }")
            code.append("    ")
            code.append("    fmt.Printf(\"Generated SQL: %s\\n\", sql)")
            code.append("    fmt.Printf(\"Args: %v\\n\", args)")
            code.append("    ")
            code.append("    rows, err := db.Query(sql, args...)")
            code.append("    if err != nil {")
            code.append("        return fmt.Errorf(\"query failed: %w\", err)")
            code.append("    }")
            code.append("    defer rows.Close()")
            code.append("    ")
            code.append("    // Process rows...")
            code.append("    return nil")
            code.append("}")
        
        elif query.query_type == QueryType.INSERT:
            code.append("func insertData(db *sql.DB) error {")
            code.append("    psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)")
            code.append("    ")
            if query.tables:
                code.append(f"    query := psql.Insert(\"{query.tables[0].name}\")")
                code.append("    query = query.Columns(\"column1\", \"column2\").Values(\"value1\", \"value2\")")
            
            code.append("    ")
            code.append("    sql, args, err := query.ToSql()")
            code.append("    if err != nil {")
            code.append("        return fmt.Errorf(\"failed to build query: %w\", err)")
            code.append("    }")
            code.append("    ")
            code.append("    _, err = db.Exec(sql, args...)")
            code.append("    return err")
            code.append("}")
        
        code.append("")
        code.append("func main() {")
        code.append("    db, err := sql.Open(\"postgres\", \"user=username dbname=mydb sslmode=disable\")")
        code.append("    if err != nil {")
        code.append("        log.Fatal(err)")
        code.append("    }")
        code.append("    defer db.Close()")
        code.append("    ")
        
        if query.query_type == QueryType.SELECT:
            code.append("    err = queryData(db)")
        elif query.query_type == QueryType.INSERT:
            code.append("    err = insertData(db)")
        
        code.append("    if err != nil {")
        code.append("        log.Fatal(err)")
        code.append("    }")
        code.append("}")
        
        return "\n".join(code)
    
    def generate_ent(self, query: ParsedQuery) -> str:
        """Generate Ent framework code"""
        code = []
        code.append("// Ent Schema Definition")
        code.append("// Run: go run -mod=mod entgo.io/ent/cmd/ent init YourEntity")
        code.append("")
        
        if query.tables:
            struct_name = self._to_pascal_case(query.tables[0].name)
            code.append("// ent/schema/" + struct_name.lower() + ".go")
            code.append("package schema")
            code.append("")
            code.append("import (")
            code.append("    \"entgo.io/ent\"")
            code.append("    \"entgo.io/ent/schema/field\"")
            code.append(")")
            code.append("")
            code.append(f"type {struct_name} struct {{")
            code.append("    ent.Schema")
            code.append("}")
            code.append("")
            code.append(f"func ({struct_name}) Fields() []ent.Field {{")
            code.append("    return []ent.Field{")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and col.name.lower() != "id" and not col.is_aggregate:
                        code.append(f"        field.String(\"{col.name}\"),")
            else:
                code.append("        field.String(\"name\"),")
            
            code.append("    }")
            code.append("}")
            code.append("")
            code.append("// Usage in main application:")
            code.append("/*")
            code.append("package main")
            code.append("")
            code.append("import (")
            code.append("    \"context\"")
            code.append("    \"fmt\"")
            code.append("    \"log\"")
            code.append("    \"your-app/ent\"")
            code.append("    _ \"github.com/lib/pq\"")
            code.append(")")
            code.append("")
            code.append("func main() {")
            code.append("    client, err := ent.Open(\"postgres\", \"host=localhost port=5432 user=postgres dbname=test sslmode=disable\")")
            code.append("    if err != nil {")
            code.append("        log.Fatalf(\"failed opening connection to postgres: %v\", err)")
            code.append("    }")
            code.append("    defer client.Close()")
            code.append("    ")
            code.append("    // Run the auto migration tool.")
            code.append("    if err := client.Schema.Create(context.Background()); err != nil {")
            code.append("        log.Fatalf(\"failed creating schema resources: %v\", err)")
            code.append("    }")
            code.append("    ")
            
            if query.query_type == QueryType.SELECT:
                code.append(f"    // Query {struct_name}")
                code.append(f"    entities, err := client.{struct_name}.")
                code.append("        Query().")
                
                if query.where_conditions:
                    for condition in query.where_conditions:
                        field_name = self._to_pascal_case(condition.column)
                        code.append(f"        Where({struct_name.lower()}.{field_name}EQ(\"value\")).")
                
                if query.order_by:
                    for order in query.order_by:
                        field_name = self._to_pascal_case(order.column)
                        if order.direction.upper() == "DESC":
                            code.append(f"        Order(ent.Desc({struct_name.lower()}.Field{field_name})).")
                        else:
                            code.append(f"        Order(ent.Asc({struct_name.lower()}.Field{field_name})).")
                
                if query.limit:
                    code.append(f"        Limit({query.limit}).")
                
                code.append("        All(context.Background())")
            
            code.append("    if err != nil {")
            code.append("        log.Fatal(err)")
            code.append("    }")
            code.append("    ")
            code.append("    fmt.Printf(\"Found %d entities\\n\", len(entities))")
            code.append("}")
            code.append("*/")
        
        return "\n".join(code)
    
    def _to_pascal_case(self, snake_str: str) -> str:
        """Convert snake_case to PascalCase"""
        components = snake_str.split('_')
        return ''.join(word.capitalize() for word in components)
    
    def _to_camel_case(self, snake_str: str) -> str:
        """Convert snake_case to camelCase"""
        components = snake_str.split('_')
        return components[0].lower() + ''.join(word.capitalize() for word in components[1:])


# Global instance
go_generator = GoCodeGenerator()