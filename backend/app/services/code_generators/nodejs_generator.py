"""
Node.js Code Generator
Generates Node.js/JavaScript/TypeScript code from SQL queries using various frameworks
"""

from typing import Dict, List, Any, Optional
from app.services.sql_parser import ParsedQuery, QueryType, ColumnReference


class NodeJSCodeGenerator:
    """Generates Node.js code from parsed SQL queries"""
    
    def generate_all_frameworks(self, query: ParsedQuery) -> Dict[str, str]:
        """Generate code for all Node.js frameworks"""
        return {
            "raw_nodejs": self.generate_raw_nodejs(query),
            "sequelize": self.generate_sequelize(query),
            "typeorm": self.generate_typeorm(query),
            "knex": self.generate_knex(query),
            "prisma": self.generate_prisma(query),
            "mongoose": self.generate_mongoose(query)
        }
    
    def generate_raw_nodejs(self, query: ParsedQuery) -> str:
        """Generate raw Node.js with pg/mysql2"""
        code = []
        code.append("const { Pool } = require('pg');")
        code.append("// Alternative: const mysql = require('mysql2/promise');")
        code.append("")
        code.append("const pool = new Pool({")
        code.append("  user: 'your_username',")
        code.append("  host: 'localhost',")
        code.append("  database: 'your_database',")
        code.append("  password: 'your_password',")
        code.append("  port: 5432,")
        code.append("});")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.extend(self._generate_raw_nodejs_select(query))
        elif query.query_type == QueryType.INSERT:
            code.extend(self._generate_raw_nodejs_insert(query))
        elif query.query_type == QueryType.UPDATE:
            code.extend(self._generate_raw_nodejs_update(query))
        elif query.query_type == QueryType.DELETE:
            code.extend(self._generate_raw_nodejs_delete(query))
        
        return "\n".join(code)
    
    def _generate_raw_nodejs_select(self, query: ParsedQuery) -> List[str]:
        """Generate raw Node.js SELECT code"""
        code = []
        code.append("async function queryData(params = {}) {")
        code.append("  try {")
        code.append(f"    const sql = `{query.original_sql}`;")
        code.append("    const result = await pool.query(sql, Object.values(params));")
        code.append("    return result.rows;")
        code.append("  } catch (error) {")
        code.append("    console.error('Database query error:', error);")
        code.append("    throw error;")
        code.append("  }")
        code.append("}")
        code.append("")
        code.append("// Usage:")
        code.append("// queryData().then(rows => {")
        code.append("//   console.log('Found', rows.length, 'rows');")
        code.append("//   console.log(rows);")
        code.append("// }).catch(console.error);")
        
        return code
    
    def _generate_raw_nodejs_insert(self, query: ParsedQuery) -> List[str]:
        """Generate raw Node.js INSERT code"""
        code = []
        code.append("async function insertData(data) {")
        code.append("  try {")
        code.append(f"    const sql = `{query.original_sql}`;")
        code.append("    const result = await pool.query(sql, Array.isArray(data) ? data : Object.values(data));")
        code.append("    return result.rowCount;")
        code.append("  } catch (error) {")
        code.append("    console.error('Database insert error:', error);")
        code.append("    throw error;")
        code.append("  }")
        code.append("}")
        
        return code
    
    def _generate_raw_nodejs_update(self, query: ParsedQuery) -> List[str]:
        """Generate raw Node.js UPDATE code"""
        code = []
        code.append("async function updateData(data) {")
        code.append("  try {")
        code.append(f"    const sql = `{query.original_sql}`;")
        code.append("    const result = await pool.query(sql, Array.isArray(data) ? data : Object.values(data));")
        code.append("    return result.rowCount;")
        code.append("  } catch (error) {")
        code.append("    console.error('Database update error:', error);")
        code.append("    throw error;")
        code.append("  }")
        code.append("}")
        
        return code
    
    def _generate_raw_nodejs_delete(self, query: ParsedQuery) -> List[str]:
        """Generate raw Node.js DELETE code"""
        code = []
        code.append("async function deleteData(data) {")
        code.append("  try {")
        code.append(f"    const sql = `{query.original_sql}`;")
        code.append("    const result = await pool.query(sql, Array.isArray(data) ? data : Object.values(data));")
        code.append("    return result.rowCount;")
        code.append("  } catch (error) {")
        code.append("    console.error('Database delete error:', error);")
        code.append("    throw error;")
        code.append("  }")
        code.append("}")
        
        return code
    
    def generate_sequelize(self, query: ParsedQuery) -> str:
        """Generate Sequelize ORM code"""
        code = []
        code.append("const { Sequelize, DataTypes, QueryTypes } = require('sequelize');")
        code.append("")
        code.append("const sequelize = new Sequelize('database', 'username', 'password', {")
        code.append("  host: 'localhost',")
        code.append("  dialect: 'postgres', // 'mysql' | 'mariadb' | 'postgres' | 'mssql'")
        code.append("});")
        code.append("")
        
        if query.tables:
            model_name = self._to_pascal_case(query.tables[0].name)
            table_name = query.tables[0].name
            
            # Generate model definition
            code.append(f"const {model_name} = sequelize.define('{model_name}', {{")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and col.name.lower() != "id" and not col.is_aggregate:
                        code.append(f"  {col.name}: {{")
                        code.append("    type: DataTypes.STRING, // Adjust type as needed")
                        code.append("    allowNull: true")
                        code.append("  },")
            else:
                code.append("  name: {")
                code.append("    type: DataTypes.STRING,")
                code.append("    allowNull: false")
                code.append("  },")
            
            code.append("}, {")
            code.append(f"  tableName: '{table_name}',")
            code.append("  timestamps: false // Set to true if you have createdAt/updatedAt")
            code.append("});")
            code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("// Method 1: Raw Query")
            code.append("async function queryDataRaw() {")
            code.append("  try {")
            code.append(f"    const results = await sequelize.query(`{query.original_sql}`, {{")
            code.append("      type: QueryTypes.SELECT")
            code.append("    });")
            code.append("    return results;")
            code.append("  } catch (error) {")
            code.append("    console.error('Query error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
            code.append("")
            
            if query.tables:
                code.append("// Method 2: Using Model")
                code.append(f"async function query{model_name}Data() {{")
                code.append("  try {")
                code.append(f"    const results = await {model_name}.findAll({{")
                
                # Add WHERE conditions
                if query.where_conditions:
                    code.append("      where: {")
                    for condition in query.where_conditions:
                        if condition.operator == "=":
                            code.append(f"        {condition.column}: 'value', // Replace with actual value")
                        elif condition.operator == "LIKE":
                            code.append(f"        {condition.column}: {{ [Sequelize.Op.like]: '%value%' }},")
                        elif condition.operator == ">":
                            code.append(f"        {condition.column}: {{ [Sequelize.Op.gt]: 'value' }},")
                        elif condition.operator == "<":
                            code.append(f"        {condition.column}: {{ [Sequelize.Op.lt]: 'value' }},")
                    code.append("      },")
                
                # Add ORDER BY
                if query.order_by:
                    order_clauses = []
                    for order in query.order_by:
                        order_clauses.append(f"['{order.column}', '{order.direction}']")
                    code.append(f"      order: [{', '.join(order_clauses)}],")
                
                # Add LIMIT
                if query.limit:
                    code.append(f"      limit: {query.limit},")
                
                code.append("    });")
                code.append("    return results.map(r => r.toJSON());")
                code.append("  } catch (error) {")
                code.append("    console.error('Query error:', error);")
                code.append("    throw error;")
                code.append("  }")
                code.append("}")
        
        elif query.query_type == QueryType.INSERT and query.tables:
            code.append(f"async function create{model_name}(data) {{")
            code.append("  try {")
            code.append(f"    const result = await {model_name}.create(data);")
            code.append("    return result.toJSON();")
            code.append("  } catch (error) {")
            code.append("    console.error('Insert error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.UPDATE and query.tables:
            code.append(f"async function update{model_name}(id, data) {{")
            code.append("  try {")
            code.append(f"    const [affectedRows] = await {model_name}.update(data, {{")
            code.append("      where: { id: id }")
            code.append("    });")
            code.append("    return affectedRows;")
            code.append("  } catch (error) {")
            code.append("    console.error('Update error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.DELETE and query.tables:
            code.append(f"async function delete{model_name}(id) {{")
            code.append("  try {")
            code.append(f"    const affectedRows = await {model_name}.destroy({{")
            code.append("      where: { id: id }")
            code.append("    });")
            code.append("    return affectedRows;")
            code.append("  } catch (error) {")
            code.append("    console.error('Delete error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        code.append("")
        code.append("module.exports = { sequelize, " + (f"{model_name}" if query.tables else "") + " };")
        
        return "\n".join(code)
    
    def generate_typeorm(self, query: ParsedQuery) -> str:
        """Generate TypeORM code"""
        code = []
        code.append("import { Entity, PrimaryGeneratedColumn, Column, Repository, getRepository, createConnection } from 'typeorm';")
        code.append("")
        
        if query.tables:
            entity_name = self._to_pascal_case(query.tables[0].name)
            table_name = query.tables[0].name
            
            # Generate entity class
            code.append("@Entity('" + table_name + "')")
            code.append(f"export class {entity_name} {{")
            code.append("  @PrimaryGeneratedColumn()")
            code.append("  id!: number;")
            code.append("")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and col.name.lower() != "id" and not col.is_aggregate:
                        code.append("  @Column()")
                        code.append(f"  {col.name}!: string; // Adjust type as needed")
                        code.append("")
            else:
                code.append("  @Column()")
                code.append("  name!: string;")
                code.append("")
            
            code.append("}")
            code.append("")
            
            # Generate service class
            code.append(f"export class {entity_name}Service {{")
            code.append(f"  private repository: Repository<{entity_name}>;")
            code.append("")
            code.append("  constructor() {")
            code.append(f"    this.repository = getRepository({entity_name});")
            code.append("  }")
            code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("  // Method 1: Raw Query")
            code.append("  async queryDataRaw(): Promise<any[]> {")
            code.append("    try {")
            code.append(f"      const results = await this.repository.query(`{query.original_sql}`);")
            code.append("      return results;")
            code.append("    } catch (error) {")
            code.append("      console.error('Query error:', error);")
            code.append("      throw error;")
            code.append("    }")
            code.append("  }")
            code.append("")
            
            if query.tables:
                code.append("  // Method 2: Using Query Builder")
                code.append(f"  async query{entity_name}Data(): Promise<{entity_name}[]> {{")
                code.append("    try {")
                code.append(f"      let query = this.repository.createQueryBuilder('{table_name.lower()}');")
                
                # Add WHERE conditions
                if query.where_conditions:
                    for i, condition in enumerate(query.where_conditions):
                        param_name = f"param{i}"
                        code.append(f"      query = query.where('{table_name}.{condition.column} {condition.operator} :{param_name}', {{ {param_name}: 'value' }});")
                
                # Add ORDER BY
                if query.order_by:
                    for order in query.order_by:
                        code.append(f"      query = query.orderBy('{table_name}.{order.column}', '{order.direction}');")
                
                # Add LIMIT
                if query.limit:
                    code.append(f"      query = query.take({query.limit});")
                
                code.append("      return await query.getMany();")
                code.append("    } catch (error) {")
                code.append("      console.error('Query error:', error);")
                code.append("      throw error;")
                code.append("    }")
                code.append("  }")
                code.append("")
                
                code.append("  // Method 3: Using Repository Methods")
                code.append(f"  async findAll(): Promise<{entity_name}[]> {{")
                code.append("    return await this.repository.find({")
                
                if query.where_conditions:
                    code.append("      where: {")
                    for condition in query.where_conditions:
                        code.append(f"        {condition.column}: 'value', // Replace with actual value")
                    code.append("      },")
                
                if query.order_by:
                    code.append("      order: {")
                    for order in query.order_by:
                        code.append(f"        {order.column}: '{order.direction}',")
                    code.append("      },")
                
                if query.limit:
                    code.append(f"      take: {query.limit},")
                
                code.append("    });")
                code.append("  }")
        
        elif query.query_type == QueryType.INSERT and query.tables:
            code.append(f"  async create(data: Partial<{entity_name}>): Promise<{entity_name}> {{")
            code.append("    const entity = this.repository.create(data);")
            code.append("    return await this.repository.save(entity);")
            code.append("  }")
        
        elif query.query_type == QueryType.UPDATE and query.tables:
            code.append(f"  async update(id: number, data: Partial<{entity_name}>): Promise<void> {{")
            code.append("    await this.repository.update(id, data);")
            code.append("  }")
        
        elif query.query_type == QueryType.DELETE and query.tables:
            code.append("  async delete(id: number): Promise<void> {")
            code.append("    await this.repository.delete(id);")
            code.append("  }")
        
        if query.tables:
            code.append("}")
            code.append("")
            code.append("// Connection setup")
            code.append("async function setupConnection() {")
            code.append("  await createConnection({")
            code.append("    type: 'postgres',")
            code.append("    host: 'localhost',")
            code.append("    port: 5432,")
            code.append("    username: 'your_username',")
            code.append("    password: 'your_password',")
            code.append("    database: 'your_database',")
            code.append(f"    entities: [{entity_name}],")
            code.append("    synchronize: true, // Set to false in production")
            code.append("  });")
            code.append("}")
            code.append("")
            code.append("// Usage example")
            code.append("// setupConnection().then(async () => {")
            code.append(f"//   const service = new {entity_name}Service();")
            code.append("//   const results = await service.queryDataRaw();")
            code.append("//   console.log(results);")
            code.append("// });")
        
        return "\n".join(code)
    
    def generate_knex(self, query: ParsedQuery) -> str:
        """Generate Knex.js query builder code"""
        code = []
        code.append("const knex = require('knex')({")
        code.append("  client: 'pg', // 'pg' | 'mysql2' | 'sqlite3' | 'mssql'")
        code.append("  connection: {")
        code.append("    host: 'localhost',")
        code.append("    port: 5432,")
        code.append("    user: 'your_username',")
        code.append("    password: 'your_password',")
        code.append("    database: 'your_database'")
        code.append("  }")
        code.append("});")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("// Method 1: Raw Query")
            code.append("async function queryDataRaw() {")
            code.append("  try {")
            code.append(f"    const results = await knex.raw(`{query.original_sql}`);")
            code.append("    return results.rows; // PostgreSQL")
            code.append("    // return results[0]; // MySQL")
            code.append("  } catch (error) {")
            code.append("    console.error('Query error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
            code.append("")
            
            if query.tables:
                table_name = query.tables[0].name
                code.append("// Method 2: Query Builder")
                code.append("async function queryData() {")
                code.append("  try {")
                code.append(f"    let query = knex('{table_name}');")
                
                # Add SELECT columns
                if query.columns and len(query.columns) > 0 and query.columns[0].name != "*":
                    select_columns = [col.name for col in query.columns if not col.is_aggregate]
                    column_list = ', '.join([f"'{col}'" for col in select_columns])
                    code.append(f"    query = query.select({column_list});")
                else:
                    code.append("    query = query.select('*');")
                
                # Add WHERE conditions
                if query.where_conditions:
                    for condition in query.where_conditions:
                        if condition.operator == "=":
                            code.append(f"    query = query.where('{condition.column}', 'value');")
                        elif condition.operator == "LIKE":
                            code.append(f"    query = query.where('{condition.column}', 'like', '%value%');")
                        elif condition.operator == ">":
                            code.append(f"    query = query.where('{condition.column}', '>', 'value');")
                        elif condition.operator == "<":
                            code.append(f"    query = query.where('{condition.column}', '<', 'value');")
                
                # Add ORDER BY
                if query.order_by:
                    for order in query.order_by:
                        code.append(f"    query = query.orderBy('{order.column}', '{order.direction.lower()}');")
                
                # Add LIMIT
                if query.limit:
                    code.append(f"    query = query.limit({query.limit});")
                
                code.append("    const results = await query;")
                code.append("    return results;")
                code.append("  } catch (error) {")
                code.append("    console.error('Query error:', error);")
                code.append("    throw error;")
                code.append("  }")
                code.append("}")
        
        elif query.query_type == QueryType.INSERT and query.tables:
            table_name = query.tables[0].name
            code.append(f"async function insertData(data) {{")
            code.append("  try {")
            code.append(f"    const result = await knex('{table_name}').insert(data).returning('*');")
            code.append("    return result;")
            code.append("  } catch (error) {")
            code.append("    console.error('Insert error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.UPDATE and query.tables:
            table_name = query.tables[0].name
            code.append("async function updateData(id, data) {")
            code.append("  try {")
            code.append(f"    const result = await knex('{table_name}').where('id', id).update(data);")
            code.append("    return result; // Returns number of affected rows")
            code.append("  } catch (error) {")
            code.append("    console.error('Update error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.DELETE and query.tables:
            table_name = query.tables[0].name
            code.append("async function deleteData(id) {")
            code.append("  try {")
            code.append(f"    const result = await knex('{table_name}').where('id', id).del();")
            code.append("    return result; // Returns number of affected rows")
            code.append("  } catch (error) {")
            code.append("    console.error('Delete error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        code.append("")
        code.append("// Usage:")
        if query.query_type == QueryType.SELECT:
            code.append("// queryData().then(results => {")
            code.append("//   console.log('Found', results.length, 'records');")
            code.append("// }).catch(console.error);")
        
        code.append("")
        code.append("// Don't forget to close the connection when done")
        code.append("// knex.destroy();")
        
        return "\n".join(code)
    
    def generate_prisma(self, query: ParsedQuery) -> str:
        """Generate Prisma code"""
        schema = self._generate_prisma_schema(query)
        client_code = self._generate_prisma_client(query)
        
        return f"// schema.prisma\n{schema}\n\n// Client code\n{client_code}"
    
    def _generate_prisma_schema(self, query: ParsedQuery) -> str:
        """Generate Prisma schema"""
        code = []
        code.append("generator client {")
        code.append("  provider = \"prisma-client-js\"")
        code.append("}")
        code.append("")
        code.append("datasource db {")
        code.append("  provider = \"postgresql\" // \"mysql\" | \"sqlite\" | \"sqlserver\"")
        code.append("  url      = env(\"DATABASE_URL\")")
        code.append("}")
        code.append("")
        
        if query.tables:
            model_name = self._to_pascal_case(query.tables[0].name)
            table_name = query.tables[0].name
            
            code.append(f"model {model_name} {{")
            code.append("  id Int @id @default(autoincrement())")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and col.name.lower() != "id" and not col.is_aggregate:
                        code.append(f"  {col.name} String? // Adjust type as needed")
            else:
                code.append("  name String?")
            
            code.append("")
            code.append(f"  @@map(\"{table_name}\")")
            code.append("}")
        
        return "\n".join(code)
    
    def _generate_prisma_client(self, query: ParsedQuery) -> str:
        """Generate Prisma client code"""
        code = []
        code.append("import { PrismaClient } from '@prisma/client';")
        code.append("")
        code.append("const prisma = new PrismaClient();")
        code.append("")
        
        if query.tables:
            model_name = self._to_pascal_case(query.tables[0].name)
            model_name_lower = model_name.lower()
        
        if query.query_type == QueryType.SELECT:
            code.append("// Method 1: Raw Query")
            code.append("async function queryDataRaw() {")
            code.append("  try {")
            code.append(f"    const results = await prisma.$queryRaw`{query.original_sql}`;")
            code.append("    return results;")
            code.append("  } catch (error) {")
            code.append("    console.error('Query error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
            code.append("")
            
            if query.tables:
                code.append("// Method 2: Using Prisma Client")
                code.append(f"async function query{model_name}Data() {{")
                code.append("  try {")
                code.append(f"    const results = await prisma.{model_name_lower}.findMany({{")
                
                # Add WHERE conditions
                if query.where_conditions:
                    code.append("      where: {")
                    for condition in query.where_conditions:
                        if condition.operator == "=":
                            code.append(f"        {condition.column}: 'value',")
                        elif condition.operator == "LIKE":
                            code.append(f"        {condition.column}: {{ contains: 'value' }},")
                        elif condition.operator == ">":
                            code.append(f"        {condition.column}: {{ gt: 'value' }},")
                        elif condition.operator == "<":
                            code.append(f"        {condition.column}: {{ lt: 'value' }},")
                    code.append("      },")
                
                # Add ORDER BY
                if query.order_by:
                    code.append("      orderBy: {")
                    for order in query.order_by:
                        code.append(f"        {order.column}: '{order.direction.lower()}',")
                    code.append("      },")
                
                # Add LIMIT
                if query.limit:
                    code.append(f"      take: {query.limit},")
                
                code.append("    });")
                code.append("    return results;")
                code.append("  } catch (error) {")
                code.append("    console.error('Query error:', error);")
                code.append("    throw error;")
                code.append("  }")
                code.append("}")
        
        elif query.query_type == QueryType.INSERT and query.tables:
            code.append(f"async function create{model_name}(data) {{")
            code.append("  try {")
            code.append(f"    const result = await prisma.{model_name_lower}.create({{")
            code.append("      data: data")
            code.append("    });")
            code.append("    return result;")
            code.append("  } catch (error) {")
            code.append("    console.error('Insert error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.UPDATE and query.tables:
            code.append(f"async function update{model_name}(id, data) {{")
            code.append("  try {")
            code.append(f"    const result = await prisma.{model_name_lower}.update({{")
            code.append("      where: { id: id },")
            code.append("      data: data")
            code.append("    });")
            code.append("    return result;")
            code.append("  } catch (error) {")
            code.append("    console.error('Update error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.DELETE and query.tables:
            code.append(f"async function delete{model_name}(id) {{")
            code.append("  try {")
            code.append(f"    const result = await prisma.{model_name_lower}.delete({{")
            code.append("      where: { id: id }")
            code.append("    });")
            code.append("    return result;")
            code.append("  } catch (error) {")
            code.append("    console.error('Delete error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        code.append("")
        code.append("// Don't forget to disconnect")
        code.append("// await prisma.$disconnect();")
        
        return "\n".join(code)
    
    def generate_mongoose(self, query: ParsedQuery) -> str:
        """Generate Mongoose (MongoDB) code"""
        code = []
        code.append("const mongoose = require('mongoose');")
        code.append("")
        code.append("// MongoDB connection")
        code.append("mongoose.connect('mongodb://localhost:27017/your_database');")
        code.append("")
        
        if query.tables:
            model_name = self._to_pascal_case(query.tables[0].name)
            collection_name = query.tables[0].name
            
            # Generate schema
            code.append(f"const {model_name.lower()}Schema = new mongoose.Schema({{")
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and col.name.lower() != "id" and not col.is_aggregate:
                        code.append(f"  {col.name}: String, // Adjust type as needed")
            else:
                code.append("  name: String,")
            
            code.append("}, {")
            code.append(f"  collection: '{collection_name}'")
            code.append("});")
            code.append("")
            code.append(f"const {model_name} = mongoose.model('{model_name}', {model_name.lower()}Schema);")
            code.append("")
        
        code.append("// Note: SQL queries are not directly applicable to MongoDB")
        code.append("// Here's how you might achieve similar functionality:")
        code.append("")
        
        if query.query_type == QueryType.SELECT and query.tables:
            code.append(f"async function query{model_name}Data() {{")
            code.append("  try {")
            code.append(f"    let query = {model_name}.find();")
            
            # Convert SQL WHERE to MongoDB filter
            if query.where_conditions:
                code.append("    // WHERE equivalent")
                filter_obj = "{"
                for condition in query.where_conditions:
                    if condition.operator == "=":
                        filter_obj += f" {condition.column}: 'value',"
                    elif condition.operator == "LIKE":
                        filter_obj += f" {condition.column}: {{ $regex: 'value', $options: 'i' }},"
                    elif condition.operator == ">":
                        filter_obj += f" {condition.column}: {{ $gt: 'value' }},"
                    elif condition.operator == "<":
                        filter_obj += f" {condition.column}: {{ $lt: 'value' }},"
                filter_obj += " }"
                code.append(f"    query = {model_name}.find({filter_obj});")
            
            # Add ORDER BY equivalent
            if query.order_by:
                sort_obj = "{"
                for order in query.order_by:
                    sort_direction = "1" if order.direction.upper() == "ASC" else "-1"
                    sort_obj += f" {order.column}: {sort_direction},"
                sort_obj += " }"
                code.append(f"    query = query.sort({sort_obj});")
            
            # Add LIMIT
            if query.limit:
                code.append(f"    query = query.limit({query.limit});")
            
            code.append("    const results = await query.exec();")
            code.append("    return results;")
            code.append("  } catch (error) {")
            code.append("    console.error('Query error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.INSERT and query.tables:
            code.append(f"async function create{model_name}(data) {{")
            code.append("  try {")
            code.append(f"    const document = new {model_name}(data);")
            code.append("    const result = await document.save();")
            code.append("    return result;")
            code.append("  } catch (error) {")
            code.append("    console.error('Insert error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.UPDATE and query.tables:
            code.append(f"async function update{model_name}(id, data) {{")
            code.append("  try {")
            code.append(f"    const result = await {model_name}.findByIdAndUpdate(id, data, {{ new: true }});")
            code.append("    return result;")
            code.append("  } catch (error) {")
            code.append("    console.error('Update error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
        elif query.query_type == QueryType.DELETE and query.tables:
            code.append(f"async function delete{model_name}(id) {{")
            code.append("  try {")
            code.append(f"    const result = await {model_name}.findByIdAndDelete(id);")
            code.append("    return result;")
            code.append("  } catch (error) {")
            code.append("    console.error('Delete error:', error);")
            code.append("    throw error;")
            code.append("  }")
            code.append("}")
        
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
nodejs_generator = NodeJSCodeGenerator()