"""
Java Code Generator
Generates Java code from SQL queries using various frameworks
"""

from typing import Dict, List, Any, Optional
from app.services.sql_parser import ParsedQuery, QueryType, ColumnReference


class JavaCodeGenerator:
    """Generates Java code from parsed SQL queries"""
    
    def generate_all_frameworks(self, query: ParsedQuery) -> Dict[str, str]:
        """Generate code for all Java frameworks"""
        return {
            "jdbc": self.generate_jdbc(query),
            "mybatis": self.generate_mybatis(query),
            "jpa_hibernate": self.generate_jpa_hibernate(query),
            "spring_data_jpa": self.generate_spring_data_jpa(query),
            "jooq": self.generate_jooq(query)
        }
    
    def generate_jdbc(self, query: ParsedQuery) -> str:
        """Generate plain JDBC code"""
        code = []
        code.append("import java.sql.*;")
        code.append("import java.util.*;")
        code.append("")
        code.append("public class DatabaseService {")
        code.append("    private static final String URL = \"jdbc:postgresql://localhost:5432/your_database\";")
        code.append("    private static final String USERNAME = \"your_username\";")
        code.append("    private static final String PASSWORD = \"your_password\";")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.extend(self._generate_jdbc_select(query))
        elif query.query_type == QueryType.INSERT:
            code.extend(self._generate_jdbc_insert(query))
        elif query.query_type == QueryType.UPDATE:
            code.extend(self._generate_jdbc_update(query))
        elif query.query_type == QueryType.DELETE:
            code.extend(self._generate_jdbc_delete(query))
        
        code.append("}")
        
        return "\n".join(code)
    
    def _generate_jdbc_select(self, query: ParsedQuery) -> List[str]:
        """Generate JDBC SELECT code"""
        code = []
        code.append("    public List<Map<String, Object>> executeQuery() {")
        code.append("        List<Map<String, Object>> results = new ArrayList<>();")
        code.append("        ")
        code.append(f'        String sql = "{query.original_sql}";')
        code.append("        ")
        code.append("        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);")
        code.append("             PreparedStatement stmt = conn.prepareStatement(sql);")
        code.append("             ResultSet rs = stmt.executeQuery()) {")
        code.append("            ")
        code.append("            ResultSetMetaData metaData = rs.getMetaData();")
        code.append("            int columnCount = metaData.getColumnCount();")
        code.append("            ")
        code.append("            while (rs.next()) {")
        code.append("                Map<String, Object> row = new HashMap<>();")
        code.append("                for (int i = 1; i <= columnCount; i++) {")
        code.append("                    String columnName = metaData.getColumnName(i);")
        code.append("                    Object value = rs.getObject(i);")
        code.append("                    row.put(columnName, value);")
        code.append("                }")
        code.append("                results.add(row);")
        code.append("            }")
        code.append("        } catch (SQLException e) {")
        code.append("            e.printStackTrace();")
        code.append("            throw new RuntimeException(\"Database query failed\", e);")
        code.append("        }")
        code.append("        ")
        code.append("        return results;")
        code.append("    }")
        
        return code
    
    def _generate_jdbc_insert(self, query: ParsedQuery) -> List[str]:
        """Generate JDBC INSERT code"""
        code = []
        code.append("    public int insertData(Object... params) {")
        code.append(f'        String sql = "{query.original_sql}";')
        code.append("        ")
        code.append("        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);")
        code.append("             PreparedStatement stmt = conn.prepareStatement(sql)) {")
        code.append("            ")
        code.append("            for (int i = 0; i < params.length; i++) {")
        code.append("                stmt.setObject(i + 1, params[i]);")
        code.append("            }")
        code.append("            ")
        code.append("            return stmt.executeUpdate();")
        code.append("        } catch (SQLException e) {")
        code.append("            e.printStackTrace();")
        code.append("            throw new RuntimeException(\"Database insert failed\", e);")
        code.append("        }")
        code.append("    }")
        
        return code
    
    def _generate_jdbc_update(self, query: ParsedQuery) -> List[str]:
        """Generate JDBC UPDATE code"""
        code = []
        code.append("    public int updateData(Object... params) {")
        code.append(f'        String sql = "{query.original_sql}";')
        code.append("        ")
        code.append("        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);")
        code.append("             PreparedStatement stmt = conn.prepareStatement(sql)) {")
        code.append("            ")
        code.append("            for (int i = 0; i < params.length; i++) {")
        code.append("                stmt.setObject(i + 1, params[i]);")
        code.append("            }")
        code.append("            ")
        code.append("            return stmt.executeUpdate();")
        code.append("        } catch (SQLException e) {")
        code.append("            e.printStackTrace();")
        code.append("            throw new RuntimeException(\"Database update failed\", e);")
        code.append("        }")
        code.append("    }")
        
        return code
    
    def _generate_jdbc_delete(self, query: ParsedQuery) -> List[str]:
        """Generate JDBC DELETE code"""
        code = []
        code.append("    public int deleteData(Object... params) {")
        code.append(f'        String sql = "{query.original_sql}";')
        code.append("        ")
        code.append("        try (Connection conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);")
        code.append("             PreparedStatement stmt = conn.prepareStatement(sql)) {")
        code.append("            ")
        code.append("            for (int i = 0; i < params.length; i++) {")
        code.append("                stmt.setObject(i + 1, params[i]);")
        code.append("            }")
        code.append("            ")
        code.append("            return stmt.executeUpdate();")
        code.append("        } catch (SQLException e) {")
        code.append("            e.printStackTrace();")
        code.append("            throw new RuntimeException(\"Database delete failed\", e);")
        code.append("        }")
        code.append("    }")
        
        return code
    
    def generate_mybatis(self, query: ParsedQuery) -> str:
        """Generate MyBatis code"""
        mapper_xml = self._generate_mybatis_mapper(query)
        java_interface = self._generate_mybatis_interface(query)
        
        return f"// Mapper Interface\n{java_interface}\n\n// Mapper XML\n{mapper_xml}"
    
    def _generate_mybatis_mapper(self, query: ParsedQuery) -> str:
        """Generate MyBatis mapper XML"""
        if not query.tables:
            return "<!-- Cannot generate mapper without table information -->"
        
        primary_table = query.tables[0]
        
        code = []
        code.append('<?xml version="1.0" encoding="UTF-8" ?>')
        code.append('<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">')
        code.append(f'<mapper namespace="com.example.mapper.{self._to_pascal_case(primary_table.name)}Mapper">')
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("    <!-- Result Map -->")
            code.append(f'    <resultMap id="{primary_table.name}ResultMap" type="com.example.model.{self._to_pascal_case(primary_table.name)}">')
            
            if query.columns:
                for col in query.columns:
                    if col.name != "*" and not col.is_aggregate:
                        code.append(f'        <result property="{self._to_camel_case(col.name)}" column="{col.name}"/>')
            
            code.append("    </resultMap>")
            code.append("")
            code.append(f'    <select id="selectData" resultMap="{primary_table.name}ResultMap">')
            code.append(f"        {query.original_sql}")
            code.append("    </select>")
        
        elif query.query_type == QueryType.INSERT:
            code.append(f'    <insert id="insertData" parameterType="com.example.model.{self._to_pascal_case(primary_table.name)}">')
            code.append(f"        {query.original_sql}")
            code.append("    </insert>")
        
        elif query.query_type == QueryType.UPDATE:
            code.append(f'    <update id="updateData" parameterType="com.example.model.{self._to_pascal_case(primary_table.name)}">')
            code.append(f"        {query.original_sql}")
            code.append("    </update>")
        
        elif query.query_type == QueryType.DELETE:
            code.append('    <delete id="deleteData" parameterType="java.lang.Integer">')
            code.append(f"        {query.original_sql}")
            code.append("    </delete>")
        
        code.append("</mapper>")
        
        return "\n".join(code)
    
    def _generate_mybatis_interface(self, query: ParsedQuery) -> str:
        """Generate MyBatis mapper interface"""
        if not query.tables:
            return "// Cannot generate interface without table information"
        
        primary_table = query.tables[0]
        class_name = self._to_pascal_case(primary_table.name)
        
        code = []
        code.append(f"package com.example.mapper;")
        code.append("")
        code.append("import java.util.List;")
        code.append("import org.apache.ibatis.annotations.Mapper;")
        code.append(f"import com.example.model.{class_name};")
        code.append("")
        code.append("@Mapper")
        code.append(f"public interface {class_name}Mapper {{")
        
        if query.query_type == QueryType.SELECT:
            code.append(f"    List<{class_name}> selectData();")
        elif query.query_type == QueryType.INSERT:
            code.append(f"    int insertData({class_name} entity);")
        elif query.query_type == QueryType.UPDATE:
            code.append(f"    int updateData({class_name} entity);")
        elif query.query_type == QueryType.DELETE:
            code.append("    int deleteData(Integer id);")
        
        code.append("}")
        
        return "\n".join(code)
    
    def generate_jpa_hibernate(self, query: ParsedQuery) -> str:
        """Generate JPA/Hibernate code"""
        if not query.tables:
            return "// Cannot generate JPA code without table information"
        
        primary_table = query.tables[0]
        class_name = self._to_pascal_case(primary_table.name)
        
        entity_code = self._generate_jpa_entity(query)
        repository_code = self._generate_jpa_repository(query)
        service_code = self._generate_jpa_service(query)
        
        return f"// Entity Class\n{entity_code}\n\n// Repository Interface\n{repository_code}\n\n// Service Class\n{service_code}"
    
    def _generate_jpa_entity(self, query: ParsedQuery) -> str:
        """Generate JPA entity class"""
        primary_table = query.tables[0]
        class_name = self._to_pascal_case(primary_table.name)
        
        code = []
        code.append("import javax.persistence.*;")
        code.append("")
        code.append("@Entity")
        code.append(f"@Table(name = \"{primary_table.name}\")")
        code.append(f"public class {class_name} {{")
        code.append("    @Id")
        code.append("    @GeneratedValue(strategy = GenerationType.IDENTITY)")
        code.append("    private Long id;")
        code.append("")
        
        # Add fields based on columns
        if query.columns:
            for col in query.columns:
                if col.name != "*" and col.name != "id" and not col.is_aggregate:
                    field_name = self._to_camel_case(col.name)
                    code.append(f"    @Column(name = \"{col.name}\")")
                    code.append(f"    private String {field_name};")
                    code.append("")
        
        # Add getters and setters
        code.append("    // Getters and Setters")
        code.append("    public Long getId() { return id; }")
        code.append("    public void setId(Long id) { this.id = id; }")
        
        if query.columns:
            for col in query.columns:
                if col.name != "*" and col.name != "id" and not col.is_aggregate:
                    field_name = self._to_camel_case(col.name)
                    field_name_pascal = self._to_pascal_case(col.name)
                    code.append(f"    public String get{field_name_pascal}() {{ return {field_name}; }}")
                    code.append(f"    public void set{field_name_pascal}(String {field_name}) {{ this.{field_name} = {field_name}; }}")
        
        code.append("}")
        
        return "\n".join(code)
    
    def _generate_jpa_repository(self, query: ParsedQuery) -> str:
        """Generate JPA repository interface"""
        primary_table = query.tables[0]
        class_name = self._to_pascal_case(primary_table.name)
        
        code = []
        code.append("import org.springframework.data.jpa.repository.JpaRepository;")
        code.append("import org.springframework.data.jpa.repository.Query;")
        code.append("import java.util.List;")
        code.append("")
        code.append(f"public interface {class_name}Repository extends JpaRepository<{class_name}, Long> {{")
        
        if query.query_type == QueryType.SELECT:
            code.append(f"    @Query(\"{query.original_sql}\")")
            code.append(f"    List<{class_name}> findCustomData();")
            
            # Generate method names based on WHERE conditions
            if query.where_conditions:
                for condition in query.where_conditions:
                    method_name = f"findBy{self._to_pascal_case(condition.column)}"
                    code.append(f"    List<{class_name}> {method_name}(String {condition.column});")
        
        code.append("}")
        
        return "\n".join(code)
    
    def _generate_jpa_service(self, query: ParsedQuery) -> str:
        """Generate JPA service class"""
        primary_table = query.tables[0]
        class_name = self._to_pascal_case(primary_table.name)
        
        code = []
        code.append("import org.springframework.beans.factory.annotation.Autowired;")
        code.append("import org.springframework.stereotype.Service;")
        code.append("import java.util.List;")
        code.append("")
        code.append("@Service")
        code.append(f"public class {class_name}Service {{")
        code.append("")
        code.append("    @Autowired")
        code.append(f"    private {class_name}Repository repository;")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append(f"    public List<{class_name}> getData() {{")
            code.append("        return repository.findCustomData();")
            code.append("    }")
        
        code.append("}")
        
        return "\n".join(code)
    
    def generate_spring_data_jpa(self, query: ParsedQuery) -> str:
        """Generate Spring Data JPA code"""
        return self.generate_jpa_hibernate(query)  # Same as JPA/Hibernate for now
    
    def generate_jooq(self, query: ParsedQuery) -> str:
        """Generate jOOQ code"""
        if not query.tables:
            return "// Cannot generate jOOQ code without table information"
        
        primary_table = query.tables[0]
        
        code = []
        code.append("import static org.jooq.impl.DSL.*;")
        code.append("import org.jooq.*;")
        code.append("import org.jooq.impl.DSL;")
        code.append("")
        code.append("public class DatabaseService {")
        code.append("    private DSLContext create;")
        code.append("")
        code.append("    public DatabaseService(DSLContext create) {")
        code.append("        this.create = create;")
        code.append("    }")
        code.append("")
        
        if query.query_type == QueryType.SELECT:
            code.append("    public Result<Record> selectData() {")
            code.append(f"        return create.resultQuery(\"{query.original_sql}\").fetch();")
            code.append("    }")
            code.append("")
            code.append("    // Type-safe alternative")
            code.append("    public Result<Record> selectDataTypeSafe() {")
            table_name_upper = primary_table.name.upper()
            code.append(f"        return create.select()")
            code.append(f"                    .from(table(\"{primary_table.name}\"))")
            
            if query.where_conditions:
                for condition in query.where_conditions:
                    code.append(f"                    .where(field(\"{condition.column}\").eq(val(\"value\")))")
            
            code.append("                    .fetch();")
            code.append("    }")
        
        code.append("}")
        
        return "\n".join(code)
    
    def _to_pascal_case(self, snake_str: str) -> str:
        """Convert snake_case to PascalCase"""
        components = snake_str.split('_')
        return ''.join(word.capitalize() for word in components)
    
    def _to_camel_case(self, snake_str: str) -> str:
        """Convert snake_case to camelCase"""
        components = snake_str.split('_')
        return components[0] + ''.join(word.capitalize() for word in components[1:])


# Global instance
java_generator = JavaCodeGenerator()