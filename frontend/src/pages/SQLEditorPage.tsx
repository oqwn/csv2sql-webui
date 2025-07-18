import React, { useState } from 'react';
import {
  Box,
  Paper,
  Button,
  TextField,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Alert,
  AlertTitle,
  CircularProgress,
  Tabs,
  Tab,
  Grid,
  IconButton,
  MenuItem,
  Chip,
  Divider,
  FormControlLabel,
  Switch,
  FormControl,
  InputLabel,
  Select,
  Checkbox,
} from '@mui/material';
import {
  PlayArrow as PlayArrowIcon,
  TableChart as TableChartIcon,
  Add as AddIcon,
  FileDownload as FileDownloadIcon,
  ContentCopy as ContentCopyIcon,
  Clear as ClearIcon,
  CloudUpload as CloudUploadIcon,
} from '@mui/icons-material';
import { sqlAPI, exportAPI, importAPI } from '../services/api';
import { SQLEditor } from '../components/sql/SQLEditor';
import { generateBulkInsertSQL } from '../utils/dataGenerator';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`sql-tabpanel-${index}`}
      aria-labelledby={`sql-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ pt: 2 }}>{children}</Box>}
    </div>
  );
}

// Data types organized by category
const DATA_TYPES = {
  'Numeric': [
    { value: 'INTEGER', label: 'INTEGER', description: 'Signed 4-byte integer' },
    { value: 'BIGINT', label: 'BIGINT', description: 'Signed 8-byte integer' },
    { value: 'SMALLINT', label: 'SMALLINT', description: 'Signed 2-byte integer' },
    { value: 'SERIAL', label: 'SERIAL', description: 'Auto-incrementing integer' },
    { value: 'BIGSERIAL', label: 'BIGSERIAL', description: 'Auto-incrementing big integer' },
    { value: 'DECIMAL(10,2)', label: 'DECIMAL(10,2)', description: 'Fixed precision decimal' },
    { value: 'NUMERIC(10,2)', label: 'NUMERIC(10,2)', description: 'Variable precision decimal' },
    { value: 'REAL', label: 'REAL', description: 'Single precision float' },
    { value: 'DOUBLE PRECISION', label: 'DOUBLE PRECISION', description: 'Double precision float' },
  ],
  'Character': [
    { value: 'VARCHAR(255)', label: 'VARCHAR(255)', description: 'Variable length string' },
    { value: 'VARCHAR(100)', label: 'VARCHAR(100)', description: 'Variable length string (100)' },
    { value: 'VARCHAR(50)', label: 'VARCHAR(50)', description: 'Variable length string (50)' },
    { value: 'CHAR(10)', label: 'CHAR(10)', description: 'Fixed length string' },
    { value: 'TEXT', label: 'TEXT', description: 'Variable unlimited length string' },
  ],
  'Date/Time': [
    { value: 'DATE', label: 'DATE', description: 'Calendar date (year, month, day)' },
    { value: 'TIME', label: 'TIME', description: 'Time of day' },
    { value: 'TIMESTAMP', label: 'TIMESTAMP', description: 'Date and time' },
    { value: 'TIMESTAMPTZ', label: 'TIMESTAMPTZ', description: 'Date and time with timezone' },
    { value: 'INTERVAL', label: 'INTERVAL', description: 'Time span' },
  ],
  'Boolean': [
    { value: 'BOOLEAN', label: 'BOOLEAN', description: 'True/false value' },
  ],
  'Binary': [
    { value: 'BYTEA', label: 'BYTEA', description: 'Binary data' },
  ],
  'JSON': [
    { value: 'JSON', label: 'JSON', description: 'JSON data' },
    { value: 'JSONB', label: 'JSONB', description: 'Binary JSON data' },
  ],
  'UUID': [
    { value: 'UUID', label: 'UUID', description: 'Universally unique identifier' },
  ],
  'Arrays': [
    { value: 'INTEGER[]', label: 'INTEGER[]', description: 'Array of integers' },
    { value: 'TEXT[]', label: 'TEXT[]', description: 'Array of text' },
  ],
};

// Constraint options
const CONSTRAINT_OPTIONS = [
  { value: '', label: 'None', description: 'No constraint' },
  { value: 'NOT NULL', label: 'NOT NULL', description: 'Value cannot be null' },
  { value: 'UNIQUE', label: 'UNIQUE', description: 'Value must be unique' },
  { value: 'PRIMARY KEY', label: 'PRIMARY KEY', description: 'Primary key constraint' },
  { value: 'NOT NULL UNIQUE', label: 'NOT NULL UNIQUE', description: 'Not null and unique' },
  { value: 'DEFAULT NULL', label: 'DEFAULT NULL', description: 'Default value is null' },
  { value: 'DEFAULT CURRENT_TIMESTAMP', label: 'DEFAULT CURRENT_TIMESTAMP', description: 'Default to current timestamp' },
  { value: 'DEFAULT 0', label: 'DEFAULT 0', description: 'Default value is 0' },
  { value: 'CHECK (length(value) > 0)', label: 'CHECK (length > 0)', description: 'Check constraint for length' },
];

// Table templates
const TABLE_TEMPLATES = {
  'users': [
    { name: 'username', type: 'VARCHAR(50)', constraints: 'NOT NULL UNIQUE', customType: false, customConstraints: false },
    { name: 'email', type: 'VARCHAR(255)', constraints: 'NOT NULL UNIQUE', customType: false, customConstraints: false },
    { name: 'password_hash', type: 'VARCHAR(255)', constraints: 'NOT NULL', customType: false, customConstraints: false },
    { name: 'created_at', type: 'TIMESTAMP', constraints: 'DEFAULT CURRENT_TIMESTAMP', customType: false, customConstraints: false },
    { name: 'updated_at', type: 'TIMESTAMP', constraints: 'DEFAULT CURRENT_TIMESTAMP', customType: false, customConstraints: false },
  ],
  'products': [
    { name: 'name', type: 'VARCHAR(255)', constraints: 'NOT NULL', customType: false, customConstraints: false },
    { name: 'description', type: 'TEXT', constraints: '', customType: false, customConstraints: false },
    { name: 'price', type: 'DECIMAL(10,2)', constraints: 'NOT NULL', customType: false, customConstraints: false },
    { name: 'stock_quantity', type: 'INTEGER', constraints: 'DEFAULT 0', customType: false, customConstraints: false },
    { name: 'category', type: 'VARCHAR(100)', constraints: '', customType: false, customConstraints: false },
    { name: 'created_at', type: 'TIMESTAMP', constraints: 'DEFAULT CURRENT_TIMESTAMP', customType: false, customConstraints: false },
  ],
  'orders': [
    { name: 'user_id', type: 'INTEGER', constraints: 'NOT NULL', customType: false, customConstraints: false },
    { name: 'status', type: 'VARCHAR(50)', constraints: 'DEFAULT \'pending\'', customType: false, customConstraints: false },
    { name: 'total_amount', type: 'DECIMAL(10,2)', constraints: 'NOT NULL', customType: false, customConstraints: false },
    { name: 'order_date', type: 'TIMESTAMP', constraints: 'DEFAULT CURRENT_TIMESTAMP', customType: false, customConstraints: false },
    { name: 'shipping_address', type: 'TEXT', constraints: '', customType: false, customConstraints: false },
  ],
};

const SQLEditorPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState(0);
  const [query, setQuery] = useState('');
  const [createTableName, setCreateTableName] = useState('');
  const [includeIdColumn, setIncludeIdColumn] = useState(true);
  const [idColumnType, setIdColumnType] = useState('SERIAL');
  const [idColumnName, setIdColumnName] = useState('id');
  const [columns, setColumns] = useState<Array<{name: string; type: string; constraints: string; customType: boolean; customConstraints: boolean}>>([{name: '', type: 'VARCHAR(255)', constraints: '', customType: false, customConstraints: false}]);
  const [insertTableName, setInsertTableName] = useState('');
  const [insertColumns, setInsertColumns] = useState<Array<{column: string; value: string; useDropdown: boolean}>>([{column: '', value: '', useDropdown: true}]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState<any>(null);
  const [tables, setTables] = useState<string[]>([]);
  const [tableColumns, setTableColumns] = useState<Array<{name: string; type: string; nullable: boolean; default: string | null}>>([]);
  const [showBulkGenerate, setShowBulkGenerate] = useState(false);
  const [bulkRowCount, setBulkRowCount] = useState<100 | 1000 | 10000>(100);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [dragActive, setDragActive] = useState(false);
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [importTableName, setImportTableName] = useState('');

  const executeQuery = async (sqlQuery?: string) => {
    const queryToExecute = sqlQuery || query;
    if (!queryToExecute.trim()) {
      setError('Please enter a SQL query');
      return;
    }

    setLoading(true);
    setError('');
    setResult(null);
    
    try {
      const response = await sqlAPI.executeQuery(queryToExecute);
      // Store the query in the result for success message detection
      setResult({...response.data, executedQuery: queryToExecute});
      
      // Refresh tables list if CREATE TABLE was executed
      if (queryToExecute.trim().toUpperCase().startsWith('CREATE TABLE')) {
        await fetchTables();
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Query execution failed');
    } finally {
      setLoading(false);
    }
  };

  const fetchTables = async () => {
    try {
      const response = await sqlAPI.getTables();
      setTables(response.data.tables || []);
    } catch (err) {
      console.error('Failed to fetch tables:', err);
    }
  };

  const fetchTableColumns = async (tableName: string) => {
    try {
      const response = await sqlAPI.getTableColumns(tableName);
      setTableColumns(response.data.columns || []);
    } catch (err) {
      console.error('Failed to fetch table columns:', err);
      setTableColumns([]);
    }
  };

  React.useEffect(() => {
    fetchTables();
  }, []);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
    setError('');
  };

  const generateCreateTableSQL = () => {
    if (!createTableName.trim()) {
      return '';
    }

    const validColumns = columns.filter(col => col.name.trim());
    if (!includeIdColumn && validColumns.length === 0) {
      return '';
    }

    // List of PostgreSQL reserved keywords
    const reservedKeywords = ['user', 'order', 'group', 'table', 'column', 'select', 'from', 'where', 'join', 'union', 'insert', 'update', 'delete', 'create', 'alter', 'drop', 'grant', 'revoke', 'index', 'view', 'trigger', 'function', 'procedure', 'database', 'schema', 'role', 'password', 'authorization'];
    
    // Quote identifier if it's a reserved keyword
    const quoteIdentifier = (name: string) => {
      if (reservedKeywords.includes(name.toLowerCase())) {
        return `"${name}"`;
      }
      return name;
    };

    const allColumnDefs = [];
    
    // Add ID column if enabled
    if (includeIdColumn) {
      let idDef = `${quoteIdentifier(idColumnName)} ${idColumnType}`;
      
      // Add PRIMARY KEY constraint for ID column
      if (idColumnType === 'SERIAL' || idColumnType === 'BIGSERIAL') {
        idDef += ' PRIMARY KEY';
      } else if (idColumnType === 'UUID') {
        idDef += ' DEFAULT gen_random_uuid() PRIMARY KEY';
      } else {
        idDef += ' PRIMARY KEY';
      }
      
      allColumnDefs.push(idDef);
    }
    
    // Add user-defined columns
    const userColumnDefs = validColumns.map(col => 
      `${quoteIdentifier(col.name)} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
    );
    
    allColumnDefs.push(...userColumnDefs);
    
    if (allColumnDefs.length === 0) {
      return '';
    }

    return `CREATE TABLE ${quoteIdentifier(createTableName)} (\n  ${allColumnDefs.join(',\n  ')}\n);`;
  };

  const generateInsertSQL = () => {
    if (!insertTableName.trim()) {
      return '';
    }

    // If bulk generation is enabled, use the generator
    if (showBulkGenerate && tableColumns.length > 0) {
      // Filter columns to only include selected ones
      const columnsToGenerate = tableColumns.filter(col => selectedColumns.includes(col.name));
      if (columnsToGenerate.length === 0) {
        return '-- No columns selected';
      }
      return generateBulkInsertSQL(insertTableName, columnsToGenerate, bulkRowCount, true);
    }

    const validColumns = insertColumns.filter(col => col.column.trim() && col.value.trim());
    if (validColumns.length === 0) {
      return '';
    }

    // List of PostgreSQL reserved keywords
    const reservedKeywords = ['user', 'order', 'group', 'table', 'column', 'select', 'from', 'where', 'join', 'union', 'insert', 'update', 'delete', 'create', 'alter', 'drop', 'grant', 'revoke', 'index', 'view', 'trigger', 'function', 'procedure', 'database', 'schema', 'role', 'password', 'authorization'];
    
    // Quote identifier if it's a reserved keyword
    const quoteIdentifier = (name: string) => {
      if (reservedKeywords.includes(name.toLowerCase())) {
        return `"${name}"`;
      }
      return name;
    };

    const columnNames = validColumns.map(col => quoteIdentifier(col.column)).join(', ');
    const values = validColumns.map(col => {
      // Handle DEFAULT keyword for auto-increment columns
      if (col.value.toUpperCase() === 'DEFAULT') {
        return 'DEFAULT';
      }
      // Handle NULL values
      if (col.value.toUpperCase() === 'NULL') {
        return 'NULL';
      }
      // Add quotes for string values
      if (isNaN(Number(col.value))) {
        return `'${col.value.replace(/'/g, "''")}'`;
      }
      return col.value;
    }).join(', ');

    return `INSERT INTO ${quoteIdentifier(insertTableName)} (${columnNames})\nVALUES (${values});`;
  };

  const handleCreateTable = () => {
    if (!createTableName.trim()) {
      setError('Please enter a table name');
      return;
    }

    const validColumns = columns.filter(col => col.name.trim());
    if (!includeIdColumn && validColumns.length === 0) {
      setError('Please add at least one column or enable ID column');
      return;
    }

    if (includeIdColumn && !idColumnName.trim()) {
      setError('Please enter an ID column name');
      return;
    }

    const sql = generateCreateTableSQL();
    if (sql) {
      executeQuery(sql);
    }
  };

  const handleInsertData = () => {
    if (!insertTableName.trim()) {
      setError('Please select a table');
      return;
    }

    // For bulk generation, validate selected columns
    if (showBulkGenerate && tableColumns.length > 0) {
      if (selectedColumns.length === 0) {
        setError('Please select at least one column to generate data for');
        return;
      }
      
      // Check if all required columns are selected
      const requiredColumns = tableColumns.filter(col => {
        const isAutoGenerated = col.default?.includes('nextval') || col.default?.includes('sequence');
        return !col.nullable && !col.default && !isAutoGenerated;
      });
      
      const missingRequired = requiredColumns.filter(col => !selectedColumns.includes(col.name));
      if (missingRequired.length > 0) {
        setError(`Missing required columns: ${missingRequired.map(c => c.name).join(', ')}`);
        return;
      }
      
      const sql = generateInsertSQL();
      if (sql) {
        executeQuery(sql);
      }
      return;
    }

    const validColumns = insertColumns.filter(col => col.column.trim() && col.value.trim());
    if (validColumns.length === 0) {
      setError('Please add at least one column with value');
      return;
    }

    // Check for required fields (excluding auto-generated fields)
    const requiredColumns = tableColumns.filter(col => {
      const hasDefault = col.default !== null && col.default !== undefined;
      const isAutoGenerated = col.default?.includes('nextval') || col.default?.includes('sequence');
      return !col.nullable && !hasDefault && !isAutoGenerated;
    });
    
    const missingRequired = requiredColumns.filter(
      reqCol => !validColumns.some(vc => vc.column === reqCol.name)
    );
    
    if (missingRequired.length > 0) {
      setError(`Missing required fields: ${missingRequired.map(c => c.name).join(', ')}`);
      return;
    }

    const sql = generateInsertSQL();
    if (sql) {
      executeQuery(sql);
    }
  };

  const addColumn = () => {
    setColumns([...columns, {name: '', type: 'VARCHAR(255)', constraints: '', customType: false, customConstraints: false}]);
  };

  const removeColumn = (index: number) => {
    setColumns(columns.filter((_, i) => i !== index));
  };

  const updateColumn = (index: number, field: 'name' | 'type' | 'constraints' | 'customType' | 'customConstraints', value: string | boolean) => {
    const updated = [...columns];
    if (field === 'customType') {
      updated[index][field] = value as boolean;
      if (value === true) {
        updated[index]['type'] = ''; // Clear type when switching to custom
      } else {
        updated[index]['type'] = 'VARCHAR(255)'; // Set default when switching back
      }
    } else if (field === 'customConstraints') {
      updated[index][field] = value as boolean;
      if (value === true) {
        updated[index]['constraints'] = ''; // Clear constraints when switching to custom
      } else {
        updated[index]['constraints'] = ''; // Set default when switching back
      }
    } else {
      updated[index][field] = value as string;
    }
    setColumns(updated);
  };

  const addInsertColumn = () => {
    setInsertColumns([...insertColumns, {column: '', value: '', useDropdown: true}]);
  };

  const removeInsertColumn = (index: number) => {
    setInsertColumns(insertColumns.filter((_, i) => i !== index));
  };

  const updateInsertColumn = (index: number, field: 'column' | 'value' | 'useDropdown', value: string | boolean) => {
    const updated = [...insertColumns];
    if (field === 'useDropdown') {
      updated[index][field] = value as boolean;
      if (value === false) {
        updated[index]['column'] = ''; // Clear column when switching to text mode
      }
    } else {
      updated[index][field] = value as string;
    }
    setInsertColumns(updated);
  };

  const handleExport = async (format: 'csv' | 'excel') => {
    if (!result || !result.rows || result.rows.length === 0) {
      setError('No data to export');
      return;
    }

    try {
      const response = await exportAPI.exportData({
        data: result.rows,
        columns: result.columns,
        format: format,
        filename: `export_${new Date().getTime()}`
      });
      
      // Create download link
      const blob = new Blob([response.data], {
        type: format === 'csv' ? 'text/csv' : 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `export_${new Date().getTime()}.${format === 'csv' ? 'csv' : 'xlsx'}`;
      link.click();
      window.URL.revokeObjectURL(url);
      
      // Show success message
      setError(''); // Clear any existing errors
      // You could add a success state here if needed
    } catch {
      setError('Export failed');
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  const handleFile = (file: File) => {
    const validTypes = ['text/csv', 'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'];
    if (!validTypes.includes(file.type) && !file.name.endsWith('.csv')) {
      setError('Please upload a CSV or Excel file');
      return;
    }
    setUploadFile(file);
    setError('');
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      handleFile(e.target.files[0]);
    }
  };

  const handleImport = async () => {
    if (!uploadFile) {
      setError('Please select a file to import');
      return;
    }

    if (!importTableName.trim()) {
      setError('Please enter a table name');
      return;
    }

    setLoading(true);
    try {
      const response = await importAPI.uploadCSV(uploadFile, importTableName);
      setResult({
        columns: ['Status'],
        rows: [[response.data.message || 'Import successful']],
        row_count: response.data.rows_imported || 0,
        execution_time: 0,
        executedQuery: `IMPORT ${uploadFile.name} INTO ${importTableName}`
      });
      setUploadFile(null);
      await fetchTables();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Import failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        SQL Editor
      </Typography>
      
      <Paper sx={{ width: '100%' }}>
        <Tabs
          value={activeTab}
          onChange={handleTabChange}
          indicatorColor="primary"
          textColor="primary"
          variant="fullWidth"
        >
          <Tab label="Query Editor" icon={<PlayArrowIcon />} iconPosition="start" />
          <Tab label="Create Table" icon={<TableChartIcon />} iconPosition="start" />
          <Tab label="Insert Data" icon={<AddIcon />} iconPosition="start" />
          <Tab label="Import Data" icon={<CloudUploadIcon />} iconPosition="start" />
        </Tabs>

        <Box sx={{ p: 2 }}>
          <TabPanel value={activeTab} index={0}>
            <SQLEditor
              value={query}
              onChange={setQuery}
              onExecute={() => executeQuery()}
              error={error}
              readOnly={loading}
            />
            <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
              <Button
                variant="contained"
                startIcon={<PlayArrowIcon />}
                onClick={() => executeQuery()}
                disabled={!query.trim() || loading}
              >
                {loading ? 'Executing...' : 'Execute Query'}
              </Button>
              <Button
                variant="outlined"
                startIcon={<ClearIcon />}
                onClick={() => setQuery('')}
                disabled={loading}
              >
                Clear
              </Button>
            </Box>
          </TabPanel>

          <TabPanel value={activeTab} index={1}>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  label="Table Name"
                  value={createTableName}
                  onChange={(e) => setCreateTableName(e.target.value)}
                  placeholder="e.g., users, products"
                  error={['user', 'order', 'group', 'table', 'column', 'select', 'from', 'where'].includes(createTableName.toLowerCase())}
                  helperText={
                    ['user', 'order', 'group', 'table', 'column', 'select', 'from', 'where'].includes(createTableName.toLowerCase())
                      ? `"${createTableName}" is a reserved keyword in PostgreSQL. Consider using "${createTableName}s" or wrap it in quotes.`
                      : ''
                  }
                />
              </Grid>
              
              {/* ID Column Configuration */}
              <Grid item xs={12}>
                <Typography variant="h6" gutterBottom>
                  ID Column Configuration
                </Typography>
                <FormControlLabel
                  control={
                    <Switch
                      checked={includeIdColumn}
                      onChange={(e) => setIncludeIdColumn(e.target.checked)}
                    />
                  }
                  label="Include ID column"
                />
              </Grid>
              
              {includeIdColumn && (
                <>
                  <Grid item xs={4}>
                    <TextField
                      fullWidth
                      label="ID Column Name"
                      value={idColumnName}
                      onChange={(e) => setIdColumnName(e.target.value)}
                      placeholder="e.g., id, user_id"
                    />
                  </Grid>
                  <Grid item xs={8}>
                    <FormControl fullWidth>
                      <InputLabel>ID Column Type</InputLabel>
                      <Select
                        value={idColumnType}
                        label="ID Column Type"
                        onChange={(e) => setIdColumnType(e.target.value)}
                      >
                        <MenuItem value="SERIAL">SERIAL (Auto-increment integer)</MenuItem>
                        <MenuItem value="BIGSERIAL">BIGSERIAL (Auto-increment big integer)</MenuItem>
                        <MenuItem value="UUID">UUID (Universally unique identifier)</MenuItem>
                        <MenuItem value="INTEGER">INTEGER (Manual integer)</MenuItem>
                        <MenuItem value="BIGINT">BIGINT (Manual big integer)</MenuItem>
                        <MenuItem value="VARCHAR(36)">VARCHAR(36) (String ID)</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>
                </>
              )}
              <Grid item xs={12}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                  <Typography variant="h6">
                    {includeIdColumn ? 'Additional Columns' : 'Columns'}
                  </Typography>
                  <FormControl size="small" sx={{ minWidth: 120 }}>
                    <InputLabel>Template</InputLabel>
                    <Select
                      value={''}
                      label="Template"
                      onChange={(e) => {
                        const template = TABLE_TEMPLATES[e.target.value as keyof typeof TABLE_TEMPLATES];
                        if (template) {
                          setColumns(template);
                        }
                      }}
                    >
                      <MenuItem value="">
                        <em>Choose Template</em>
                      </MenuItem>
                      {Object.keys(TABLE_TEMPLATES).map((templateName) => (
                        <MenuItem key={templateName} value={templateName}>
                          {templateName.charAt(0).toUpperCase() + templateName.slice(1)} Table
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Box>
                {columns.map((column, index) => (
                  <Grid container spacing={1} key={index} sx={{ mb: 2 }}>
                    <Grid item xs={12} sm={3}>
                      <TextField
                        fullWidth
                        size="small"
                        label="Column Name"
                        value={column.name}
                        onChange={(e) => updateColumn(index, 'name', e.target.value)}
                        placeholder="e.g., username, email"
                        error={['user', 'order', 'group', 'table', 'column', 'select', 'from', 'where'].includes(column.name.toLowerCase())}
                        helperText={
                          ['user', 'order', 'group', 'table', 'column', 'select', 'from', 'where'].includes(column.name.toLowerCase())
                            ? 'Reserved keyword (will be quoted)'
                            : ''
                        }
                      />
                    </Grid>
                    <Grid item xs={12} sm={4}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <FormControlLabel
                          control={
                            <Switch
                              size="small"
                              checked={column.customType}
                              onChange={(e) => updateColumn(index, 'customType', e.target.checked)}
                            />
                          }
                          label="Custom"
                          sx={{ mb: 0 }}
                        />
                        {column.customType ? (
                          <TextField
                            fullWidth
                            size="small"
                            label="Custom Data Type"
                            value={column.type}
                            onChange={(e) => updateColumn(index, 'type', e.target.value)}
                            placeholder="e.g., VARCHAR(100), DECIMAL(10,2)"
                          />
                        ) : (
                          <FormControl fullWidth size="small">
                            <InputLabel>Data Type</InputLabel>
                            <Select
                              value={column.type}
                              label="Data Type"
                              onChange={(e) => updateColumn(index, 'type', e.target.value)}
                            >
                              {Object.entries(DATA_TYPES).map(([category, types]) => [
                                <MenuItem key={category} disabled sx={{ fontWeight: 'bold', color: 'primary.main' }}>
                                  {category}
                                </MenuItem>,
                                ...types.map((type) => (
                                  <MenuItem key={type.value} value={type.value}>
                                    <Box>
                                      <Typography variant="body2">{type.label}</Typography>
                                      <Typography variant="caption" color="text.secondary">
                                        {type.description}
                                      </Typography>
                                    </Box>
                                  </MenuItem>
                                ))
                              ])}
                            </Select>
                          </FormControl>
                        )}
                      </Box>
                    </Grid>
                    <Grid item xs={12} sm={4}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <FormControlLabel
                          control={
                            <Switch
                              size="small"
                              checked={column.customConstraints}
                              onChange={(e) => updateColumn(index, 'customConstraints', e.target.checked)}
                            />
                          }
                          label="Custom"
                          sx={{ mb: 0 }}
                        />
                        {column.customConstraints ? (
                          <TextField
                            fullWidth
                            size="small"
                            label="Custom Constraints"
                            value={column.constraints}
                            onChange={(e) => updateColumn(index, 'constraints', e.target.value)}
                            placeholder="e.g., NOT NULL DEFAULT 'active'"
                          />
                        ) : (
                          <FormControl fullWidth size="small">
                            <InputLabel>Constraints</InputLabel>
                            <Select
                              value={column.constraints}
                              label="Constraints"
                              onChange={(e) => updateColumn(index, 'constraints', e.target.value)}
                            >
                              {CONSTRAINT_OPTIONS.map((constraint) => (
                                <MenuItem key={constraint.value} value={constraint.value}>
                                  <Box>
                                    <Typography variant="body2">{constraint.label}</Typography>
                                    <Typography variant="caption" color="text.secondary">
                                      {constraint.description}
                                    </Typography>
                                  </Box>
                                </MenuItem>
                              ))}
                            </Select>
                          </FormControl>
                        )}
                      </Box>
                    </Grid>
                    <Grid item xs={12} sm={1}>
                      <IconButton
                        size="small"
                        onClick={() => removeColumn(index)}
                        disabled={columns.length === 1}
                        sx={{ mt: 1 }}
                      >
                        <ClearIcon />
                      </IconButton>
                    </Grid>
                  </Grid>
                ))}
                <Button
                  variant="outlined"
                  startIcon={<AddIcon />}
                  onClick={addColumn}
                  size="small"
                >
                  Add {includeIdColumn ? 'Additional' : ''} Column
                </Button>
              </Grid>
              <Grid item xs={12}>
                <Divider sx={{ my: 2 }} />
                <Typography variant="subtitle2" gutterBottom>
                  Generated SQL:
                </Typography>
                <Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50' }}>
                  <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.875rem' }}>
                    {generateCreateTableSQL() || 'Fill in the form above to generate SQL'}
                  </pre>
                </Paper>
              </Grid>
              <Grid item xs={12}>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    variant="contained"
                    startIcon={<PlayArrowIcon />}
                    onClick={handleCreateTable}
                    disabled={!createTableName.trim() || (!includeIdColumn && columns.filter(c => c.name).length === 0) || loading}
                  >
                    {loading ? 'Creating...' : 'Create Table'}
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<ContentCopyIcon />}
                    onClick={() => copyToClipboard(generateCreateTableSQL())}
                    disabled={!generateCreateTableSQL()}
                  >
                    Copy SQL
                  </Button>
                </Box>
              </Grid>
            </Grid>
          </TabPanel>

          <TabPanel value={activeTab} index={2}>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <TextField
                  fullWidth
                  select
                  label="Table"
                  value={insertTableName}
                  onChange={async (e) => {
                    const tableName = e.target.value;
                    setInsertTableName(tableName);
                    if (tableName) {
                      await fetchTableColumns(tableName);
                      // Auto-populate ALL columns for better UX
                      const response = await sqlAPI.getTableColumns(tableName);
                      const columns = response.data.columns || [];
                      
                      if (columns.length > 0) {
                        // Create insert columns for ALL fields
                        const newInsertColumns = columns.map((col: any) => ({
                          column: col.name,
                          value: '',
                          useDropdown: true
                        }));
                        
                        setInsertColumns(newInsertColumns);
                        
                        // For bulk generation, pre-select required columns and commonly used optional columns
                        const requiredColumns = columns
                          .filter((col: any) => {
                            const isAutoGenerated = col.default?.includes('nextval') || col.default?.includes('sequence');
                            return !isAutoGenerated && (!col.nullable || col.name.toLowerCase().includes('name') || 
                                   col.name.toLowerCase().includes('email') || col.name.toLowerCase().includes('title') ||
                                   col.name.toLowerCase().includes('description') || col.name.toLowerCase().includes('status'));
                          })
                          .map((col: any) => col.name);
                        
                        setSelectedColumns(requiredColumns);
                      }
                    } else {
                      setTableColumns([]);
                      setInsertColumns([{column: '', value: '', useDropdown: true}]);
                    }
                  }}
                  placeholder="Select a table"
                >
                  {tables.map((table) => (
                    <MenuItem key={table} value={table}>
                      {table}
                    </MenuItem>
                  ))}
                </TextField>
              </Grid>
              <Grid item xs={12}>
                <Box sx={{ mb: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Box>
                      <Typography variant="h6" gutterBottom>
                        Values
                      </Typography>
                      {tableColumns.length > 0 && !showBulkGenerate && (
                        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
                          All table columns are shown below. Fill in values for each column (required fields are marked).
                        </Typography>
                      )}
                    </Box>
                    {tableColumns.length > 0 && (
                      <Button
                        variant={showBulkGenerate ? "contained" : "outlined"}
                        size="small"
                        onClick={() => setShowBulkGenerate(!showBulkGenerate)}
                      >
                        {showBulkGenerate ? 'Manual Entry' : 'Generate Test Data'}
                      </Button>
                    )}
                  </Box>
                </Box>
                
                {showBulkGenerate ? (
                  <Paper variant="outlined" sx={{ p: 3, mb: 2 }}>
                    <Typography variant="h6" gutterBottom>
                      Generate Test Data
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                      Automatically generate realistic test data for your table. Data is generated based on column names and types.
                    </Typography>
                    
                    <Grid container spacing={3}>
                      <Grid item xs={12} sm={6}>
                        <FormControl fullWidth>
                          <InputLabel>Number of Rows</InputLabel>
                          <Select
                            value={bulkRowCount}
                            label="Number of Rows"
                            onChange={(e) => setBulkRowCount(e.target.value as 100 | 1000 | 10000)}
                          >
                            <MenuItem value={100}>100 rows</MenuItem>
                            <MenuItem value={1000}>1,000 rows</MenuItem>
                            <MenuItem value={10000}>10,000 rows</MenuItem>
                          </Select>
                        </FormControl>
                      </Grid>
                      
                      <Grid item xs={12}>
                        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                          <Typography variant="subtitle2">
                            Select Columns to Include:
                          </Typography>
                          <Box>
                            <Button
                              size="small"
                              onClick={() => {
                                const allNonAuto = tableColumns
                                  .filter(col => !(col.default?.includes('nextval') || col.default?.includes('sequence')))
                                  .map(col => col.name);
                                setSelectedColumns(allNonAuto);
                              }}
                            >
                              Select All
                            </Button>
                            <Button
                              size="small"
                              onClick={() => {
                                const requiredOnly = tableColumns
                                  .filter(col => {
                                    const isAutoGenerated = col.default?.includes('nextval') || col.default?.includes('sequence');
                                    return !col.nullable && !col.default && !isAutoGenerated;
                                  })
                                  .map(col => col.name);
                                setSelectedColumns(requiredOnly);
                              }}
                            >
                              Required Only
                            </Button>
                          </Box>
                        </Box>
                        <Paper variant="outlined" sx={{ p: 2, maxHeight: 300, overflow: 'auto' }}>
                          {tableColumns.map((column) => {
                            const isAutoGenerated = column.default?.includes('nextval') || column.default?.includes('sequence');
                            const isRequired = !column.nullable && !column.default && !isAutoGenerated;
                            
                            return (
                              <FormControlLabel
                                key={column.name}
                                control={
                                  <Checkbox
                                    checked={selectedColumns.includes(column.name)}
                                    onChange={(e) => {
                                      if (e.target.checked) {
                                        setSelectedColumns([...selectedColumns, column.name]);
                                      } else {
                                        setSelectedColumns(selectedColumns.filter(c => c !== column.name));
                                      }
                                    }}
                                    disabled={isAutoGenerated}
                                  />
                                }
                                label={
                                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                    <Typography variant="body2">{column.name}</Typography>
                                    {isRequired && <Chip label="Required" size="small" color="error" sx={{ height: 20 }} />}
                                    {isAutoGenerated && <Chip label="Auto" size="small" color="success" sx={{ height: 20 }} />}
                                    <Typography variant="caption" color="text.secondary">
                                      ({column.type})
                                    </Typography>
                                  </Box>
                                }
                                sx={{ width: '100%', mb: 1 }}
                              />
                            );
                          })}
                        </Paper>
                      </Grid>
                      
                      <Grid item xs={12}>
                        <Alert severity="info">
                          <AlertTitle>Smart Data Generation</AlertTitle>
                          <ul style={{ margin: 0, paddingLeft: 20 }}>
                            <li>Names: Realistic first and last names</li>
                            <li>Addresses: Real city names and street addresses</li>
                            <li>Emails, usernames: Common patterns</li>
                            <li>Titles, descriptions: Relevant sample text</li>
                            <li>Numbers: Random values within reasonable ranges</li>
                            <li>Dates: Recent dates within the last few years</li>
                          </ul>
                        </Alert>
                      </Grid>
                    </Grid>
                  </Paper>
                ) : (
                  insertColumns.map((col, index) => {
                  const columnInfo = tableColumns.find(tc => tc.name === col.column);
                  const isRequired = columnInfo && !columnInfo.nullable && !columnInfo.default;
                  const isAutoGenerated = (columnInfo?.default && columnInfo.default.includes('nextval')) || 
                                         (columnInfo?.type === 'integer' && columnInfo?.default?.includes('sequence'));
                  
                  // If no table is selected, show simple input fields
                  if (tableColumns.length === 0) {
                    return (
                      <Grid container spacing={1} key={index} sx={{ mb: 1 }}>
                        <Grid item xs={5}>
                          <TextField
                            fullWidth
                            size="small"
                            label="Column"
                            value={col.column}
                            onChange={(e) => updateInsertColumn(index, 'column', e.target.value)}
                            placeholder="e.g., name, email"
                          />
                        </Grid>
                        <Grid item xs={6}>
                          <TextField
                            fullWidth
                            size="small"
                            label="Value"
                            value={col.value}
                            onChange={(e) => updateInsertColumn(index, 'value', e.target.value)}
                            placeholder="e.g., 'John Doe', 123"
                          />
                        </Grid>
                        <Grid item xs={1}>
                          <IconButton
                            size="small"
                            onClick={() => removeInsertColumn(index)}
                            disabled={insertColumns.length === 1}
                          >
                            <ClearIcon />
                          </IconButton>
                        </Grid>
                      </Grid>
                    );
                  }
                  
                  // For table columns, show enhanced UI
                  return (
                    <Grid container spacing={2} key={index} sx={{ mb: 2, alignItems: 'center' }}>
                      <Grid item xs={12} sm={4}>
                        <Box>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                            <Typography variant="subtitle2" sx={{ fontWeight: 'bold' }}>
                              {col.column}
                            </Typography>
                            {isRequired && (
                              <Chip label="Required" size="small" color="error" sx={{ height: 20 }} />
                            )}
                            {isAutoGenerated && (
                              <Chip label="Auto" size="small" color="success" sx={{ height: 20 }} />
                            )}
                          </Box>
                          {columnInfo && (
                            <Typography variant="caption" color="text.secondary">
                              {columnInfo.type}
                              {columnInfo.default && ` • Default: ${columnInfo.default}`}
                            </Typography>
                          )}
                        </Box>
                      </Grid>
                      <Grid item xs={12} sm={7}>
                        <TextField
                          fullWidth
                          size="small"
                          label={isAutoGenerated ? "Value (Optional)" : "Value"}
                          value={col.value}
                          onChange={(e) => updateInsertColumn(index, 'value', e.target.value)}
                          placeholder={
                            isAutoGenerated 
                              ? 'DEFAULT or leave empty' 
                              : columnInfo?.type.includes('VARCHAR') 
                                ? "'text value'"
                                : columnInfo?.type.includes('INT') || columnInfo?.type.includes('NUMERIC')
                                  ? '123'
                                  : columnInfo?.type.includes('BOOL')
                                    ? 'true or false'
                                    : columnInfo?.type.includes('DATE')
                                      ? '2024-01-01'
                                      : columnInfo?.type.includes('TIMESTAMP')
                                        ? '2024-01-01 12:00:00'
                                        : 'Enter value'
                          }
                          error={!!(isRequired && !col.value)}
                          helperText={
                            isAutoGenerated 
                              ? 'Leave empty or use DEFAULT for auto-generated values'
                              : isRequired && !col.value
                                ? 'This field is required'
                                : ''
                          }
                        />
                      </Grid>
                      <Grid item xs={12} sm={1}>
                        <IconButton
                          size="small"
                          onClick={() => removeInsertColumn(index)}
                          disabled={insertColumns.length <= 1}
                          title="Remove column"
                        >
                          <ClearIcon />
                        </IconButton>
                      </Grid>
                    </Grid>
                  );
                })
                )}
                {tableColumns.length === 0 && !showBulkGenerate && (
                  <Button
                    variant="outlined"
                    startIcon={<AddIcon />}
                    onClick={addInsertColumn}
                    size="small"
                  >
                    Add Column
                  </Button>
                )}
              </Grid>
              <Grid item xs={12}>
                <Divider sx={{ my: 2 }} />
                <Typography variant="subtitle2" gutterBottom>
                  Generated SQL:
                </Typography>
                <Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50' }}>
                  <pre style={{ margin: 0, fontFamily: 'monospace', fontSize: '0.875rem' }}>
                    {generateInsertSQL() || 'Fill in the form above to generate SQL'}
                  </pre>
                </Paper>
              </Grid>
              <Grid item xs={12}>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    variant="contained"
                    startIcon={<PlayArrowIcon />}
                    onClick={handleInsertData}
                    disabled={!insertTableName.trim() || (!showBulkGenerate && insertColumns.filter(c => c.column && c.value).length === 0) || loading}
                  >
                    {loading ? 'Inserting...' : showBulkGenerate ? `Generate ${bulkRowCount} Rows` : 'Insert Data'}
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<ContentCopyIcon />}
                    onClick={() => copyToClipboard(generateInsertSQL())}
                    disabled={!generateInsertSQL()}
                  >
                    Copy SQL
                  </Button>
                </Box>
              </Grid>
            </Grid>
          </TabPanel>

          <TabPanel value={activeTab} index={3}>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <Box
                  sx={{
                    border: dragActive ? '2px dashed primary.main' : '2px dashed grey.300',
                    borderRadius: 2,
                    p: 4,
                    textAlign: 'center',
                    bgcolor: dragActive ? 'action.hover' : 'background.paper',
                    cursor: 'pointer',
                    transition: 'all 0.3s ease',
                  }}
                  onDragEnter={handleDrag}
                  onDragLeave={handleDrag}
                  onDragOver={handleDrag}
                  onDrop={handleDrop}
                  onClick={() => document.getElementById('file-input')?.click()}
                >
                  <CloudUploadIcon sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />
                  <Typography variant="h6" gutterBottom>
                    Drag and drop your CSV or Excel file here
                  </Typography>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    or click to browse
                  </Typography>
                  <input
                    id="file-input"
                    type="file"
                    accept=".csv,.xlsx,.xls"
                    onChange={handleFileInput}
                    style={{ display: 'none' }}
                  />
                  {uploadFile && (
                    <Chip
                      label={uploadFile.name}
                      onDelete={() => setUploadFile(null)}
                      color="primary"
                      sx={{ mt: 2 }}
                    />
                  )}
                </Box>
              </Grid>
              
              {uploadFile && (
                <>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Table Name"
                      value={importTableName}
                      onChange={(e) => setImportTableName(e.target.value)}
                      placeholder="Enter table name for imported data"
                      helperText="The table will be created if it doesn't exist"
                    />
                  </Grid>
                  
                  <Grid item xs={12}>
                    <Alert severity="info">
                      <AlertTitle>Import Information</AlertTitle>
                      <ul style={{ margin: 0, paddingLeft: 20 }}>
                        <li>File: {uploadFile.name}</li>
                        <li>Size: {(uploadFile.size / 1024).toFixed(2)} KB</li>
                        <li>Type: {uploadFile.type || 'CSV'}</li>
                        <li>First row will be used as column headers</li>
                        <li>Data types will be automatically detected</li>
                      </ul>
                    </Alert>
                  </Grid>
                  
                  <Grid item xs={12}>
                    <Box sx={{ display: 'flex', gap: 2 }}>
                      <Button
                        variant="contained"
                        startIcon={<CloudUploadIcon />}
                        onClick={handleImport}
                        disabled={!importTableName.trim() || loading}
                      >
                        {loading ? 'Importing...' : 'Import Data'}
                      </Button>
                      <Button
                        variant="outlined"
                        onClick={() => {
                          setUploadFile(null);
                          setImportTableName('');
                        }}
                      >
                        Cancel
                      </Button>
                    </Box>
                  </Grid>
                </>
              )}
            </Grid>
          </TabPanel>
        </Box>
      </Paper>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      )}

      {result && (
        <Paper sx={{ p: 2 }}>
          {/* Check if this is a DDL operation (CREATE, ALTER, DROP) or DML with no results */}
          {(result.row_count === -1 || (result.row_count === 0 && result.columns.length === 0)) ? (
            <Alert 
              severity="success" 
              sx={{ mb: 2 }}
              action={
                <Chip 
                  label={`${result.execution_time.toFixed(3)}s`} 
                  size="small" 
                  color="success" 
                  variant="outlined" 
                />
              }
            >
              <AlertTitle>Query Executed Successfully</AlertTitle>
              {result.executedQuery?.trim().toUpperCase().startsWith('CREATE TABLE') && 'Table created successfully!'}
              {result.executedQuery?.trim().toUpperCase().startsWith('INSERT') && `${result.row_count > 0 ? result.row_count : 1} row(s) inserted successfully!`}
              {result.executedQuery?.trim().toUpperCase().startsWith('UPDATE') && `${result.row_count} row(s) updated successfully!`}
              {result.executedQuery?.trim().toUpperCase().startsWith('DELETE') && `${result.row_count} row(s) deleted successfully!`}
              {result.executedQuery?.trim().toUpperCase().startsWith('DROP') && 'Operation completed successfully!'}
              {result.executedQuery?.trim().toUpperCase().startsWith('ALTER') && 'Table altered successfully!'}
              {!['CREATE', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER'].some(op => 
                result.executedQuery?.trim().toUpperCase().startsWith(op)
              ) && 'Operation completed successfully!'}
            </Alert>
          ) : (
            <>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Typography variant="h6">
                    Results
                  </Typography>
                  <Chip 
                    label={`${result.row_count} rows`} 
                    size="small" 
                    color="primary" 
                    variant="outlined" 
                  />
                  <Chip 
                    label={`${result.execution_time.toFixed(3)}s`} 
                    size="small" 
                    color="success" 
                    variant="outlined" 
                  />
                </Box>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button
                    variant="outlined"
                    size="small"
                    startIcon={<FileDownloadIcon />}
                    onClick={() => handleExport('csv')}
                    disabled={!result.rows || result.rows.length === 0}
                  >
                    Export CSV
                  </Button>
                  <Button
                    variant="outlined"
                    size="small"
                    startIcon={<FileDownloadIcon />}
                    onClick={() => handleExport('excel')}
                    disabled={!result.rows || result.rows.length === 0}
                  >
                    Export Excel
                  </Button>
                </Box>
              </Box>
              <TableContainer sx={{ maxHeight: 600 }}>
                <Table size="small" stickyHeader>
                  <TableHead>
                    <TableRow>
                      {result.columns.map((column: string, index: number) => (
                        <TableCell key={index} sx={{ fontWeight: 'bold', bgcolor: 'grey.100' }}>
                          {column}
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {result.rows.map((row: any[], rowIndex: number) => (
                      <TableRow key={rowIndex} hover>
                        {row.map((cell: any, cellIndex: number) => (
                          <TableCell key={cellIndex}>
                            {cell === null ? (
                              <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                NULL
                              </Typography>
                            ) : (
                              String(cell)
                            )}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </>
          )}
        </Paper>
      )}
    </Box>
  );
};

export default SQLEditorPage;