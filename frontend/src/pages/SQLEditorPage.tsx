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
  Menu,
  MenuItem,
  Chip,
  Divider,
  FormControlLabel,
  Switch,
  FormControl,
  InputLabel,
  Select,
} from '@mui/material';
import {
  PlayArrow as PlayArrowIcon,
  TableChart as TableChartIcon,
  Add as AddIcon,
  FileDownload as FileDownloadIcon,
  MoreVert as MoreVertIcon,
  ContentCopy as ContentCopyIcon,
  Clear as ClearIcon,
} from '@mui/icons-material';
import { sqlAPI, exportAPI } from '../services/api';
import { SQLEditor } from '../components/sql/SQLEditor';

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
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);

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

    const validColumns = insertColumns.filter(col => col.column.trim() && col.value.trim());
    if (validColumns.length === 0) {
      setError('Please add at least one column with value');
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

  const handleExportClick = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleExportClose = () => {
    setAnchorEl(null);
  };

  const handleExport = async (format: 'csv' | 'excel') => {
    if (!result || !result.rows || result.rows.length === 0) {
      setError('No data to export');
      handleExportClose();
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
    } catch {
      setError('Export failed');
    } finally {
      handleExportClose();
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
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
                  onChange={(e) => {
                    const tableName = e.target.value;
                    setInsertTableName(tableName);
                    if (tableName) {
                      fetchTableColumns(tableName);
                    } else {
                      setTableColumns([]);
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
                <Typography variant="h6" gutterBottom>
                  Values
                </Typography>
                {insertColumns.map((col, index) => (
                  <Grid container spacing={1} key={index} sx={{ mb: 1 }}>
                    <Grid item xs={5}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <FormControlLabel
                          control={
                            <Switch
                              size="small"
                              checked={col.useDropdown}
                              onChange={(e) => updateInsertColumn(index, 'useDropdown', e.target.checked)}
                              disabled={tableColumns.length === 0}
                            />
                          }
                          label="Select"
                          sx={{ mb: 0 }}
                        />
                        {col.useDropdown && tableColumns.length > 0 ? (
                          <TextField
                            fullWidth
                            size="small"
                            select
                            label="Column"
                            value={col.column}
                            onChange={(e) => updateInsertColumn(index, 'column', e.target.value)}
                            disabled={tableColumns.length === 0}
                          >
                            {tableColumns.map((column) => (
                              <MenuItem key={column.name} value={column.name}>
                                <Box>
                                  <Typography variant="body2">{column.name}</Typography>
                                  <Typography variant="caption" color="text.secondary">
                                    {column.type} {column.nullable ? '' : 'NOT NULL'} {column.default ? `DEFAULT ${column.default}` : ''}
                                  </Typography>
                                </Box>
                              </MenuItem>
                            ))}
                          </TextField>
                        ) : (
                          <TextField
                            fullWidth
                            size="small"
                            label="Column Name"
                            value={col.column}
                            onChange={(e) => updateInsertColumn(index, 'column', e.target.value)}
                            placeholder="e.g., name, email"
                            helperText={col.column.toLowerCase().includes('id') ? 'ID columns are usually auto-generated' : ''}
                          />
                        )}
                      </Box>
                    </Grid>
                    <Grid item xs={6}>
                      <TextField
                        fullWidth
                        size="small"
                        label="Value"
                        value={col.value}
                        onChange={(e) => updateInsertColumn(index, 'value', e.target.value)}
                        placeholder={col.column.toLowerCase().includes('id') ? 'Leave empty for auto-increment' : "e.g., 'John Doe', 123"}
                        helperText={col.column.toLowerCase().includes('id') ? 'Use DEFAULT for auto-increment' : ''}
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
                ))}
                <Button
                  variant="outlined"
                  startIcon={<AddIcon />}
                  onClick={addInsertColumn}
                  size="small"
                >
                  Add Column
                </Button>
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
                    disabled={!insertTableName.trim() || insertColumns.filter(c => c.column && c.value).length === 0 || loading}
                  >
                    {loading ? 'Inserting...' : 'Insert Data'}
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
                <Box>
                  <IconButton onClick={handleExportClick} disabled={!result.rows || result.rows.length === 0}>
                    <MoreVertIcon />
                  </IconButton>
                  <Menu
                    anchorEl={anchorEl}
                    open={Boolean(anchorEl)}
                    onClose={handleExportClose}
                  >
                    <MenuItem onClick={() => handleExport('csv')}>
                      <FileDownloadIcon sx={{ mr: 1 }} /> Export as CSV
                    </MenuItem>
                    <MenuItem onClick={() => handleExport('excel')}>
                      <FileDownloadIcon sx={{ mr: 1 }} /> Export as Excel
                    </MenuItem>
                  </Menu>
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