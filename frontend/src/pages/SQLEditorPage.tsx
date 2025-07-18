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
  CircularProgress,
  Tabs,
  Tab,
  Grid,
  IconButton,
  Menu,
  MenuItem,
  Chip,
  Divider,
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

const SQLEditorPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState(0);
  const [query, setQuery] = useState('');
  const [createTableName, setCreateTableName] = useState('');
  const [columns, setColumns] = useState<Array<{name: string; type: string; constraints: string}>>([{name: '', type: 'VARCHAR(255)', constraints: ''}]);
  const [insertTableName, setInsertTableName] = useState('');
  const [insertColumns, setInsertColumns] = useState<Array<{column: string; value: string}>>([{column: '', value: ''}]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState<any>(null);
  const [tables, setTables] = useState<string[]>([]);
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
      setResult(response.data);
      
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

  React.useEffect(() => {
    fetchTables();
  }, []);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
    setError('');
  };

  const generateCreateTableSQL = () => {
    if (!createTableName.trim()) {
      setError('Please enter a table name');
      return '';
    }

    const validColumns = columns.filter(col => col.name.trim());
    if (validColumns.length === 0) {
      setError('Please add at least one column');
      return '';
    }

    const columnDefs = validColumns.map(col => 
      `${col.name} ${col.type}${col.constraints ? ' ' + col.constraints : ''}`
    ).join(',\n  ');

    return `CREATE TABLE ${createTableName} (\n  ${columnDefs}\n);`;
  };

  const generateInsertSQL = () => {
    if (!insertTableName.trim()) {
      setError('Please select a table');
      return '';
    }

    const validColumns = insertColumns.filter(col => col.column.trim() && col.value.trim());
    if (validColumns.length === 0) {
      setError('Please add at least one column with value');
      return '';
    }

    const columnNames = validColumns.map(col => col.column).join(', ');
    const values = validColumns.map(col => {
      // Add quotes for string values
      if (isNaN(Number(col.value)) && col.value.toUpperCase() !== 'NULL') {
        return `'${col.value.replace(/'/g, "''")}'`;
      }
      return col.value;
    }).join(', ');

    return `INSERT INTO ${insertTableName} (${columnNames})\nVALUES (${values});`;
  };

  const handleCreateTable = () => {
    const sql = generateCreateTableSQL();
    if (sql) {
      executeQuery(sql);
    }
  };

  const handleInsertData = () => {
    const sql = generateInsertSQL();
    if (sql) {
      executeQuery(sql);
    }
  };

  const addColumn = () => {
    setColumns([...columns, {name: '', type: 'VARCHAR(255)', constraints: ''}]);
  };

  const removeColumn = (index: number) => {
    setColumns(columns.filter((_, i) => i !== index));
  };

  const updateColumn = (index: number, field: 'name' | 'type' | 'constraints', value: string) => {
    const updated = [...columns];
    updated[index][field] = value;
    setColumns(updated);
  };

  const addInsertColumn = () => {
    setInsertColumns([...insertColumns, {column: '', value: ''}]);
  };

  const removeInsertColumn = (index: number) => {
    setInsertColumns(insertColumns.filter((_, i) => i !== index));
  };

  const updateInsertColumn = (index: number, field: 'column' | 'value', value: string) => {
    const updated = [...insertColumns];
    updated[index][field] = value;
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
                />
              </Grid>
              <Grid item xs={12}>
                <Typography variant="h6" gutterBottom>
                  Columns
                </Typography>
                {columns.map((column, index) => (
                  <Grid container spacing={1} key={index} sx={{ mb: 1 }}>
                    <Grid item xs={4}>
                      <TextField
                        fullWidth
                        size="small"
                        label="Column Name"
                        value={column.name}
                        onChange={(e) => updateColumn(index, 'name', e.target.value)}
                        placeholder="e.g., id, name"
                      />
                    </Grid>
                    <Grid item xs={3}>
                      <TextField
                        fullWidth
                        size="small"
                        label="Data Type"
                        value={column.type}
                        onChange={(e) => updateColumn(index, 'type', e.target.value)}
                        placeholder="e.g., INT, VARCHAR(255)"
                      />
                    </Grid>
                    <Grid item xs={4}>
                      <TextField
                        fullWidth
                        size="small"
                        label="Constraints"
                        value={column.constraints}
                        onChange={(e) => updateColumn(index, 'constraints', e.target.value)}
                        placeholder="e.g., PRIMARY KEY, NOT NULL"
                      />
                    </Grid>
                    <Grid item xs={1}>
                      <IconButton
                        size="small"
                        onClick={() => removeColumn(index)}
                        disabled={columns.length === 1}
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
                    disabled={!createTableName.trim() || columns.filter(c => c.name).length === 0 || loading}
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
                  onChange={(e) => setInsertTableName(e.target.value)}
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
        </Paper>
      )}
    </Box>
  );
};

export default SQLEditorPage;