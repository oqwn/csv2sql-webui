import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  IconButton,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  TextField,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  Chip,
  CircularProgress,
  Tooltip,
  MenuItem,
  FormControl,
  InputLabel,
  Select,
  Snackbar,
  Grid,
  Card,
  CardContent,
  Divider,
  InputAdornment,
  Skeleton,
  FormControlLabel,
  Checkbox,
} from '@mui/material';
import {
  Edit as EditIcon,
  Delete as DeleteIcon,
  Add as AddIcon,
  Refresh as RefreshIcon,
  Search as SearchIcon,
  TableChart as TableChartIcon,
  Info as InfoIcon,
} from '@mui/icons-material';
import { sqlAPI, tableAPI } from '../services/api';

interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
  is_primary?: boolean;
  is_unique?: boolean;
  foreign_key?: {
    table: string;
    column: string;
  } | null;
}

interface TableInfo {
  table_name: string;
  columns: ColumnInfo[];
  primary_key: string | null;
}

const TableManagerPage: React.FC = () => {
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [tableInfo, setTableInfo] = useState<TableInfo | null>(null);
  const [tableData, setTableData] = useState<any[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  
  // Pagination
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  
  // Add/Edit dialog
  const [dialogOpen, setDialogOpen] = useState(false);
  const [dialogMode, setDialogMode] = useState<'add' | 'edit'>('add');
  const [formData, setFormData] = useState<Record<string, any>>({});
  const [editingRecord, setEditingRecord] = useState<any>(null);
  
  // Delete confirmation
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<any>(null);
  
  // Search and filter
  const [searchQuery, setSearchQuery] = useState('');
  const [filterColumn, setFilterColumn] = useState('');

  useEffect(() => {
    fetchTables();
  }, []);

  useEffect(() => {
    if (selectedTable) {
      setTableInfo(null); // Reset table info when table changes
      const loadTableData = async () => {
        await fetchTableInfo();
        await fetchTableData();
      };
      loadTableData();
    }
  }, [selectedTable]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (selectedTable) {
      fetchTableData();
    }
  }, [page, rowsPerPage, searchQuery, filterColumn]); // eslint-disable-line react-hooks/exhaustive-deps

  const fetchTables = async () => {
    try {
      const response = await sqlAPI.getTables();
      setTables(response.data.tables || []);
    } catch (err) {
      console.error('Failed to fetch tables:', err);
    }
  };

  const fetchTableInfo = async () => {
    if (!selectedTable) return;
    try {
      console.log('Fetching table info for:', selectedTable);
      const response = await tableAPI.getTableInfo(selectedTable);
      console.log('Table info response:', response.data);
      
      if (response.data && response.data.table_name) {
        setTableInfo(response.data);
        console.log('Table info set successfully:', response.data);
      } else {
        console.error('Invalid table info response:', response.data);
        setError('Invalid table information received');
      }
    } catch (err: any) {
      console.error('Failed to fetch table info:', err);
      setError(err.response?.data?.detail || 'Failed to fetch table information');
    }
  };

  const fetchTableData = async () => {
    if (!selectedTable) return;
    
    setLoading(true);
    setError('');
    
    try {
      // Try to get primary key from tableInfo, otherwise let backend determine it
      const params: any = {
        table_name: selectedTable,
        page: page,
        page_size: rowsPerPage,
        search_column: filterColumn || undefined,
        search_value: searchQuery || undefined,
        order_direction: 'ASC',
      };
      
      // If we have tableInfo, use its primary key; otherwise try 'id'
      if (tableInfo?.primary_key) {
        params.order_by = tableInfo.primary_key;
      } else {
        // Try to order by 'id' - backend will handle if column doesn't exist
        params.order_by = 'id';
      }
      
      const response = await tableAPI.getTableData(params);
      
      setTableData(response.data.rows);
      setTotalRows(response.data.total_count);
      
      // Update table info primary key if not set
      if (tableInfo && !tableInfo.primary_key && response.data.primary_key) {
        setTableInfo(prev => prev ? { ...prev, primary_key: response.data.primary_key } : null);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to fetch table data');
      setTableData([]);
    } finally {
      setLoading(false);
    }
  };

  const handleAdd = () => {
    const newRecord: Record<string, any> = {};
    tableInfo?.columns.forEach(col => {
      if (!col.is_primary || col.type.toLowerCase().includes('serial')) {
        newRecord[col.name] = col.default || '';
      }
    });
    setFormData(newRecord);
    setEditingRecord(null);
    setDialogMode('add');
    setDialogOpen(true);
  };

  const handleEdit = (record: any) => {
    console.log('Editing record:', record);
    console.log('Table info:', tableInfo);
    
    const editData = { ...record };
    
    // Format datetime values for HTML5 datetime-local input
    tableInfo?.columns.forEach(column => {
      const value = editData[column.name];
      if (value && column.type.toLowerCase().includes('timestamp')) {
        // Convert from database format (with microseconds) to HTML5 datetime-local format
        // HTML5 datetime-local expects: yyyy-MM-ddThh:mm or yyyy-MM-ddThh:mm:ss or yyyy-MM-ddThh:mm:ss.SSS
        const date = new Date(value);
        if (!isNaN(date.getTime())) {
          // Format to yyyy-MM-ddThh:mm:ss.SSS (truncate microseconds to milliseconds)
          editData[column.name] = date.toISOString().slice(0, 23);
        }
      }
    });
    
    // Always remove primary key from form data for updates
    // The backend will handle the WHERE clause using the primary key value separately
    const primaryKey = tableInfo?.primary_key;
    const primaryKeyColumn = tableInfo?.columns.find(col => col.name === primaryKey);
    
    console.log('Primary key info:', { primaryKey, primaryKeyColumn });
    
    if (primaryKey) {
      console.log('Removing primary key from form data to prevent overwriting');
      delete editData[primaryKey];
    }
    
    console.log('Form data for edit:', editData);
    
    setFormData(editData);
    setEditingRecord(record);
    setDialogMode('edit');
    setDialogOpen(true);
  };

  const handleSaveDialog = async () => {
    setLoading(true);
    try {
      if (dialogMode === 'add') {
        console.log('Creating record:', { selectedTable, formData });
        await tableAPI.createRecord(selectedTable, formData);
        setSuccess('Record added successfully');
      } else {
        const primaryKey = tableInfo?.primary_key;
        if (!primaryKey || !editingRecord) {
          console.error('Missing primary key or editing record:', { primaryKey, editingRecord });
          throw new Error('Primary key not found');
        }
        
        const primaryKeyValue = editingRecord[primaryKey];
        console.log('Updating record:', { 
          selectedTable, 
          primaryKey, 
          primaryKeyValue, 
          formData,
          editingRecord 
        });
        
        await tableAPI.updateRecord(
          selectedTable,
          primaryKey,
          primaryKeyValue,
          formData
        );
        setSuccess('Record updated successfully');
      }
      
      setDialogOpen(false);
      await fetchTableData();
    } catch (err: any) {
      console.error('Save dialog error:', err);
      setError(err.response?.data?.detail || err.message || 'Failed to save record');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = (record: any) => {
    setDeleteTarget(record);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = async () => {
    if (!deleteTarget || !tableInfo?.primary_key) return;
    
    setLoading(true);
    try {
      await tableAPI.deleteRecord(
        selectedTable,
        tableInfo.primary_key,
        deleteTarget[tableInfo.primary_key]
      );
      
      setSuccess('Record deleted successfully');
      setDeleteDialogOpen(false);
      await fetchTableData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete record');
    } finally {
      setLoading(false);
    }
  };

  const handleChangePage = (_event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const renderCellValue = (value: any, columnName: string) => {
    if (value === null || value === undefined) {
      return <Chip label="NULL" size="small" variant="outlined" />;
    }
    
    const column = tableInfo?.columns.find(col => col.name === columnName);
    const type = column?.type || '';
    
    if (type.toLowerCase().includes('bool')) {
      return <Chip label={value ? 'TRUE' : 'FALSE'} size="small" color={value ? 'success' : 'default'} />;
    }
    
    if (type.toLowerCase().includes('json')) {
      return (
        <Tooltip title={<pre style={{ margin: 0 }}>{JSON.stringify(value, null, 2)}</pre>}>
          <Chip label="JSON" size="small" variant="outlined" icon={<InfoIcon />} />
        </Tooltip>
      );
    }
    
    if (type.toLowerCase().includes('timestamp') || type.toLowerCase().includes('date')) {
      try {
        return new Date(value).toLocaleString();
      } catch {
        return String(value);
      }
    }
    
    return String(value);
  };

  const getFieldType = (columnType: string): string => {
    const type = columnType.toLowerCase();
    if (type.includes('int') || type.includes('serial')) return 'number';
    if (type.includes('bool')) return 'checkbox';
    if (type.includes('date') && !type.includes('timestamp')) return 'date';
    if (type.includes('timestamp')) return 'datetime-local';
    if (type.includes('text') || type.includes('json')) return 'textarea';
    return 'text';
  };

  const renderFormField = (column: ColumnInfo) => {
    const fieldType = getFieldType(column.type);
    const value = formData[column.name] ?? '';
    
    if (fieldType === 'checkbox') {
      return (
        <FormControl fullWidth>
          <FormControlLabel
            control={
              <Checkbox
                checked={value === true || value === 't' || value === 'true'}
                onChange={(e) => setFormData({ ...formData, [column.name]: e.target.checked })}
              />
            }
            label={column.name}
          />
        </FormControl>
      );
    }
    
    if (fieldType === 'textarea') {
      return (
        <TextField
          fullWidth
          multiline
          rows={4}
          label={column.name}
          value={value}
          onChange={(e) => setFormData({ ...formData, [column.name]: e.target.value })}
          helperText={`Type: ${column.type}${column.nullable ? ' (Optional)' : ' (Required)'}`}
        />
      );
    }
    
    return (
      <TextField
        fullWidth
        type={fieldType}
        label={column.name}
        value={value}
        onChange={(e) => setFormData({ ...formData, [column.name]: e.target.value })}
        helperText={`Type: ${column.type}${column.nullable ? ' (Optional)' : ' (Required)'}`}
        InputLabelProps={
          fieldType === 'date' || fieldType === 'datetime-local'
            ? { shrink: true }
            : undefined
        }
      />
    );
  };

  return (
    <Box>
      <Grid container spacing={3}>
        {/* Header */}
        <Grid item xs={12}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            <Typography variant="h4" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <TableChartIcon sx={{ fontSize: 32 }} />
              Database Tables
            </Typography>
            <Button
              variant="contained"
              startIcon={<RefreshIcon />}
              onClick={() => {
                fetchTables();
                if (selectedTable) fetchTableData();
              }}
            >
              Refresh
            </Button>
          </Box>
        </Grid>

        {/* Table Selection */}
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Tables
              </Typography>
              <Divider sx={{ mb: 2 }} />
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                {tables.map((table) => (
                  <Button
                    key={table}
                    variant={selectedTable === table ? 'contained' : 'outlined'}
                    onClick={() => {
                      setSelectedTable(table);
                      setPage(0);
                      setSearchQuery('');
                      setFilterColumn('');
                    }}
                    sx={{ justifyContent: 'flex-start' }}
                  >
                    {table}
                  </Button>
                ))}
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Table Data */}
        <Grid item xs={12} md={9}>
          {selectedTable ? (
            <Paper sx={{ p: 3 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="h5">
                  {selectedTable}
                  {tableInfo?.primary_key && (
                    <Chip 
                      label={`PK: ${tableInfo.primary_key}`} 
                      size="small" 
                      sx={{ ml: 2 }} 
                      color="primary" 
                      variant="outlined" 
                    />
                  )}
                </Typography>
                <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                  <TextField
                    size="small"
                    placeholder="Search..."
                    value={searchQuery}
                    onChange={(e) => {
                      setSearchQuery(e.target.value);
                      setPage(0);
                    }}
                    InputProps={{
                      startAdornment: (
                        <InputAdornment position="start">
                          <SearchIcon />
                        </InputAdornment>
                      ),
                    }}
                  />
                  <FormControl size="small" sx={{ minWidth: 150 }}>
                    <InputLabel>Filter by</InputLabel>
                    <Select
                      value={filterColumn}
                      label="Filter by"
                      onChange={(e) => {
                        setFilterColumn(e.target.value);
                        setPage(0);
                      }}
                    >
                      <MenuItem value="">All columns</MenuItem>
                      {tableInfo?.columns.map((col) => (
                        <MenuItem key={col.name} value={col.name}>
                          {col.name}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                  <Button
                    variant="contained"
                    color="success"
                    startIcon={<AddIcon />}
                    onClick={handleAdd}
                  >
                    Add Record
                  </Button>
                </Box>
              </Box>

              {loading ? (
                <Box>
                  {[...Array(5)].map((_, i) => (
                    <Skeleton key={i} height={60} sx={{ mb: 1 }} />
                  ))}
                </Box>
              ) : error ? (
                <Alert severity="error" sx={{ mb: 2 }}>
                  {error}
                </Alert>
              ) : tableData.length > 0 ? (
                <>
                  <TableContainer>
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          {tableInfo?.columns.map((column) => (
                            <TableCell key={column.name} sx={{ fontWeight: 'bold' }}>
                              {column.name}
                              {column.is_primary && (
                                <Chip label="PK" size="small" sx={{ ml: 1 }} />
                              )}
                              {column.foreign_key && (
                                <Tooltip title={`References ${column.foreign_key.table}.${column.foreign_key.column}`}>
                                  <Chip label="FK" size="small" sx={{ ml: 1 }} color="secondary" />
                                </Tooltip>
                              )}
                            </TableCell>
                          ))}
                          <TableCell align="right" sx={{ fontWeight: 'bold' }}>
                            Actions
                          </TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {tableData.map((row, rowIndex) => (
                          <TableRow key={rowIndex} hover>
                            {tableInfo?.columns.map((column) => (
                              <TableCell key={column.name}>
                                {renderCellValue(row[column.name], column.name)}
                              </TableCell>
                            ))}
                            <TableCell align="right">
                              <Box sx={{ display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
                                <IconButton
                                  size="small"
                                  color="primary"
                                  onClick={() => handleEdit(row)}
                                  title="Edit record"
                                >
                                  <EditIcon />
                                </IconButton>
                                <IconButton
                                  size="small"
                                  color="error"
                                  onClick={() => handleDelete(row)}
                                  title="Delete record"
                                >
                                  <DeleteIcon />
                                </IconButton>
                              </Box>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                  <TablePagination
                    rowsPerPageOptions={[10, 25, 50, 100]}
                    component="div"
                    count={totalRows}
                    rowsPerPage={rowsPerPage}
                    page={page}
                    onPageChange={handleChangePage}
                    onRowsPerPageChange={handleChangeRowsPerPage}
                  />
                </>
              ) : (
                <Typography color="text.secondary" sx={{ textAlign: 'center', py: 4 }}>
                  No data available
                </Typography>
              )}
            </Paper>
          ) : (
            <Paper sx={{ p: 4, textAlign: 'center' }}>
              <TableChartIcon sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
              <Typography variant="h6" color="text.secondary">
                Select a table to view and manage data
              </Typography>
            </Paper>
          )}
        </Grid>
      </Grid>

      {/* Add/Edit Dialog */}
      <Dialog 
        open={dialogOpen} 
        onClose={() => setDialogOpen(false)} 
        maxWidth="md" 
        fullWidth
        PaperProps={{
          sx: { maxHeight: '90vh' }
        }}
      >
        <DialogTitle>
          {dialogMode === 'add' ? 'Add New Record' : 'Edit Record'}
          {tableInfo?.primary_key && editingRecord && dialogMode === 'edit' && (
            <Chip 
              label={`${tableInfo.primary_key}: ${editingRecord[tableInfo.primary_key]}`} 
              size="small" 
              sx={{ ml: 2 }} 
              color="primary" 
            />
          )}
        </DialogTitle>
        <DialogContent>
          <Grid container spacing={2} sx={{ mt: 1 }}>
            {tableInfo?.columns
              .filter(col => {
                // Skip primary key if it's auto-generated (SERIAL)
                if (col.is_primary && col.type.toLowerCase().includes('serial')) {
                  return false;
                }
                // In edit mode, skip primary key
                if (dialogMode === 'edit' && col.is_primary) {
                  return false;
                }
                return true;
              })
              .map((column) => (
                <Grid item xs={12} sm={6} key={column.name}>
                  {renderFormField(column)}
                </Grid>
              ))}
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button 
            variant="contained" 
            onClick={handleSaveDialog}
            disabled={loading}
          >
            {loading ? <CircularProgress size={20} /> : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)}>
        <DialogTitle>Confirm Delete</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to delete this record? This action cannot be undone.
          </Typography>
          {tableInfo?.primary_key && deleteTarget && (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Deleting record with {tableInfo.primary_key}: {deleteTarget[tableInfo.primary_key]}
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>
          <Button 
            variant="contained" 
            color="error" 
            onClick={confirmDelete}
            disabled={loading}
          >
            {loading ? <CircularProgress size={20} /> : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Success Snackbar */}
      <Snackbar
        open={!!success}
        autoHideDuration={3000}
        onClose={() => setSuccess('')}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert severity="success" onClose={() => setSuccess('')}>
          {success}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default TableManagerPage;