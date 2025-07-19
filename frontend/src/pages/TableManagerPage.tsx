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
} from '@mui/material';
import {
  Edit as EditIcon,
  Delete as DeleteIcon,
  Add as AddIcon,
  Refresh as RefreshIcon,
  Search as SearchIcon,
  TableChart as TableChartIcon,
  Save as SaveIcon,
  Cancel as CancelIcon,
} from '@mui/icons-material';
import { sqlAPI } from '../services/api';

interface TableData {
  columns: string[];
  rows: any[][];
  totalRows: number;
}

interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
  isPrimary?: boolean;
}

const TableManagerPage: React.FC = () => {
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  
  // Pagination
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  
  // Edit state
  const [editingRow, setEditingRow] = useState<number | null>(null);
  const [editedData, setEditedData] = useState<Record<string, any>>({});
  
  // Add/Edit dialog
  const [dialogOpen, setDialogOpen] = useState(false);
  const [dialogMode, setDialogMode] = useState<'add' | 'edit'>('add');
  const [formData, setFormData] = useState<Record<string, any>>({});
  
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
    const loadTableData = async () => {
      if (selectedTable) {
        await fetchTableData();
        await fetchColumnInfo();
      }
    };
    loadTableData();
  }, [selectedTable, page, rowsPerPage]); // eslint-disable-line react-hooks/exhaustive-deps

  const fetchTables = async () => {
    try {
      const response = await sqlAPI.getTables();
      setTables(response.data.tables || []);
    } catch (err) {
      console.error('Failed to fetch tables:', err);
    }
  };

  const fetchColumnInfo = async () => {
    if (!selectedTable) return;
    try {
      const response = await sqlAPI.getTableColumns(selectedTable);
      setColumns(response.data.columns || []);
    } catch (err) {
      console.error('Failed to fetch column info:', err);
    }
  };

  const fetchTableData = async () => {
    if (!selectedTable) return;
    
    setLoading(true);
    setError('');
    
    try {
      let query = `SELECT * FROM "${selectedTable}"`;
      
      // Add search filter
      if (searchQuery && filterColumn) {
        query += ` WHERE "${filterColumn}" ILIKE '%${searchQuery}%'`;
      }
      
      // Add pagination
      query += ` LIMIT ${rowsPerPage} OFFSET ${page * rowsPerPage}`;
      
      const response = await sqlAPI.executeQuery(query);
      
      // Get total count
      let countQuery = `SELECT COUNT(*) as total FROM "${selectedTable}"`;
      if (searchQuery && filterColumn) {
        countQuery += ` WHERE "${filterColumn}" ILIKE '%${searchQuery}%'`;
      }
      const countResponse = await sqlAPI.executeQuery(countQuery);
      const totalRows = countResponse.data.rows[0][0];
      
      setTableData({
        columns: response.data.columns,
        rows: response.data.rows,
        totalRows: totalRows,
      });
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to fetch table data');
    } finally {
      setLoading(false);
    }
  };

  const handleEdit = (rowIndex: number) => {
    const row = tableData?.rows[rowIndex];
    if (!row || !tableData) return;
    
    const rowData: Record<string, any> = {};
    tableData.columns.forEach((col, idx) => {
      rowData[col] = row[idx];
    });
    
    setEditingRow(rowIndex);
    setEditedData(rowData);
  };

  const handleSaveEdit = async () => {
    if (editingRow === null || !tableData) return;
    
    setLoading(true);
    try {
      // Build UPDATE query
      const setClauses = Object.entries(editedData)
        .map(([col, val]) => {
          if (val === null) return `"${col}" = NULL`;
          if (typeof val === 'string') return `"${col}" = '${val.replace(/'/g, "''")}'`;
          return `"${col}" = ${val}`;
        })
        .join(', ');
      
      // Assuming first column is primary key
      const primaryKey = tableData.columns[0];
      const primaryValue = tableData.rows[editingRow][0];
      const whereClause = typeof primaryValue === 'string' 
        ? `"${primaryKey}" = '${primaryValue}'`
        : `"${primaryKey}" = ${primaryValue}`;
      
      const updateQuery = `UPDATE "${selectedTable}" SET ${setClauses} WHERE ${whereClause}`;
      
      await sqlAPI.executeQuery(updateQuery);
      setSuccess('Record updated successfully');
      setEditingRow(null);
      fetchTableData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to update record');
    } finally {
      setLoading(false);
    }
  };

  const handleCancelEdit = () => {
    setEditingRow(null);
    setEditedData({});
  };

  const handleAdd = () => {
    const newRecord: Record<string, any> = {};
    columns.forEach(col => {
      newRecord[col.name] = col.default || '';
    });
    setFormData(newRecord);
    setDialogMode('add');
    setDialogOpen(true);
  };


  const handleSaveDialog = async () => {
    setLoading(true);
    try {
      if (dialogMode === 'add') {
        // Build INSERT query
        const columns = Object.keys(formData).filter(col => formData[col] !== '');
        const values = columns.map(col => {
          const val = formData[col];
          if (val === null || val === 'NULL') return 'NULL';
          if (typeof val === 'string') return `'${val.replace(/'/g, "''")}'`;
          return val;
        });
        
        const insertQuery = `INSERT INTO "${selectedTable}" ("${columns.join('", "')}") VALUES (${values.join(', ')})`;
        await sqlAPI.executeQuery(insertQuery);
        setSuccess('Record added successfully');
      } else {
        // Build UPDATE query
        const setClauses = Object.entries(formData)
          .filter(([_, val]) => val !== '')
          .map(([col, val]) => {
            if (val === null || val === 'NULL') return `"${col}" = NULL`;
            if (typeof val === 'string') return `"${col}" = '${val.replace(/'/g, "''")}'`;
            return `"${col}" = ${val}`;
          })
          .join(', ');
        
        // Assuming first column is primary key
        const primaryKey = tableData?.columns[0];
        const primaryValue = formData[primaryKey!];
        const whereClause = typeof primaryValue === 'string' 
          ? `"${primaryKey}" = '${primaryValue}'`
          : `"${primaryKey}" = ${primaryValue}`;
        
        const updateQuery = `UPDATE "${selectedTable}" SET ${setClauses} WHERE ${whereClause}`;
        await sqlAPI.executeQuery(updateQuery);
        setSuccess('Record updated successfully');
      }
      
      setDialogOpen(false);
      fetchTableData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to save record');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = (row: any[]) => {
    setDeleteTarget(row);
    setDeleteDialogOpen(true);
  };

  const confirmDelete = async () => {
    if (!deleteTarget || !tableData) return;
    
    setLoading(true);
    try {
      // Assuming first column is primary key
      const primaryKey = tableData.columns[0];
      const primaryValue = deleteTarget[0];
      const whereClause = typeof primaryValue === 'string' 
        ? `"${primaryKey}" = '${primaryValue}'`
        : `"${primaryKey}" = ${primaryValue}`;
      
      const deleteQuery = `DELETE FROM "${selectedTable}" WHERE ${whereClause}`;
      await sqlAPI.executeQuery(deleteQuery);
      
      setSuccess('Record deleted successfully');
      setDeleteDialogOpen(false);
      fetchTableData();
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

  const getColumnType = (columnName: string): string => {
    const column = columns.find(col => col.name === columnName);
    return column?.type || 'text';
  };

  const renderCellValue = (value: any, columnName: string) => {
    if (value === null) {
      return <Chip label="NULL" size="small" variant="outlined" />;
    }
    
    const type = getColumnType(columnName);
    if (type.includes('bool')) {
      return <Chip label={value ? 'TRUE' : 'FALSE'} size="small" color={value ? 'success' : 'default'} />;
    }
    
    if (type.includes('json')) {
      return (
        <Tooltip title={<pre>{JSON.stringify(value, null, 2)}</pre>}>
          <Chip label="JSON" size="small" variant="outlined" />
        </Tooltip>
      );
    }
    
    return String(value);
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
                </Typography>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <TextField
                    size="small"
                    placeholder="Search..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    onKeyPress={(e) => {
                      if (e.key === 'Enter') fetchTableData();
                    }}
                    InputProps={{
                      startAdornment: (
                        <InputAdornment position="start">
                          <SearchIcon />
                        </InputAdornment>
                      ),
                    }}
                  />
                  <FormControl size="small" sx={{ minWidth: 120 }}>
                    <InputLabel>Filter by</InputLabel>
                    <Select
                      value={filterColumn}
                      label="Filter by"
                      onChange={(e) => setFilterColumn(e.target.value)}
                    >
                      <MenuItem value="">None</MenuItem>
                      {columns.map((col) => (
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
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                  <CircularProgress />
                </Box>
              ) : error ? (
                <Alert severity="error" sx={{ mb: 2 }}>
                  {error}
                </Alert>
              ) : tableData ? (
                <>
                  <TableContainer>
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          {tableData.columns.map((column, idx) => (
                            <TableCell key={idx} sx={{ fontWeight: 'bold' }}>
                              {column}
                              {columns.find(c => c.name === column)?.isPrimary && (
                                <Chip label="PK" size="small" sx={{ ml: 1 }} />
                              )}
                            </TableCell>
                          ))}
                          <TableCell align="right" sx={{ fontWeight: 'bold' }}>
                            Actions
                          </TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {tableData.rows.map((row, rowIndex) => (
                          <TableRow key={rowIndex} hover>
                            {row.map((cell, cellIndex) => (
                              <TableCell key={cellIndex}>
                                {editingRow === rowIndex ? (
                                  <TextField
                                    size="small"
                                    value={editedData[tableData.columns[cellIndex]] ?? ''}
                                    onChange={(e) => {
                                      setEditedData({
                                        ...editedData,
                                        [tableData.columns[cellIndex]]: e.target.value,
                                      });
                                    }}
                                  />
                                ) : (
                                  renderCellValue(cell, tableData.columns[cellIndex])
                                )}
                              </TableCell>
                            ))}
                            <TableCell align="right">
                              {editingRow === rowIndex ? (
                                <Box sx={{ display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
                                  <IconButton
                                    size="small"
                                    color="success"
                                    onClick={handleSaveEdit}
                                  >
                                    <SaveIcon />
                                  </IconButton>
                                  <IconButton
                                    size="small"
                                    color="error"
                                    onClick={handleCancelEdit}
                                  >
                                    <CancelIcon />
                                  </IconButton>
                                </Box>
                              ) : (
                                <Box sx={{ display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
                                  <IconButton
                                    size="small"
                                    color="primary"
                                    onClick={() => handleEdit(rowIndex)}
                                  >
                                    <EditIcon />
                                  </IconButton>
                                  <IconButton
                                    size="small"
                                    color="error"
                                    onClick={() => handleDelete(row)}
                                  >
                                    <DeleteIcon />
                                  </IconButton>
                                </Box>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                  <TablePagination
                    rowsPerPageOptions={[10, 25, 50, 100]}
                    component="div"
                    count={tableData.totalRows}
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
      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>
          {dialogMode === 'add' ? 'Add New Record' : 'Edit Record'}
        </DialogTitle>
        <DialogContent>
          <Grid container spacing={2} sx={{ mt: 1 }}>
            {columns.map((col) => (
              <Grid item xs={12} key={col.name}>
                <TextField
                  fullWidth
                  label={`${col.name} (${col.type})`}
                  value={formData[col.name] ?? ''}
                  onChange={(e) => {
                    setFormData({
                      ...formData,
                      [col.name]: e.target.value,
                    });
                  }}
                  helperText={
                    col.nullable ? 'Nullable' : 'Required'
                  }
                  disabled={col.isPrimary && dialogMode === 'edit'}
                />
              </Grid>
            ))}
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={handleSaveDialog}>
            Save
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
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" color="error" onClick={confirmDelete}>
            Delete
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