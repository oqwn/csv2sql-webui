import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Alert,
  CircularProgress,
  Typography,
  Paper,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
} from '@mui/material';
import { SQLEditor } from '../sql/SQLEditor';
import { importAPI } from '../../services/api';

interface Props {
  open: boolean;
  onClose: () => void;
  file: File;
  tableName: string;
  onImport: (sql?: string) => void;
}

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
      {...other}
    >
      {value === index && <Box sx={{ pt: 2 }}>{children}</Box>}
    </div>
  );
}

const CSVSQLPreviewDialog: React.FC<Props> = ({
  open,
  onClose,
  file,
  tableName,
  onImport,
}) => {
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState<string>('');
  const [tabValue, setTabValue] = useState(0);
  const [createTableSQL, setCreateTableSQL] = useState('');
  const [editedSQL, setEditedSQL] = useState('');
  const [preview, setPreview] = useState<{
    columns: Array<any>;
    sample_data: Array<any>;
    total_rows: number;
  } | null>(null);

  useEffect(() => {
    if (open && file) {
      loadPreview();
    }
  }, [open, file, tableName]);

  const loadPreview = async () => {
    setLoading(true);
    setError('');
    try {
      const isExcel = file.name.endsWith('.xlsx') || file.name.endsWith('.xls');
      let response;
      
      if (isExcel) {
        response = await importAPI.previewExcel(file);
      } else {
        response = await importAPI.previewCSV(file, tableName);
      }
      
      const data = response.data;
      
      // Validate response data structure
      if (!data || !data.columns || !data.sample_data || !data.create_table_sql) {
        throw new Error('Invalid response structure from preview API');
      }
      
      setPreview({
        columns: data.columns,
        sample_data: data.sample_data,
        total_rows: data.total_rows,
      });
      setCreateTableSQL(data.create_table_sql);
      setEditedSQL(data.create_table_sql);
    } catch (err: any) {
      const fileType = file.name.endsWith('.xlsx') || file.name.endsWith('.xls') ? 'Excel' : 'CSV';
      setError(err.response?.data?.detail || `Failed to preview ${fileType} file`);
    } finally {
      setLoading(false);
    }
  };

  const handleImport = async () => {
    setImporting(true);
    setError('');
    try {
      // Always call onImport with the SQL - the parent component will decide what to do
      onImport(editedSQL);
      onClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to import CSV file');
    } finally {
      setImporting(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle>
        Import File - Preview & Edit Schema
      </DialogTitle>
      <DialogContent>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
            <CircularProgress />
          </Box>
        ) : error ? (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        ) : preview && preview.columns && Array.isArray(preview.columns) && preview.sample_data && Array.isArray(preview.sample_data) ? (
          <>
            <Alert severity="info" sx={{ mb: 2 }}>
              <Typography variant="body2">
                File: {file.name} | Total rows: {preview.total_rows} | Columns: {preview.columns.length}
              </Typography>
            </Alert>

            <Tabs value={tabValue} onChange={(_, v) => setTabValue(v)}>
              <Tab label="CREATE TABLE SQL" />
              <Tab label="Data Preview" />
              <Tab label="Column Details" />
            </Tabs>

            <TabPanel value={tabValue} index={0}>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Edit the CREATE TABLE statement below to customize column types and constraints:
              </Typography>
              <Box sx={{ height: 400, overflow: 'auto' }}>
                <SQLEditor
                  value={editedSQL}
                  onChange={setEditedSQL}
                  rows={15}
                />
              </Box>
              <Box sx={{ mt: 2, display: 'flex', gap: 2 }}>
                <Button
                  size="small"
                  onClick={() => setEditedSQL(createTableSQL)}
                  disabled={editedSQL === createTableSQL}
                >
                  Reset to Original
                </Button>
              </Box>
            </TabPanel>

            <TabPanel value={tabValue} index={1}>
              <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
                <Table stickyHeader size="small">
                  <TableHead>
                    <TableRow>
                      {preview.columns.map((col) => (
                        <TableCell key={col.name}>
                          {col.name}
                          <Typography variant="caption" display="block" color="text.secondary">
                            {col.suggested_type}
                          </Typography>
                        </TableCell>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {preview.sample_data.map((row, idx) => (
                      <TableRow key={idx}>
                        {preview.columns.map((col) => (
                          <TableCell key={col.name}>
                            {row[col.name] === null ? (
                              <Chip label="NULL" size="small" variant="outlined" />
                            ) : (
                              String(row[col.name])
                            )}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </TabPanel>

            <TabPanel value={tabValue} index={2}>
              <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Column Name</TableCell>
                      <TableCell>Suggested Type</TableCell>
                      <TableCell align="center">Nullable</TableCell>
                      <TableCell align="center">Unique Values</TableCell>
                      <TableCell align="center">Null Count</TableCell>
                      <TableCell>Sample Values</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {preview.columns.map((col) => (
                      <TableRow key={col.name}>
                        <TableCell>{col.name}</TableCell>
                        <TableCell>
                          <Chip label={col.suggested_type} size="small" />
                        </TableCell>
                        <TableCell align="center">
                          {col.nullable ? '✓' : '✗'}
                        </TableCell>
                        <TableCell align="center">{col.unique_values}</TableCell>
                        <TableCell align="center">{col.null_count}</TableCell>
                        <TableCell>
                          <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                            {col.sample_values.slice(0, 3).map((val: any, i: number) => (
                              <Chip
                                key={i}
                                label={String(val)}
                                size="small"
                                variant="outlined"
                              />
                            ))}
                          </Box>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </TabPanel>
          </>
        ) : preview ? (
          <Alert severity="warning" sx={{ mb: 2 }}>
            Preview data is incomplete or malformed. Please try again or contact support.
          </Alert>
        ) : null}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={importing}>
          Cancel
        </Button>
        <Button
          onClick={handleImport}
          variant="contained"
          disabled={importing || !preview || !editedSQL.trim()}
        >
          {importing ? 'Importing...' : 'Execute & Import'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default CSVSQLPreviewDialog;