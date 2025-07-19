import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Alert,
  CircularProgress,
  Chip,
  LinearProgress,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
} from '@mui/material';
import {
  ExpandMore as ExpandMoreIcon,
  TableChart as TableChartIcon,
} from '@mui/icons-material';
import { importAPI, sqlAPI } from '../../services/api';
import { SQLEditor } from '../sql/SQLEditor';

interface FilePreview {
  file: File;
  tableName: string;
  preview?: any;
  error?: string;
  loading: boolean;
  customSQL?: string;
  originalSQL?: string;
  tabValue?: number;
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

interface Props {
  open: boolean;
  onClose: () => void;
  files: File[];
  onImport: () => void;
}

const CSVBatchPreviewDialog: React.FC<Props> = ({
  open,
  onClose,
  files,
  onImport,
}) => {
  const [filePreviews, setFilePreviews] = useState<FilePreview[]>([]);
  const [importing, setImporting] = useState(false);
  const [loadingProgress, setLoadingProgress] = useState(0);

  useEffect(() => {
    if (open && files.length > 0) {
      initializePreviews();
    }
  }, [open, files]);

  const initializePreviews = async () => {
    const previews: FilePreview[] = files.map(file => {
      const tableName = file.name
        .replace(/\.csv$/, '')
        .toLowerCase()
        .replace(/[^a-z0-9]/g, '_');
      
      return {
        file,
        tableName,
        loading: true,
      };
    });

    setFilePreviews(previews);
    
    // Load previews for each file
    for (let i = 0; i < files.length; i++) {
      try {
        const isExcel = files[i].name.endsWith('.xlsx') || files[i].name.endsWith('.xls');
        const response = isExcel 
          ? await importAPI.previewExcel(files[i])
          : await importAPI.previewCSV(files[i]);
        setFilePreviews(prev => prev.map((fp, index) => 
          index === i 
            ? { 
                ...fp, 
                preview: response.data, 
                originalSQL: response.data.create_table_sql,
                customSQL: response.data.create_table_sql,
                tabValue: 0,
                loading: false 
              }
            : fp
        ));
      } catch (error: any) {
        setFilePreviews(prev => prev.map((fp, index) => 
          index === i 
            ? { ...fp, error: error.response?.data?.detail || 'Failed to preview file', loading: false }
            : fp
        ));
      }
      
      setLoadingProgress((i + 1) / files.length * 100);
    }
  };

  const handleTabChange = (fileIndex: number, newValue: number) => {
    setFilePreviews(prev => prev.map((fp, index) => 
      index === fileIndex 
        ? { ...fp, tabValue: newValue }
        : fp
    ));
  };

  const handleSQLChange = (fileIndex: number, newSQL: string) => {
    setFilePreviews(prev => prev.map((fp, index) => 
      index === fileIndex 
        ? { ...fp, customSQL: newSQL }
        : fp
    ));
  };

  const handleResetSQL = (fileIndex: number) => {
    setFilePreviews(prev => prev.map((fp, index) => 
      index === fileIndex 
        ? { ...fp, customSQL: fp.originalSQL }
        : fp
    ));
  };

  const handleBatchImport = async () => {
    setImporting(true);
    
    try {
      // Import files with custom SQL by executing SQL first, then importing data
      const importPromises = filePreviews.map(async (fp) => {
        const isExcel = fp.file.name.endsWith('.xlsx') || fp.file.name.endsWith('.xls');
        
        if (fp.customSQL) {
          // Execute the CREATE TABLE SQL first
          await sqlAPI.executeQuery(fp.customSQL);
          
          // Then import the data into the created table
          if (isExcel) {
            return importAPI.uploadExcel(fp.file, fp.tableName, undefined, false, false, false);
          } else {
            return importAPI.uploadCSV(fp.file, fp.tableName, false, false);
          }
        } else {
          // Standard import with auto-detection
          if (isExcel) {
            return importAPI.uploadExcel(fp.file, fp.tableName, undefined, false, true, true);
          } else {
            return importAPI.uploadCSV(fp.file, fp.tableName, true, true);
          }
        }
      });

      await Promise.all(importPromises);
      onImport();
    } catch (error) {
      console.error('Batch import failed:', error);
    } finally {
      setImporting(false);
    }
  };

  const allPreviewsLoaded = filePreviews.every(fp => !fp.loading);
  const hasErrors = filePreviews.some(fp => fp.error);
  const validPreviews = filePreviews.filter(fp => fp.preview && !fp.error);

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: { height: '90vh' }
      }}
    >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <TableChartIcon />
            <Typography variant="h6">
              Preview Table Structures ({files.length} files)
            </Typography>
          </Box>
        </DialogTitle>

        <DialogContent>
          {!allPreviewsLoaded && (
            <Box sx={{ mb: 3 }}>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Loading previews... {Math.round(loadingProgress)}%
              </Typography>
              <LinearProgress variant="determinate" value={loadingProgress} />
            </Box>
          )}

          {hasErrors && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              Some files could not be previewed. You can still import the valid files.
            </Alert>
          )}

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {filePreviews.map((filePreview, index) => (
              <Accordion key={index} defaultExpanded={files.length <= 2}>
                <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 'medium' }}>
                      {filePreview.file.name}
                    </Typography>
                    <Chip 
                      label={`→ ${filePreview.tableName}`} 
                      size="small" 
                      variant="outlined"
                      color="primary"
                    />
                    {filePreview.loading && (
                      <CircularProgress size={16} />
                    )}
                    {filePreview.error && (
                      <Chip label="Error" size="small" color="error" />
                    )}
                    {filePreview.customSQL !== filePreview.originalSQL && (
                      <Chip label="Customized" size="small" color="success" />
                    )}
                  </Box>
                </AccordionSummary>

                <AccordionDetails>
                  {filePreview.loading && (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, py: 2 }}>
                      <CircularProgress size={20} />
                      <Typography variant="body2" color="text.secondary">
                        Analyzing file structure...
                      </Typography>
                    </Box>
                  )}

                  {filePreview.error && (
                    <Alert severity="error">
                      {filePreview.error}
                    </Alert>
                  )}

                  {filePreview.preview && (
                    <>
                      <Alert severity="info" sx={{ mb: 2 }}>
                        <Typography variant="body2">
                          File: {filePreview.file.name} | Total rows: {filePreview.preview.total_rows} | Columns: {filePreview.preview.columns.length}
                        </Typography>
                      </Alert>

                      <Tabs 
                        value={filePreview.tabValue || 0} 
                        onChange={(_, v) => handleTabChange(index, v)}
                        sx={{ mb: 1 }}
                      >
                        <Tab label="CREATE TABLE SQL" />
                        <Tab label="Data Preview" />
                        <Tab label="Column Details" />
                      </Tabs>

                      <TabPanel value={filePreview.tabValue || 0} index={0}>
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                          Edit the CREATE TABLE statement below to customize column types and constraints:
                        </Typography>
                        <Box sx={{ height: 300, overflow: 'auto' }}>
                          <SQLEditor
                            value={filePreview.customSQL || ''}
                            onChange={(sql) => handleSQLChange(index, sql)}
                            rows={12}
                          />
                        </Box>
                        <Box sx={{ mt: 2, display: 'flex', gap: 2 }}>
                          <Button
                            size="small"
                            onClick={() => handleResetSQL(index)}
                            disabled={filePreview.customSQL === filePreview.originalSQL}
                          >
                            Reset to Original
                          </Button>
                        </Box>
                      </TabPanel>

                      <TabPanel value={filePreview.tabValue || 0} index={1}>
                        <TableContainer component={Paper} sx={{ maxHeight: 300 }}>
                          <Table stickyHeader size="small">
                            <TableHead>
                              <TableRow>
                                {filePreview.preview.columns.map((col: any) => (
                                  <TableCell key={col?.name || 'unnamed'}>
                                    {col?.name || 'Column'}
                                    <Typography variant="caption" display="block" color="text.secondary">
                                      {col?.suggested_type || 'Unknown'}
                                    </Typography>
                                  </TableCell>
                                ))}
                              </TableRow>
                            </TableHead>
                            <TableBody>
                              {filePreview.preview.sample_data.map((row: any, idx: number) => (
                                <TableRow key={idx}>
                                  {filePreview.preview.columns.map((col: any) => (
                                    <TableCell key={col?.name || 'unnamed'}>
                                      {row[col?.name] === null ? (
                                        <Chip label="NULL" size="small" variant="outlined" />
                                      ) : (
                                        String(row[col?.name || 'unknown'])
                                      )}
                                    </TableCell>
                                  ))}
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </TableContainer>
                      </TabPanel>

                      <TabPanel value={filePreview.tabValue || 0} index={2}>
                        <TableContainer component={Paper} sx={{ maxHeight: 300 }}>
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
                              {filePreview.preview.columns.map((col: any) => (
                                <TableRow key={col?.name || 'unnamed'}>
                                  <TableCell>{col?.name || 'Column'}</TableCell>
                                  <TableCell>
                                    <Chip label={col?.suggested_type || 'Unknown'} size="small" />
                                  </TableCell>
                                  <TableCell align="center">
                                    {col?.nullable ? '✓' : '✗'}
                                  </TableCell>
                                  <TableCell align="center">{col?.unique_values || 0}</TableCell>
                                  <TableCell align="center">{col?.null_count || 0}</TableCell>
                                  <TableCell>
                                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                                      {col.sample_values && Array.isArray(col.sample_values) ? col.sample_values.slice(0, 3).map((val: any, i: number) => (
                                        <Chip
                                          key={i}
                                          label={String(val)}
                                          size="small"
                                          variant="outlined"
                                        />
                                      )) : (
                                        <Typography variant="caption" color="text.secondary">
                                          No samples available
                                        </Typography>
                                      )}
                                    </Box>
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </TableContainer>
                      </TabPanel>
                    </>
                  )}
                </AccordionDetails>
              </Accordion>
            ))}
          </Box>
        </DialogContent>

        <DialogActions>
          <Button onClick={onClose} disabled={importing}>
            Cancel
          </Button>
          <Button
            onClick={handleBatchImport}
            variant="contained"
            disabled={importing || validPreviews.length === 0}
          >
            {importing ? 'Importing...' : `Import ${validPreviews.length} Tables`}
          </Button>
        </DialogActions>
    </Dialog>
  );
};

export default CSVBatchPreviewDialog;