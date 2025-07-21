import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Select,
  MenuItem,
  Checkbox,
  FormControl,
  Paper,
  Box,
  Alert,
  CircularProgress,
  Typography,
  Chip,
} from '@mui/material';
import { importAPI } from '../../services/api';

interface ColumnConfig {
  name: string;
  original_name: string;
  type: string;
  nullable: boolean;
  primary_key: boolean;
  unique: boolean;
  default_value?: string;
}

interface CSVPreview {
  columns: Array<{
    name: string;
    original_name: string;
    suggested_type: string;
    nullable: boolean;
    unique_values: number;
    null_count: number;
    sample_values: any[];
  }>;
  sample_data: Record<string, any>[];
  total_rows: number;
}

interface Props {
  open: boolean;
  onClose: () => void;
  file: File;
  tableName: string;
  onImport: () => void;
  currentDataSourceId: number;
}

const SQL_TYPES = [
  'INTEGER',
  'BIGINT',
  'BIGSERIAL',
  'SMALLINT',
  'SERIAL',
  'DOUBLE PRECISION',
  'NUMERIC',
  'VARCHAR(255)',
  'TEXT',
  'BOOLEAN',
  'DATE',
  'TIMESTAMP',
  'TIME',
  'JSON',
  'JSONB',
];

const CSVColumnConfigDialog: React.FC<Props> = ({
  open,
  onClose,
  file,
  tableName,
  onImport,
  currentDataSourceId,
}) => {
  const [loading, setLoading] = useState(true);
  const [preview, setPreview] = useState<CSVPreview | null>(null);
  const [columnConfigs, setColumnConfigs] = useState<ColumnConfig[]>([]);
  const [error, setError] = useState<string>('');
  const [importing, setImporting] = useState(false);

  useEffect(() => {
    if (open && file) {
      loadPreview();
    }
  }, [open, file]);

  const loadPreview = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await importAPI.previewCSV(file);
      const data = response.data as CSVPreview;
      setPreview(data);
      
      // Initialize column configurations
      const configs: ColumnConfig[] = data.columns.map((col, _index) => ({
        name: col.name.toLowerCase().replace(/[^a-z0-9_]/g, '_'),
        original_name: col.original_name,
        type: col.suggested_type,
        nullable: col.nullable,
        primary_key: col.name.toLowerCase() === 'id',
        unique: col.unique_values === data.total_rows,
        default_value: undefined,
      }));
      setColumnConfigs(configs);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to preview CSV file');
    } finally {
      setLoading(false);
    }
  };

  const handleColumnConfigChange = (
    index: number,
    field: keyof ColumnConfig,
    value: any
  ) => {
    const updatedConfigs = [...columnConfigs];
    updatedConfigs[index] = {
      ...updatedConfigs[index],
      [field]: value,
    };
    
    // If setting primary key, ensure only one column has it
    if (field === 'primary_key' && value === true) {
      updatedConfigs.forEach((config, i) => {
        if (i !== index) {
          config.primary_key = false;
        }
      });
    }
    
    setColumnConfigs(updatedConfigs);
  };

  const handleImport = async () => {
    setImporting(true);
    setError('');
    try {
      await importAPI.importCSVWithConfig(currentDataSourceId!, file, {
        table_name: tableName,
        columns: columnConfigs,
      });
      onImport();
      onClose();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to import CSV file');
    } finally {
      setImporting(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle>Configure CSV Import - {tableName}</DialogTitle>
      <DialogContent>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
            <CircularProgress />
          </Box>
        ) : error ? (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        ) : preview ? (
          <>
            <Alert severity="info" sx={{ mb: 2 }}>
              <Typography variant="body2">
                Total rows: {preview.total_rows} | Columns: {preview.columns.length}
              </Typography>
            </Alert>
            
            <TableContainer component={Paper} sx={{ maxHeight: 400, mb: 2 }}>
              <Table stickyHeader size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Column Name</TableCell>
                    <TableCell>Data Type</TableCell>
                    <TableCell align="center">Nullable</TableCell>
                    <TableCell align="center">Primary Key</TableCell>
                    <TableCell align="center">Unique</TableCell>
                    <TableCell>Default Value</TableCell>
                    <TableCell>Sample Values</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {columnConfigs.map((config, index) => (
                    <TableRow key={index}>
                      <TableCell>
                        <TextField
                          size="small"
                          value={config.name}
                          onChange={(e) =>
                            handleColumnConfigChange(index, 'name', e.target.value)
                          }
                          fullWidth
                        />
                      </TableCell>
                      <TableCell>
                        <FormControl size="small" fullWidth>
                          <Select
                            value={config.type}
                            onChange={(e) =>
                              handleColumnConfigChange(index, 'type', e.target.value)
                            }
                          >
                            {SQL_TYPES.map((type) => (
                              <MenuItem key={type} value={type}>
                                {type}
                              </MenuItem>
                            ))}
                          </Select>
                        </FormControl>
                      </TableCell>
                      <TableCell align="center">
                        <Checkbox
                          checked={config.nullable}
                          onChange={(e) =>
                            handleColumnConfigChange(index, 'nullable', e.target.checked)
                          }
                          disabled={config.primary_key}
                        />
                      </TableCell>
                      <TableCell align="center">
                        <Checkbox
                          checked={config.primary_key}
                          onChange={(e) =>
                            handleColumnConfigChange(index, 'primary_key', e.target.checked)
                          }
                        />
                      </TableCell>
                      <TableCell align="center">
                        <Checkbox
                          checked={config.unique}
                          onChange={(e) =>
                            handleColumnConfigChange(index, 'unique', e.target.checked)
                          }
                          disabled={config.primary_key}
                        />
                      </TableCell>
                      <TableCell>
                        <TextField
                          size="small"
                          value={config.default_value || ''}
                          onChange={(e) =>
                            handleColumnConfigChange(index, 'default_value', e.target.value)
                          }
                          placeholder="NULL"
                          fullWidth
                        />
                      </TableCell>
                      <TableCell>
                        <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                          {preview.columns[index]?.sample_values.slice(0, 3).map((val, i) => (
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
            
            <Typography variant="caption" color="text.secondary">
              Review and adjust column configurations before importing. Primary key columns are automatically set to NOT NULL and UNIQUE.
            </Typography>
          </>
        ) : null}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={importing}>
          Cancel
        </Button>
        <Button
          onClick={handleImport}
          variant="contained"
          disabled={importing || !preview || columnConfigs.length === 0}
        >
          {importing ? 'Importing...' : 'Import'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default CSVColumnConfigDialog;