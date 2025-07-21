import React from 'react';
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
  Paper,
  Typography,
  Box,
  Chip,
  Grid,
  Alert,
} from '@mui/material';
import { TransformationPreviewResponse } from '../../types/transformation.types';

interface Props {
  open: boolean;
  onClose: () => void;
  previewData: TransformationPreviewResponse | null;
}

const TransformationPreview: React.FC<Props> = ({ open, onClose, previewData }) => {
  if (!previewData) {
    return null;
  }

  const formatValue = (value: any): string => {
    if (value === null || value === undefined) {
      return '-';
    }
    if (typeof value === 'object') {
      return JSON.stringify(value);
    }
    return String(value);
  };

  const getDataTypeIcon = (dtype: string): string => {
    if (dtype.includes('int')) return '🔢';
    if (dtype.includes('float')) return '🔢';
    if (dtype.includes('object') || dtype.includes('string')) return '📝';
    if (dtype.includes('datetime')) return '📅';
    if (dtype.includes('bool')) return '✓';
    return '❓';
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="lg" fullWidth>
      <DialogTitle>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography variant="h6">Transformation Preview</Typography>
          <Box>
            <Chip
              label={`${previewData.transformations_applied} transformations applied`}
              color="primary"
              size="small"
            />
          </Box>
        </Box>
      </DialogTitle>
      <DialogContent dividers>
        <Grid container spacing={2} sx={{ mb: 3 }}>
          <Grid item xs={6}>
            <Alert severity="info" icon={false}>
              <Typography variant="subtitle2">Original Data</Typography>
              <Typography variant="body2">
                {previewData.original_shape.rows} rows × {previewData.original_shape.columns} columns
              </Typography>
            </Alert>
          </Grid>
          <Grid item xs={6}>
            <Alert severity="success" icon={false}>
              <Typography variant="subtitle2">Transformed Data</Typography>
              <Typography variant="body2">
                {previewData.transformed_shape.rows} rows × {previewData.transformed_shape.columns} columns
              </Typography>
            </Alert>
          </Grid>
        </Grid>

        <Typography variant="subtitle2" gutterBottom>
          Column Types
        </Typography>
        <Box sx={{ mb: 3, display: 'flex', flexWrap: 'wrap', gap: 1 }}>
          {Object.entries(previewData.data_types).map(([column, dtype]) => (
            <Chip
              key={column}
              label={`${getDataTypeIcon(dtype)} ${column}: ${dtype}`}
              size="small"
              variant="outlined"
            />
          ))}
        </Box>

        <Typography variant="subtitle2" gutterBottom>
          Preview Data (First {previewData.preview.length} rows)
        </Typography>
        <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
          <Table stickyHeader size="small">
            <TableHead>
              <TableRow>
                {previewData.columns.map((column) => (
                  <TableCell key={column}>
                    <Typography variant="subtitle2">{column}</Typography>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {previewData.preview.map((row, index) => (
                <TableRow key={index}>
                  {previewData.columns.map((column) => (
                    <TableCell key={column}>
                      <Typography variant="body2" sx={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {formatValue(row[column])}
                      </Typography>
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {previewData.preview.length === 0 && (
          <Alert severity="warning" sx={{ mt: 2 }}>
            The transformation resulted in no data. Please check your transformation steps.
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} variant="contained">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default TransformationPreview;