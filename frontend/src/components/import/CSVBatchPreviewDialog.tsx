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
} from '@mui/material';
import {
  ExpandMore as ExpandMoreIcon,
  TableChart as TableChartIcon,
} from '@mui/icons-material';
import { importAPI } from '../../services/api';
import CSVSQLPreviewDialog from './CSVSQLPreviewDialog';

interface FilePreview {
  file: File;
  tableName: string;
  preview?: any;
  error?: string;
  loading: boolean;
  customSQL?: string;
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
  const [selectedFileIndex, setSelectedFileIndex] = useState<number | null>(null);
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
        const response = await importAPI.previewCSV(files[i]);
        setFilePreviews(prev => prev.map((fp, index) => 
          index === i 
            ? { ...fp, preview: response.data, loading: false }
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

  const handleEditTableStructure = (index: number) => {
    setSelectedFileIndex(index);
  };

  const handleSQLUpdated = (sql: string) => {
    if (selectedFileIndex !== null) {
      setFilePreviews(prev => prev.map((fp, index) => 
        index === selectedFileIndex 
          ? { ...fp, customSQL: sql }
          : fp
      ));
    }
  };

  const handleBatchImport = async () => {
    setImporting(true);
    
    try {
      // For files with custom SQL, use the import-with-sql endpoint
      const importPromises = filePreviews.map(async (fp) => {
        if (fp.customSQL) {
          return importAPI.importCSVWithSQL(fp.file, fp.customSQL, fp.tableName);
        } else {
          return importAPI.uploadCSV(fp.file, fp.tableName, true, true);
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
    <>
      <Dialog
        open={open && selectedFileIndex === null}
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
              <Accordion key={index} defaultExpanded={files.length <= 3}>
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
                    {filePreview.customSQL && (
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
                    <Box>
                      <Typography variant="subtitle2" gutterBottom>
                        Proposed CREATE TABLE statement:
                      </Typography>
                      <Box 
                        sx={{ 
                          bgcolor: 'grey.50', 
                          p: 2, 
                          borderRadius: 1, 
                          fontFamily: 'monospace',
                          fontSize: '0.875rem',
                          border: '1px solid',
                          borderColor: 'grey.300',
                          mb: 2,
                          maxHeight: 200,
                          overflow: 'auto'
                        }}
                      >
                        <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
                          {filePreview.customSQL || filePreview.preview.create_table_sql}
                        </pre>
                      </Box>

                      <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                        <Chip 
                          label={`${filePreview.preview.total_rows} rows`} 
                          size="small" 
                          variant="outlined" 
                        />
                        <Chip 
                          label={`${filePreview.preview.columns.length} columns`} 
                          size="small" 
                          variant="outlined" 
                        />
                        <Chip 
                          label={`${(filePreview.file.size / 1024).toFixed(1)} KB`} 
                          size="small" 
                          variant="outlined" 
                        />
                      </Box>

                      <Button
                        variant="outlined"
                        size="small"
                        onClick={() => handleEditTableStructure(index)}
                      >
                        Edit Table Structure
                      </Button>
                    </Box>
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

      {/* Individual file edit dialog */}
      {selectedFileIndex !== null && filePreviews[selectedFileIndex] && (
        <CSVSQLPreviewDialog
          open={true}
          onClose={() => setSelectedFileIndex(null)}
          file={filePreviews[selectedFileIndex].file}
          tableName={filePreviews[selectedFileIndex].tableName}
          onImport={(sql?: string) => {
            if (sql) {
              handleSQLUpdated(sql);
            }
            setSelectedFileIndex(null);
          }}
        />
      )}
    </>
  );
};

export default CSVBatchPreviewDialog;