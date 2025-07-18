import React, { useState } from 'react';
import {
  Box,
  Paper,
  Button,
  Typography,
  TextField,
  Alert,
  LinearProgress,
} from '@mui/material';
import { CloudUpload as CloudUploadIcon } from '@mui/icons-material';
import { importAPI } from '../services/api';

const ImportPage: React.FC = () => {
  const [file, setFile] = useState<File | null>(null);
  const [tableName, setTableName] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (files && files[0]) {
      setFile(files[0]);
      if (!tableName) {
        const suggestedName = files[0].name
          .replace(/\.[^/.]+$/, '')
          .toLowerCase()
          .replace(/[^a-z0-9]/g, '_');
        setTableName(suggestedName);
      }
    }
  };

  const handleUpload = async () => {
    if (!file) return;

    setLoading(true);
    setError('');
    setSuccess('');

    try {
      const response = await importAPI.uploadCSV(file, tableName);
      setSuccess(response.data.message);
      setFile(null);
      setTableName('');
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Upload failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Import Data
      </Typography>
      
      <Paper sx={{ p: 3, maxWidth: 600 }}>
        <Typography variant="h6" gutterBottom>
          CSV Import
        </Typography>
        <Typography variant="body2" color="text.secondary" paragraph>
          Upload a CSV file to import data into the database
        </Typography>

        <Box sx={{ mb: 3 }}>
          <input
            accept=".csv"
            style={{ display: 'none' }}
            id="file-upload"
            type="file"
            onChange={handleFileChange}
          />
          <label htmlFor="file-upload">
            <Button
              variant="outlined"
              component="span"
              startIcon={<CloudUploadIcon />}
            >
              Choose File
            </Button>
          </label>
          {file && (
            <Typography variant="body2" sx={{ mt: 1 }}>
              Selected: {file.name}
            </Typography>
          )}
        </Box>

        <TextField
          fullWidth
          label="Table Name"
          value={tableName}
          onChange={(e) => setTableName(e.target.value)}
          margin="normal"
          helperText="Leave empty to use filename as table name"
        />

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        {success && (
          <Alert severity="success" sx={{ mt: 2 }}>
            {success}
          </Alert>
        )}

        {loading && <LinearProgress sx={{ mt: 2 }} />}

        <Button
          variant="contained"
          onClick={handleUpload}
          disabled={!file || loading}
          sx={{ mt: 3 }}
        >
          Import Data
        </Button>
      </Paper>
    </Box>
  );
};

export default ImportPage;