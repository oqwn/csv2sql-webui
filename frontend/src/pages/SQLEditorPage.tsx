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
} from '@mui/material';
import { PlayArrow as PlayArrowIcon } from '@mui/icons-material';
import { sqlAPI } from '../services/api';

const SQLEditorPage: React.FC = () => {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [result, setResult] = useState<any>(null);

  const executeQuery = async () => {
    setLoading(true);
    setError('');
    setResult(null);
    
    try {
      const response = await sqlAPI.executeQuery(query);
      setResult(response.data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Query execution failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        SQL Editor
      </Typography>
      
      <Paper sx={{ p: 2, mb: 2 }}>
        <TextField
          fullWidth
          multiline
          rows={10}
          variant="outlined"
          placeholder="Enter your SQL query here..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          sx={{
            fontFamily: 'monospace',
            '& .MuiInputBase-input': {
              fontFamily: 'monospace',
            },
          }}
        />
        <Box sx={{ mt: 2 }}>
          <Button
            variant="contained"
            startIcon={<PlayArrowIcon />}
            onClick={executeQuery}
            disabled={!query.trim() || loading}
          >
            {loading ? 'Executing...' : 'Execute Query'}
          </Button>
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
          <Typography variant="h6" gutterBottom>
            Results ({result.row_count} rows, {result.execution_time.toFixed(3)}s)
          </Typography>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  {result.columns.map((column: string, index: number) => (
                    <TableCell key={index}>{column}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {result.rows.map((row: any[], rowIndex: number) => (
                  <TableRow key={rowIndex}>
                    {row.map((cell: any, cellIndex: number) => (
                      <TableCell key={cellIndex}>
                        {cell === null ? 'NULL' : String(cell)}
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