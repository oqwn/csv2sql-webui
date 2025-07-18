import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  Paper,
  TextField,
  Typography,
  Chip,
  Stack,
  useTheme,
} from '@mui/material';
import { SQLHighlighter, defaultTheme, darkTheme } from '../../utils/sql/sqlHighlighter';
import { SQLValidator } from '../../utils/sql/sqlValidator';

interface SQLEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute?: () => void;
  placeholder?: string;
  rows?: number;
  error?: string;
  readOnly?: boolean;
}

export const SQLEditor: React.FC<SQLEditorProps> = ({
  value,
  onChange,
  onExecute,
  placeholder = 'Enter your SQL query here...',
  rows = 10,
  error,
  readOnly = false,
}) => {
  const theme = useTheme();
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [validation, setValidation] = useState<ReturnType<typeof SQLValidator.validate> | null>(null);
  const [cursorPosition, setCursorPosition] = useState(0);
  const textFieldRef = useRef<HTMLTextAreaElement>(null);
  
  const highlighter = new SQLHighlighter(
    theme.palette.mode === 'dark' ? darkTheme : defaultTheme
  );

  useEffect(() => {
    // Validate SQL on change
    if (value) {
      const result = SQLValidator.validate(value);
      setValidation(result);
    } else {
      setValidation(null);
    }
  }, [value]);

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement | HTMLInputElement>) => {
    onChange(e.target.value);
    if ('selectionStart' in e.target && e.target.selectionStart !== null) {
      setCursorPosition(e.target.selectionStart);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    // Execute on Ctrl/Cmd + Enter
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter' && onExecute) {
      e.preventDefault();
      onExecute();
    }

    // Auto-complete on Tab
    if (e.key === 'Tab' && suggestions.length > 0) {
      e.preventDefault();
      insertSuggestion(suggestions[0]);
    }
  };

  const insertSuggestion = (suggestion: string) => {
    if (!textFieldRef.current) return;

    const textarea = textFieldRef.current;
    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    
    // Find the word being typed
    const beforeCursor = value.substring(0, start);
    const afterCursor = value.substring(end);
    const words = beforeCursor.split(/\s+/);
    const lastWord = words[words.length - 1];
    const replaceStart = beforeCursor.lastIndexOf(lastWord);
    
    const newValue = 
      value.substring(0, replaceStart) + 
      suggestion + 
      afterCursor;
    
    onChange(newValue);
    
    // Set cursor position after the inserted suggestion
    setTimeout(() => {
      textarea.setSelectionRange(
        replaceStart + suggestion.length,
        replaceStart + suggestion.length
      );
    }, 0);
  };

  const handleSelect = (e: React.SyntheticEvent<HTMLDivElement>) => {
    const target = e.target as HTMLTextAreaElement;
    if (target.selectionStart !== null) {
      setCursorPosition(target.selectionStart);
    }
  };

  useEffect(() => {
    // Get suggestions based on cursor position
    const newSuggestions = SQLValidator.getSuggestions(value, cursorPosition);
    setSuggestions(newSuggestions);
  }, [value, cursorPosition]);

  const renderHighlightedSQL = () => {
    const segments = highlighter.highlight(value);
    return segments.map((segment, index) => (
      <span key={index} style={{ color: segment.color }}>
        {segment.text}
      </span>
    ));
  };

  return (
    <Box>
      <Paper variant="outlined" sx={{ position: 'relative' }}>
        {/* Syntax highlighted overlay */}
        <Box
          sx={{
            position: 'absolute',
            top: 14,
            left: 14,
            right: 14,
            fontFamily: 'monospace',
            fontSize: '0.875rem',
            lineHeight: 1.5,
            pointerEvents: 'none',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            color: 'transparent',
            zIndex: 1,
          }}
        >
          {renderHighlightedSQL()}
        </Box>

        {/* Actual textarea */}
        <TextField
          fullWidth
          multiline
          rows={rows}
          value={value}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onSelect={handleSelect}
          placeholder={placeholder}
          inputRef={textFieldRef}
          disabled={readOnly}
          error={!!error || (validation ? !validation.isValid : false)}
          sx={{
            '& .MuiInputBase-root': {
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              lineHeight: 1.5,
              backgroundColor: 'transparent',
            },
            '& .MuiInputBase-input': {
              color: 'transparent',
              caretColor: theme.palette.text.primary,
              position: 'relative',
              zIndex: 2,
              '&::selection': {
                backgroundColor: theme.palette.action.selected,
                color: 'transparent',
              },
            },
          }}
        />
      </Paper>

      {/* Validation messages */}
      {validation && (
        <Stack spacing={1} sx={{ mt: 1 }}>
          {validation.errors.map((err, idx) => (
            <Chip
              key={`error-${idx}`}
              label={err}
              color="error"
              size="small"
              variant="outlined"
            />
          ))}
          {validation.warnings.map((warn, idx) => (
            <Chip
              key={`warning-${idx}`}
              label={warn}
              color="warning"
              size="small"
              variant="outlined"
            />
          ))}
          {validation.suggestions.map((sug, idx) => (
            <Chip
              key={`suggestion-${idx}`}
              label={sug}
              color="info"
              size="small"
              variant="outlined"
            />
          ))}
        </Stack>
      )}

      {/* Error from parent */}
      {error && (
        <Typography color="error" variant="caption" sx={{ mt: 1, display: 'block' }}>
          {error}
        </Typography>
      )}

      {/* Autocomplete suggestions */}
      {suggestions.length > 0 && (
        <Paper
          elevation={3}
          sx={{
            position: 'absolute',
            mt: 1,
            p: 1,
            maxHeight: 200,
            overflow: 'auto',
            zIndex: 1000,
          }}
        >
          <Typography variant="caption" color="text.secondary" gutterBottom>
            Suggestions:
          </Typography>
          <Stack spacing={0.5}>
            {suggestions.map((suggestion, idx) => (
              <Chip
                key={idx}
                label={suggestion}
                size="small"
                onClick={() => insertSuggestion(suggestion)}
                sx={{ cursor: 'pointer' }}
              />
            ))}
          </Stack>
        </Paper>
      )}

      {/* Keyboard shortcut hint */}
      {onExecute && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
          Press Ctrl+Enter to execute
        </Typography>
      )}
    </Box>
  );
};