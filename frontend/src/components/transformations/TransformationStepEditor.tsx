import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Box,
  Grid,
  Typography,
  Chip,
  IconButton,
  Switch,
  FormControlLabel,
  Alert,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import CodeEditor from '@uiw/react-textarea-code-editor';
import {
  TransformationStep,
  TransformationType,
  FilterOperator,
  AggregateFunction,
  JoinType,
  FilterRule,
  CleaningRule,
} from '../../types/transformation.types';
import { dataSourceService } from '../../services/api';
import { tableService } from '../../services/api';

interface Props {
  open: boolean;
  step: TransformationStep;
  onSave: (step: TransformationStep) => void;
  onCancel: () => void;
  sourceConfig?: any;
}

const TransformationStepEditor: React.FC<Props> = ({
  open,
  step,
  onSave,
  onCancel,
  sourceConfig,
}) => {
  const [name, setName] = useState(step.name);
  const [description, setDescription] = useState(step.description || '');
  const [config, setConfig] = useState<any>(step.config);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [columns, setColumns] = useState<string[]>([]);

  useEffect(() => {
    if (sourceConfig?.datasource_id && sourceConfig?.table_name) {
      loadTableColumns();
    }
  }, [sourceConfig]);

  const loadTableColumns = async () => {
    try {
      const tableInfo = await tableService.getTableInfo(
        sourceConfig.datasource_id,
        sourceConfig.table_name
      );
      setColumns(tableInfo.columns.map((col: any) => col.name));
    } catch (error) {
      console.error('Failed to load columns:', error);
    }
  };

  const handleSave = () => {
    const validationErrors: Record<string, string> = {};

    if (!name.trim()) {
      validationErrors.name = 'Step name is required';
    }

    // Validate based on transformation type
    switch (step.type) {
      case TransformationType.FILTER:
        if (!config.rules || config.rules.length === 0) {
          validationErrors.config = 'At least one filter rule is required';
        }
        break;
      case TransformationType.AGGREGATE:
        if (!config.aggregations || config.aggregations.length === 0) {
          validationErrors.config = 'At least one aggregation is required';
        }
        break;
      case TransformationType.JOIN:
        if (!config.right_source?.datasource_id) {
          validationErrors.config = 'Right data source is required';
        }
        if (!config.join_conditions || config.join_conditions.length === 0) {
          validationErrors.config = 'At least one join condition is required';
        }
        break;
      case TransformationType.SPLIT_COLUMN:
        if (!config.column) {
          validationErrors.config = 'Column to split is required';
        }
        if (!config.new_columns || config.new_columns.length === 0) {
          validationErrors.config = 'New column names are required';
        }
        break;
      case TransformationType.MERGE_COLUMN:
        if (!config.columns || config.columns.length < 2) {
          validationErrors.config = 'At least two columns to merge are required';
        }
        if (!config.new_column) {
          validationErrors.config = 'New column name is required';
        }
        break;
      case TransformationType.CUSTOM_SQL:
      case TransformationType.CUSTOM_PYTHON:
        if (!config.script) {
          validationErrors.config = 'Script is required';
        }
        break;
    }

    if (Object.keys(validationErrors).length > 0) {
      setErrors(validationErrors);
      return;
    }

    onSave({
      ...step,
      name,
      description,
      config,
    });
  };

  const renderConfigEditor = () => {
    switch (step.type) {
      case TransformationType.FILTER:
        return <FilterConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.CLEAN:
        return <CleaningConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.AGGREGATE:
        return <AggregateConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.JOIN:
        return (
          <JoinConfigEditor
            config={config}
            onChange={setConfig}
            columns={columns}
            sourceConfig={sourceConfig}
          />
        );
      case TransformationType.SPLIT_COLUMN:
        return <SplitColumnConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.MERGE_COLUMN:
        return <MergeColumnConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.TYPE_CONVERSION:
        return <TypeConversionConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.RENAME:
        return <RenameConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.DROP:
        return <DropColumnsConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.FILL_NULL:
        return <FillNullConfigEditor config={config} onChange={setConfig} columns={columns} />;
      case TransformationType.CUSTOM_SQL:
        return <CustomSQLConfigEditor config={config} onChange={setConfig} />;
      case TransformationType.CUSTOM_PYTHON:
        return <CustomPythonConfigEditor config={config} onChange={setConfig} />;
      default:
        return <Alert severity="error">Unsupported transformation type</Alert>;
    }
  };

  return (
    <Dialog open={open} onClose={onCancel} maxWidth="md" fullWidth>
      <DialogTitle>Edit Transformation Step</DialogTitle>
      <DialogContent dividers>
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Step Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              error={!!errors.name}
              helperText={errors.name}
              required
            />
          </Grid>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              multiline
              rows={2}
            />
          </Grid>
          <Grid item xs={12}>
            <Typography variant="subtitle1" gutterBottom>
              Configuration
            </Typography>
            {errors.config && (
              <Alert severity="error" sx={{ mb: 2 }}>
                {errors.config}
              </Alert>
            )}
            {renderConfigEditor()}
          </Grid>
        </Grid>
      </DialogContent>
      <DialogActions>
        <Button onClick={onCancel}>Cancel</Button>
        <Button onClick={handleSave} variant="contained">
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

// Configuration editors for each transformation type

const FilterConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [rules, setRules] = useState<FilterRule[]>(config.rules || []);
  const [logicalOperator, setLogicalOperator] = useState(config.logical_operator || 'AND');

  const addRule = () => {
    setRules([
      ...rules,
      {
        column: columns[0] || '',
        operator: FilterOperator.EQUALS,
        value: '',
      },
    ]);
  };

  const updateRule = (index: number, rule: FilterRule) => {
    const newRules = [...rules];
    newRules[index] = rule;
    setRules(newRules);
    onChange({ rules: newRules, logical_operator: logicalOperator });
  };

  const deleteRule = (index: number) => {
    const newRules = rules.filter((_, i) => i !== index);
    setRules(newRules);
    onChange({ rules: newRules, logical_operator: logicalOperator });
  };

  return (
    <Box>
      <FormControl fullWidth sx={{ mb: 2 }}>
        <InputLabel>Logical Operator</InputLabel>
        <Select
          value={logicalOperator}
          onChange={(e) => {
            setLogicalOperator(e.target.value);
            onChange({ rules, logical_operator: e.target.value });
          }}
          label="Logical Operator"
        >
          <MenuItem value="AND">AND</MenuItem>
          <MenuItem value="OR">OR</MenuItem>
        </Select>
      </FormControl>

      {rules.map((rule, index) => (
        <Box key={index} sx={{ mb: 2, p: 2, border: 1, borderColor: 'divider', borderRadius: 1 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Column</InputLabel>
                <Select
                  value={rule.column}
                  onChange={(e) => updateRule(index, { ...rule, column: e.target.value })}
                  label="Column"
                >
                  {columns.map((col) => (
                    <MenuItem key={col} value={col}>
                      {col}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Operator</InputLabel>
                <Select
                  value={rule.operator}
                  onChange={(e) =>
                    updateRule(index, { ...rule, operator: e.target.value as FilterOperator })
                  }
                  label="Operator"
                >
                  {Object.values(FilterOperator).map((op) => (
                    <MenuItem key={op} value={op}>
                      {op}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={4}>
              <TextField
                fullWidth
                size="small"
                label="Value"
                value={rule.value || ''}
                onChange={(e) => updateRule(index, { ...rule, value: e.target.value })}
                disabled={rule.operator === FilterOperator.IS_NULL || rule.operator === FilterOperator.NOT_NULL}
              />
            </Grid>
            <Grid item xs={2}>
              <IconButton onClick={() => deleteRule(index)} color="error">
                <DeleteIcon />
              </IconButton>
            </Grid>
          </Grid>
        </Box>
      ))}

      <Button startIcon={<AddIcon />} onClick={addRule} variant="outlined">
        Add Filter Rule
      </Button>
    </Box>
  );
};

const CleaningConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [rules, setRules] = useState<CleaningRule[]>(config.rules || []);

  const cleaningRuleTypes = [
    { value: 'trim', label: 'Trim Whitespace' },
    { value: 'remove_special', label: 'Remove Special Characters' },
    { value: 'lowercase', label: 'Convert to Lowercase' },
    { value: 'uppercase', label: 'Convert to Uppercase' },
    { value: 'remove_numbers', label: 'Remove Numbers' },
    { value: 'remove_spaces', label: 'Remove All Spaces' },
    { value: 'normalize_whitespace', label: 'Normalize Whitespace' },
    { value: 'remove_punctuation', label: 'Remove Punctuation' },
    { value: 'remove_html', label: 'Remove HTML Tags' },
    { value: 'remove_urls', label: 'Remove URLs' },
    { value: 'custom_regex', label: 'Custom Regex' },
  ];

  const addRule = () => {
    setRules([
      ...rules,
      {
        column: columns[0] || '',
        rule_type: 'trim',
      },
    ]);
  };

  const updateRule = (index: number, rule: CleaningRule) => {
    const newRules = [...rules];
    newRules[index] = rule;
    setRules(newRules);
    onChange({ rules: newRules });
  };

  const deleteRule = (index: number) => {
    const newRules = rules.filter((_, i) => i !== index);
    setRules(newRules);
    onChange({ rules: newRules });
  };

  return (
    <Box>
      {rules.map((rule, index) => (
        <Box key={index} sx={{ mb: 2, p: 2, border: 1, borderColor: 'divider', borderRadius: 1 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={4}>
              <FormControl fullWidth size="small">
                <InputLabel>Column</InputLabel>
                <Select
                  value={rule.column}
                  onChange={(e) => updateRule(index, { ...rule, column: e.target.value })}
                  label="Column"
                >
                  {columns.map((col) => (
                    <MenuItem key={col} value={col}>
                      {col}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={4}>
              <FormControl fullWidth size="small">
                <InputLabel>Rule Type</InputLabel>
                <Select
                  value={rule.rule_type}
                  onChange={(e) => updateRule(index, { ...rule, rule_type: e.target.value })}
                  label="Rule Type"
                >
                  {cleaningRuleTypes.map((type) => (
                    <MenuItem key={type.value} value={type.value}>
                      {type.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            {rule.rule_type === 'custom_regex' && (
              <Grid item xs={3}>
                <TextField
                  fullWidth
                  size="small"
                  label="Pattern"
                  value={rule.parameters?.pattern || ''}
                  onChange={(e) =>
                    updateRule(index, {
                      ...rule,
                      parameters: { ...rule.parameters, pattern: e.target.value },
                    })
                  }
                />
              </Grid>
            )}
            <Grid item xs={1}>
              <IconButton onClick={() => deleteRule(index)} color="error">
                <DeleteIcon />
              </IconButton>
            </Grid>
          </Grid>
        </Box>
      ))}

      <Button startIcon={<AddIcon />} onClick={addRule} variant="outlined">
        Add Cleaning Rule
      </Button>
    </Box>
  );
};

const AggregateConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [groupBy, setGroupBy] = useState<string[]>(config.group_by || []);
  const [aggregations, setAggregations] = useState(config.aggregations || []);

  const addAggregation = () => {
    setAggregations([
      ...aggregations,
      {
        column: columns[0] || '',
        function: AggregateFunction.SUM,
        alias: '',
      },
    ]);
  };

  const updateAggregation = (index: number, agg: any) => {
    const newAggs = [...aggregations];
    newAggs[index] = agg;
    setAggregations(newAggs);
    onChange({ group_by: groupBy, aggregations: newAggs });
  };

  const deleteAggregation = (index: number) => {
    const newAggs = aggregations.filter((_: any, i: number) => i !== index);
    setAggregations(newAggs);
    onChange({ group_by: groupBy, aggregations: newAggs });
  };

  return (
    <Box>
      <FormControl fullWidth sx={{ mb: 3 }}>
        <InputLabel>Group By Columns</InputLabel>
        <Select
          multiple
          value={groupBy}
          onChange={(e) => {
            setGroupBy(e.target.value as string[]);
            onChange({ group_by: e.target.value, aggregations });
          }}
          label="Group By Columns"
          renderValue={(selected) => (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
              {(selected as string[]).map((value) => (
                <Chip key={value} label={value} size="small" />
              ))}
            </Box>
          )}
        >
          {columns.map((col) => (
            <MenuItem key={col} value={col}>
              {col}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      <Typography variant="subtitle2" gutterBottom>
        Aggregations
      </Typography>
      {aggregations.map((agg: any, index: number) => (
        <Box key={index} sx={{ mb: 2, p: 2, border: 1, borderColor: 'divider', borderRadius: 1 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Column</InputLabel>
                <Select
                  value={agg.column}
                  onChange={(e) => updateAggregation(index, { ...agg, column: e.target.value })}
                  label="Column"
                >
                  {columns.map((col) => (
                    <MenuItem key={col} value={col}>
                      {col}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={3}>
              <FormControl fullWidth size="small">
                <InputLabel>Function</InputLabel>
                <Select
                  value={agg.function}
                  onChange={(e) => updateAggregation(index, { ...agg, function: e.target.value })}
                  label="Function"
                >
                  {Object.values(AggregateFunction).map((func) => (
                    <MenuItem key={func} value={func}>
                      {func.toUpperCase()}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={4}>
              <TextField
                fullWidth
                size="small"
                label="Alias"
                value={agg.alias || ''}
                onChange={(e) => updateAggregation(index, { ...agg, alias: e.target.value })}
                placeholder={`${agg.column}_${agg.function}`}
              />
            </Grid>
            <Grid item xs={2}>
              <IconButton onClick={() => deleteAggregation(index)} color="error">
                <DeleteIcon />
              </IconButton>
            </Grid>
          </Grid>
        </Box>
      ))}

      <Button startIcon={<AddIcon />} onClick={addAggregation} variant="outlined">
        Add Aggregation
      </Button>
    </Box>
  );
};

const JoinConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
  sourceConfig?: any;
}> = ({ config, onChange, columns, sourceConfig }) => {
  const [dataSources, setDataSources] = useState<any[]>([]);
  const [rightTables, setRightTables] = useState<string[]>([]);
  const [rightColumns, setRightColumns] = useState<string[]>([]);
  const [rightDataSourceId, setRightDataSourceId] = useState(
    config.right_source?.datasource_id || ''
  );
  const [rightTable, setRightTable] = useState(config.right_source?.table_name || '');
  const [joinType, setJoinType] = useState(config.join_type || JoinType.INNER);
  const [joinConditions, setJoinConditions] = useState(config.join_conditions || []);

  useEffect(() => {
    loadDataSources();
  }, []);

  useEffect(() => {
    if (rightDataSourceId) {
      loadTables(rightDataSourceId);
    }
  }, [rightDataSourceId]);

  useEffect(() => {
    if (rightDataSourceId && rightTable) {
      loadRightColumns();
    }
  }, [rightTable]);

  const loadDataSources = async () => {
    try {
      const response = await dataSourceService.listDataSources();
      setDataSources(response);
    } catch (error) {
      console.error('Failed to load data sources:', error);
    }
  };

  const loadTables = async (dataSourceId: number) => {
    try {
      const response = await tableService.getTables(dataSourceId);
      setRightTables(response.tables.map((t: any) => t.name));
    } catch (error) {
      console.error('Failed to load tables:', error);
    }
  };

  const loadRightColumns = async () => {
    try {
      const tableInfo = await tableService.getTableInfo(rightDataSourceId, rightTable);
      setRightColumns(tableInfo.columns.map((col: any) => col.name));
    } catch (error) {
      console.error('Failed to load columns:', error);
    }
  };

  const addJoinCondition = () => {
    const newConditions = [
      ...joinConditions,
      {
        left: columns[0] || '',
        right: rightColumns[0] || '',
      },
    ];
    setJoinConditions(newConditions);
    updateConfig(newConditions);
  };

  const updateJoinCondition = (index: number, condition: any) => {
    const newConditions = [...joinConditions];
    newConditions[index] = condition;
    setJoinConditions(newConditions);
    updateConfig(newConditions);
  };

  const deleteJoinCondition = (index: number) => {
    const newConditions = joinConditions.filter((_: any, i: number) => i !== index);
    setJoinConditions(newConditions);
    updateConfig(newConditions);
  };

  const updateConfig = (conditions?: any[]) => {
    onChange({
      left_source: {
        datasource_id: sourceConfig?.datasource_id,
        table_name: sourceConfig?.table_name,
      },
      right_source: {
        datasource_id: rightDataSourceId,
        table_name: rightTable,
      },
      join_type: joinType,
      join_conditions: conditions || joinConditions,
    });
  };

  return (
    <Box>
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={6}>
          <FormControl fullWidth>
            <InputLabel>Right Data Source</InputLabel>
            <Select
              value={rightDataSourceId}
              onChange={(e) => {
                setRightDataSourceId(e.target.value);
                setRightTable('');
                updateConfig();
              }}
              label="Right Data Source"
            >
              {dataSources.map((ds) => (
                <MenuItem key={ds.id} value={ds.id}>
                  {ds.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>
        <Grid item xs={6}>
          <FormControl fullWidth>
            <InputLabel>Right Table</InputLabel>
            <Select
              value={rightTable}
              onChange={(e) => {
                setRightTable(e.target.value);
                updateConfig();
              }}
              label="Right Table"
              disabled={!rightDataSourceId}
            >
              {rightTables.map((table) => (
                <MenuItem key={table} value={table}>
                  {table}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>
        <Grid item xs={12}>
          <FormControl fullWidth>
            <InputLabel>Join Type</InputLabel>
            <Select
              value={joinType}
              onChange={(e) => {
                setJoinType(e.target.value);
                updateConfig();
              }}
              label="Join Type"
            >
              {Object.values(JoinType).map((type) => (
                <MenuItem key={type} value={type}>
                  {type.toUpperCase()} JOIN
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>
      </Grid>

      <Typography variant="subtitle2" gutterBottom>
        Join Conditions
      </Typography>
      {joinConditions.map((condition: any, index: number) => (
        <Box key={index} sx={{ mb: 2, p: 2, border: 1, borderColor: 'divider', borderRadius: 1 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={5}>
              <FormControl fullWidth size="small">
                <InputLabel>Left Column</InputLabel>
                <Select
                  value={condition.left}
                  onChange={(e) =>
                    updateJoinCondition(index, { ...condition, left: e.target.value })
                  }
                  label="Left Column"
                >
                  {columns.map((col) => (
                    <MenuItem key={col} value={col}>
                      {col}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={5}>
              <FormControl fullWidth size="small">
                <InputLabel>Right Column</InputLabel>
                <Select
                  value={condition.right}
                  onChange={(e) =>
                    updateJoinCondition(index, { ...condition, right: e.target.value })
                  }
                  label="Right Column"
                  disabled={rightColumns.length === 0}
                >
                  {rightColumns.map((col) => (
                    <MenuItem key={col} value={col}>
                      {col}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={2}>
              <IconButton onClick={() => deleteJoinCondition(index)} color="error">
                <DeleteIcon />
              </IconButton>
            </Grid>
          </Grid>
        </Box>
      ))}

      <Button
        startIcon={<AddIcon />}
        onClick={addJoinCondition}
        variant="outlined"
        disabled={rightColumns.length === 0}
      >
        Add Join Condition
      </Button>
    </Box>
  );
};

const SplitColumnConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [column, setColumn] = useState(config.column || '');
  const [delimiter, setDelimiter] = useState(config.delimiter || ',');
  const [pattern, setPattern] = useState(config.pattern || '');
  const [newColumns, setNewColumns] = useState<string[]>(config.new_columns || ['', '']);
  const [keepOriginal, setKeepOriginal] = useState(config.keep_original || false);
  const [useRegex, setUseRegex] = useState(!!config.pattern);

  const updateConfig = () => {
    onChange({
      column,
      ...(useRegex ? { pattern } : { delimiter }),
      new_columns: newColumns.filter((c) => c.trim()),
      keep_original: keepOriginal,
    });
  };

  const addNewColumn = () => {
    setNewColumns([...newColumns, '']);
  };

  const updateNewColumn = (index: number, value: string) => {
    const updated = [...newColumns];
    updated[index] = value;
    setNewColumns(updated);
  };

  const removeNewColumn = (index: number) => {
    const updated = newColumns.filter((_, i) => i !== index);
    setNewColumns(updated);
  };

  useEffect(() => {
    updateConfig();
  }, [column, delimiter, pattern, newColumns, keepOriginal, useRegex]);

  return (
    <Box>
      <FormControl fullWidth sx={{ mb: 2 }}>
        <InputLabel>Column to Split</InputLabel>
        <Select value={column} onChange={(e) => setColumn(e.target.value)} label="Column to Split">
          {columns.map((col) => (
            <MenuItem key={col} value={col}>
              {col}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      <FormControlLabel
        control={<Switch checked={useRegex} onChange={(e) => setUseRegex(e.target.checked)} />}
        label="Use Regular Expression"
        sx={{ mb: 2 }}
      />

      {useRegex ? (
        <TextField
          fullWidth
          label="Regular Expression Pattern"
          value={pattern}
          onChange={(e) => setPattern(e.target.value)}
          helperText="Use capture groups to define split points"
          sx={{ mb: 2 }}
        />
      ) : (
        <TextField
          fullWidth
          label="Delimiter"
          value={delimiter}
          onChange={(e) => setDelimiter(e.target.value)}
          sx={{ mb: 2 }}
        />
      )}

      <Typography variant="subtitle2" gutterBottom>
        New Column Names
      </Typography>
      {newColumns.map((colName, index) => (
        <Box key={index} sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
          <TextField
            fullWidth
            size="small"
            label={`Column ${index + 1}`}
            value={colName}
            onChange={(e) => updateNewColumn(index, e.target.value)}
          />
          <IconButton
            onClick={() => removeNewColumn(index)}
            disabled={newColumns.length <= 2}
            color="error"
          >
            <DeleteIcon />
          </IconButton>
        </Box>
      ))}

      <Button startIcon={<AddIcon />} onClick={addNewColumn} variant="outlined" sx={{ mb: 2 }}>
        Add Column
      </Button>

      <FormControlLabel
        control={
          <Switch checked={keepOriginal} onChange={(e) => setKeepOriginal(e.target.checked)} />
        }
        label="Keep Original Column"
      />
    </Box>
  );
};

const MergeColumnConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [selectedColumns, setSelectedColumns] = useState<string[]>(config.columns || []);
  const [separator, setSeparator] = useState(config.separator || ' ');
  const [newColumn, setNewColumn] = useState(config.new_column || '');
  const [dropOriginal, setDropOriginal] = useState(config.drop_original !== false);

  useEffect(() => {
    onChange({
      columns: selectedColumns,
      separator,
      new_column: newColumn,
      drop_original: dropOriginal,
    });
  }, [selectedColumns, separator, newColumn, dropOriginal]);

  return (
    <Box>
      <FormControl fullWidth sx={{ mb: 2 }}>
        <InputLabel>Columns to Merge</InputLabel>
        <Select
          multiple
          value={selectedColumns}
          onChange={(e) => setSelectedColumns(e.target.value as string[])}
          label="Columns to Merge"
          renderValue={(selected) => (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
              {(selected as string[]).map((value) => (
                <Chip key={value} label={value} size="small" />
              ))}
            </Box>
          )}
        >
          {columns.map((col) => (
            <MenuItem key={col} value={col}>
              {col}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      <TextField
        fullWidth
        label="Separator"
        value={separator}
        onChange={(e) => setSeparator(e.target.value)}
        sx={{ mb: 2 }}
      />

      <TextField
        fullWidth
        label="New Column Name"
        value={newColumn}
        onChange={(e) => setNewColumn(e.target.value)}
        sx={{ mb: 2 }}
        required
      />

      <FormControlLabel
        control={<Switch checked={dropOriginal} onChange={(e) => setDropOriginal(e.target.checked)} />}
        label="Drop Original Columns"
      />
    </Box>
  );
};

const TypeConversionConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [column, setColumn] = useState(config.column || '');
  const [targetType, setTargetType] = useState(config.target_type || 'string');
  const [format, setFormat] = useState(config.format || '');
  const [defaultValue, setDefaultValue] = useState(config.default_value || '');

  const typeOptions = [
    { value: 'integer', label: 'Integer' },
    { value: 'float', label: 'Float' },
    { value: 'string', label: 'String' },
    { value: 'date', label: 'Date' },
    { value: 'datetime', label: 'DateTime' },
    { value: 'boolean', label: 'Boolean' },
    { value: 'json', label: 'JSON' },
  ];

  const dateFormats = [
    '%Y-%m-%d',
    '%d/%m/%Y',
    '%m/%d/%Y',
    '%Y/%m/%d',
    '%Y-%m-%d %H:%M:%S',
    '%d/%m/%Y %H:%M:%S',
  ];

  useEffect(() => {
    onChange({
      column,
      target_type: targetType,
      ...(format && { format }),
      ...(defaultValue && { default_value: defaultValue }),
    });
  }, [column, targetType, format, defaultValue]);

  return (
    <Box>
      <FormControl fullWidth sx={{ mb: 2 }}>
        <InputLabel>Column</InputLabel>
        <Select value={column} onChange={(e) => setColumn(e.target.value)} label="Column">
          {columns.map((col) => (
            <MenuItem key={col} value={col}>
              {col}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      <FormControl fullWidth sx={{ mb: 2 }}>
        <InputLabel>Target Type</InputLabel>
        <Select
          value={targetType}
          onChange={(e) => setTargetType(e.target.value)}
          label="Target Type"
        >
          {typeOptions.map((type) => (
            <MenuItem key={type.value} value={type.value}>
              {type.label}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {(targetType === 'date' || targetType === 'datetime') && (
        <FormControl fullWidth sx={{ mb: 2 }}>
          <InputLabel>Date Format</InputLabel>
          <Select value={format} onChange={(e) => setFormat(e.target.value)} label="Date Format">
            {dateFormats.map((fmt) => (
              <MenuItem key={fmt} value={fmt}>
                {fmt}
              </MenuItem>
            ))}
            <MenuItem value="">Custom</MenuItem>
          </Select>
        </FormControl>
      )}

      <TextField
        fullWidth
        label="Default Value (for failed conversions)"
        value={defaultValue}
        onChange={(e) => setDefaultValue(e.target.value)}
        helperText="Value to use when conversion fails"
      />
    </Box>
  );
};

const RenameConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [renameMap, setRenameMap] = useState<Record<string, string>>(config.rename_map || {});

  const updateRename = (oldName: string, newName: string) => {
    const updated = { ...renameMap };
    if (newName) {
      updated[oldName] = newName;
    } else {
      delete updated[oldName];
    }
    setRenameMap(updated);
    onChange({ rename_map: updated });
  };

  return (
    <Box>
      {columns.map((col) => (
        <Box key={col} sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <TextField
            label="Original Name"
            value={col}
            disabled
            sx={{ mr: 2, flex: 1 }}
            size="small"
          />
          <TextField
            label="New Name"
            value={renameMap[col] || ''}
            onChange={(e) => updateRename(col, e.target.value)}
            sx={{ flex: 1 }}
            size="small"
            placeholder={col}
          />
        </Box>
      ))}
    </Box>
  );
};

const DropColumnsConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [selectedColumns, setSelectedColumns] = useState<string[]>(config.columns || []);

  return (
    <FormControl fullWidth>
      <InputLabel>Columns to Drop</InputLabel>
      <Select
        multiple
        value={selectedColumns}
        onChange={(e) => {
          setSelectedColumns(e.target.value as string[]);
          onChange({ columns: e.target.value });
        }}
        label="Columns to Drop"
        renderValue={(selected) => (
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
            {(selected as string[]).map((value) => (
              <Chip key={value} label={value} size="small" color="error" />
            ))}
          </Box>
        )}
      >
        {columns.map((col) => (
          <MenuItem key={col} value={col}>
            {col}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

const FillNullConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
  columns: string[];
}> = ({ config, onChange, columns }) => {
  const [column, setColumn] = useState(config.column || '');
  const [method, setMethod] = useState(config.method || 'value');
  const [value, setValue] = useState(config.value || '');
  const [applyToAll, setApplyToAll] = useState(!config.column);

  const methods = [
    { value: 'value', label: 'Specific Value' },
    { value: 'forward', label: 'Forward Fill' },
    { value: 'backward', label: 'Backward Fill' },
    { value: 'mean', label: 'Mean (numeric only)' },
    { value: 'median', label: 'Median (numeric only)' },
    { value: 'mode', label: 'Mode' },
  ];

  useEffect(() => {
    const configData: any = { method };
    if (!applyToAll && column) {
      configData.column = column;
    }
    if (method === 'value') {
      configData.value = value;
    }
    onChange(configData);
  }, [column, method, value, applyToAll]);

  return (
    <Box>
      <FormControlLabel
        control={<Switch checked={applyToAll} onChange={(e) => setApplyToAll(e.target.checked)} />}
        label="Apply to All Columns"
        sx={{ mb: 2 }}
      />

      {!applyToAll && (
        <FormControl fullWidth sx={{ mb: 2 }}>
          <InputLabel>Column</InputLabel>
          <Select value={column} onChange={(e) => setColumn(e.target.value)} label="Column">
            {columns.map((col) => (
              <MenuItem key={col} value={col}>
                {col}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      )}

      <FormControl fullWidth sx={{ mb: 2 }}>
        <InputLabel>Method</InputLabel>
        <Select value={method} onChange={(e) => setMethod(e.target.value)} label="Method">
          {methods.map((m) => (
            <MenuItem key={m.value} value={m.value}>
              {m.label}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      {method === 'value' && (
        <TextField
          fullWidth
          label="Fill Value"
          value={value}
          onChange={(e) => setValue(e.target.value)}
        />
      )}
    </Box>
  );
};

const CustomSQLConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
}> = ({ config, onChange }) => {
  const [script, setScript] = useState(config.script || '');

  return (
    <Box>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Write a SQL query to transform the data. The source data is available as a table named
        'source_data'.
      </Typography>
      <CodeEditor
        value={script}
        language="sql"
        placeholder="SELECT * FROM source_data WHERE ..."
        onChange={(evn) => {
          setScript(evn.target.value);
          onChange({ script: evn.target.value });
        }}
        padding={15}
        style={{
          fontSize: 12,
          backgroundColor: '#f5f5f5',
          fontFamily:
            'ui-monospace,SFMono-Regular,SF Mono,Consolas,Liberation Mono,Menlo,monospace',
        }}
        minHeight={200}
      />
    </Box>
  );
};

const CustomPythonConfigEditor: React.FC<{
  config: any;
  onChange: (config: any) => void;
}> = ({ config, onChange }) => {
  const [script, setScript] = useState(
    config.script ||
      `# Available variables:
# df - input DataFrame
# pd - pandas module
# np - numpy module
# datetime - datetime module
# re - regex module
# json - json module

# Transform the DataFrame
result = df.copy()

# Your transformation code here
# Example: result['new_column'] = result['existing_column'] * 2

# The script must set 'result' to the transformed DataFrame
`
  );

  return (
    <Box>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Write Python code to transform the data. The input DataFrame is available as 'df'. Set
        'result' to the transformed DataFrame.
      </Typography>
      <CodeEditor
        value={script}
        language="python"
        placeholder="# Transform the DataFrame..."
        onChange={(evn) => {
          setScript(evn.target.value);
          onChange({ script: evn.target.value });
        }}
        padding={15}
        style={{
          fontSize: 12,
          backgroundColor: '#f5f5f5',
          fontFamily:
            'ui-monospace,SFMono-Regular,SF Mono,Consolas,Liberation Mono,Menlo,monospace',
        }}
        minHeight={300}
      />
    </Box>
  );
};

export default TransformationStepEditor;