import React, { createContext, useContext, useState, useEffect } from 'react';
import { dataSourceAPI } from '../services/dataSourceAPI';

export interface DataSource {
  id: number;
  name: string;
  type: string;
  connection_config: Record<string, any>;
  created_at: string;
  updated_at: string;
}

interface DataSourceContextType {
  selectedDataSource: DataSource | null;
  setSelectedDataSource: (dataSource: DataSource | null) => void;
  dataSources: DataSource[];
  setDataSources: (dataSources: DataSource[]) => void;
  loading: boolean;
  fetchDataSources: () => Promise<void>;
  isConnected: boolean;
}

const DataSourceContext = createContext<DataSourceContextType | undefined>(undefined);

export const useDataSource = () => {
  const context = useContext(DataSourceContext);
  if (context === undefined) {
    throw new Error('useDataSource must be used within a DataSourceProvider');
  }
  return context;
};

interface DataSourceProviderProps {
  children: React.ReactNode;
}

export const DataSourceProvider: React.FC<DataSourceProviderProps> = ({ children }) => {
  const [selectedDataSource, setSelectedDataSource] = useState<DataSource | null>(null);
  const [dataSources, setDataSources] = useState<DataSource[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchDataSources = async () => {
    setLoading(true);
    try {
      const response = await dataSourceAPI.getDataSources();
      setDataSources(response.data);
      
      // If no data source is selected but we have data sources, auto-select the first one
      if (!selectedDataSource && response.data.length > 0) {
        setSelectedDataSource(response.data[0]);
      }
      
      // If selected data source no longer exists, clear selection
      if (selectedDataSource && !response.data.find(ds => ds.id === selectedDataSource.id)) {
        setSelectedDataSource(null);
      }
    } catch (error) {
      console.error('Failed to fetch data sources:', error);
      setDataSources([]);
    } finally {
      setLoading(false);
    }
  };

  // Load data sources on mount and when window gains focus
  useEffect(() => {
    fetchDataSources();
    
    // Refresh data sources when window regains focus (in case they were added in another tab)
    const handleFocus = () => {
      fetchDataSources();
    };
    
    window.addEventListener('focus', handleFocus);
    return () => window.removeEventListener('focus', handleFocus);
  }, []);

  // Save selected data source to localStorage
  useEffect(() => {
    if (selectedDataSource) {
      localStorage.setItem('selectedDataSourceId', selectedDataSource.id.toString());
    } else {
      localStorage.removeItem('selectedDataSourceId');
    }
  }, [selectedDataSource]);

  // Load selected data source from localStorage
  useEffect(() => {
    const savedDataSourceId = localStorage.getItem('selectedDataSourceId');
    if (savedDataSourceId && dataSources.length > 0) {
      const savedDataSource = dataSources.find(ds => ds.id === parseInt(savedDataSourceId));
      if (savedDataSource) {
        setSelectedDataSource(savedDataSource);
      }
    }
  }, [dataSources]);

  const isConnected = selectedDataSource !== null;

  const value: DataSourceContextType = {
    selectedDataSource,
    setSelectedDataSource,
    dataSources,
    setDataSources,
    loading,
    fetchDataSources,
    isConnected
  };

  return (
    <DataSourceContext.Provider value={value}>
      {children}
    </DataSourceContext.Provider>
  );
};