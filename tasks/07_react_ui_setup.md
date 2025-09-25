# Задача 7: React UI Setup

## Цель
Создать современный React frontend с поддержкой загрузки файлов, выбора destinations, конфигурации параметров и интеграции с WebSocket для real-time обновлений.

## Технические требования
- Modern React 18+ с TypeScript
- Material-UI или аналогичная UI библиотека
- State management (Zustand или Redux Toolkit)
- File upload с поддержкой больших файлов
- Форма конфигурации destinations
- Интеграция с WebSocket для real-time обновлений

## Структура Frontend проекта

```
services/frontend/
├── Dockerfile
├── package.json
├── tsconfig.json
├── public/
│   ├── index.html
│   └── favicon.ico
├── src/
│   ├── index.tsx
│   ├── App.tsx
│   ├── components/
│   │   ├── common/
│   │   │   ├── Layout.tsx
│   │   │   ├── Header.tsx
│   │   │   ├── LoadingSpinner.tsx
│   │   │   └── ErrorBoundary.tsx
│   │   ├── upload/
│   │   │   ├── FileUpload.tsx
│   │   │   ├── DestinationSelector.tsx
│   │   │   ├── ConfigurationForm.tsx
│   │   │   └── UploadProgress.tsx
│   │   ├── jobs/
│   │   │   ├── JobsList.tsx
│   │   │   ├── JobDetails.tsx
│   │   │   ├── JobStatus.tsx
│   │   │   └── ScriptViewer.tsx
│   │   └── visualization/
│   │       ├── ProcessFlow.tsx (будет в задаче 9)
│   │       └── MetricsPanel.tsx
│   ├── hooks/
│   │   ├── useWebSocket.ts (из задачи 6)
│   │   ├── useFileUpload.ts
│   │   ├── useJobs.ts
│   │   └── useDestinations.ts
│   ├── store/
│   │   ├── index.ts
│   │   ├── jobsStore.ts
│   │   ├── uploadsStore.ts
│   │   └── destinationsStore.ts
│   ├── services/
│   │   ├── api.ts
│   │   ├── websocket.ts
│   │   └── types.ts
│   ├── utils/
│   │   ├── formatters.ts
│   │   ├── validators.ts
│   │   └── constants.ts
│   └── styles/
│       ├── index.css
│       └── themes.ts
```

## Package.json

```json
{
  "name": "bigdata-frontend",
  "version": "1.0.0",
  "private": true,
  "dependencies": {
    "@types/node": "^20.8.0",
    "@types/react": "^18.2.25",
    "@types/react-dom": "^18.2.10",
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "react-scripts": "5.0.1",
    "typescript": "^5.2.0",
    "@mui/material": "^5.14.15",
    "@mui/icons-material": "^5.14.15",
    "@emotion/react": "^11.11.1",
    "@emotion/styled": "^11.11.0",
    "zustand": "^4.4.4",
    "axios": "^1.5.0",
    "react-dropzone": "^14.2.3",
    "react-query": "^3.39.3",
    "@tanstack/react-query": "^5.0.0",
    "react-hook-form": "^7.47.0",
    "@hookform/resolvers": "^3.3.1",
    "yup": "^1.3.3",
    "react-hot-toast": "^2.4.1",
    "prism-react-renderer": "^2.1.0",
    "file-saver": "^2.0.5",
    "@types/file-saver": "^2.0.5"
  },
  "scripts": {
    "start": "react-scripts start",
    "build": "react-scripts build",
    "test": "react-scripts test",
    "eject": "react-scripts eject"
  },
  "eslintConfig": {
    "extends": [
      "react-app",
      "react-app/jest"
    ]
  },
  "browserslist": {
    "production": [
      ">0.2%",
      "not dead",
      "not op_mini all"
    ],
    "development": [
      "last 1 chrome version",
      "last 1 firefox version",
      "last 1 safari version"
    ]
  },
  "proxy": "http://backend:8000"
}
```

## Dockerfile

```dockerfile
# Multi-stage build для оптимизации
FROM node:18-alpine as builder

WORKDIR /app

# Копирование package files
COPY package.json package-lock.json ./

# Установка зависимостей
RUN npm ci --only=production

# Копирование исходного кода
COPY . .

# Build приложения
RUN npm run build

# Production stage
FROM nginx:alpine

# Копирование собранного приложения
COPY --from=builder /app/build /usr/share/nginx/html

# Копирование конфигурации nginx
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
```

## Main App Component

### App.tsx
```tsx
import React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { Toaster } from 'react-hot-toast';
import { WebSocketProvider } from './contexts/WebSocketContext';
import Layout from './components/common/Layout';
import ErrorBoundary from './components/common/ErrorBoundary';

const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
    mode: 'light',
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
  },
});

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 3,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
});

const WEBSOCKET_URL = process.env.REACT_APP_WS_URL || 'ws://localhost:8000/ws/connect';

function App() {
  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider theme={theme}>
          <CssBaseline />
          <WebSocketProvider websocketUrl={WEBSOCKET_URL}>
            <Layout />
            <Toaster
              position="top-right"
              toastOptions={{
                duration: 4000,
                style: {
                  background: '#363636',
                  color: '#fff',
                },
              }}
            />
          </WebSocketProvider>
        </ThemeProvider>
      </QueryClientProvider>
    </ErrorBoundary>
  );
}

export default App;
```

## Store Management с Zustand

### store/jobsStore.ts
```tsx
import { create } from 'zustand';
import { devtools } from 'zustand/middleware';

export interface Job {
  id: string;
  job_id: string;
  filename: string;
  file_size: number;
  file_format: string;
  destination_type: string;
  destination_config: Record<string, any>;
  status: 'pending' | 'processing' | 'completed' | 'failed' | 'cancelled';
  created_at: string;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
  records_processed: number;
  records_total?: number;
  ddl_script?: string;
  etl_script?: string;
}

export interface JobProgress {
  job_id: string;
  stage: string;
  records_processed: number;
  records_total?: number;
  percentage?: number;
  timestamp: string;
}

interface JobsState {
  jobs: Job[];
  selectedJob: Job | null;
  jobProgress: Map<string, JobProgress>;
  isLoading: boolean;
  error: string | null;

  // Actions
  setJobs: (jobs: Job[]) => void;
  addJob: (job: Job) => void;
  updateJob: (jobId: string, updates: Partial<Job>) => void;
  setSelectedJob: (job: Job | null) => void;
  updateJobProgress: (jobId: string, progress: JobProgress) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  clearJobs: () => void;
}

export const useJobsStore = create<JobsState>()(
  devtools(
    (set, get) => ({
      jobs: [],
      selectedJob: null,
      jobProgress: new Map(),
      isLoading: false,
      error: null,

      setJobs: (jobs) =>
        set(() => ({ jobs }), false, 'setJobs'),

      addJob: (job) =>
        set((state) => ({ jobs: [job, ...state.jobs] }), false, 'addJob'),

      updateJob: (jobId, updates) =>
        set((state) => ({
          jobs: state.jobs.map(job =>
            job.job_id === jobId ? { ...job, ...updates } : job
          ),
          selectedJob: state.selectedJob?.job_id === jobId
            ? { ...state.selectedJob, ...updates }
            : state.selectedJob
        }), false, 'updateJob'),

      setSelectedJob: (job) =>
        set(() => ({ selectedJob: job }), false, 'setSelectedJob'),

      updateJobProgress: (jobId, progress) =>
        set((state) => {
          const newProgressMap = new Map(state.jobProgress);
          newProgressMap.set(jobId, progress);
          return { jobProgress: newProgressMap };
        }, false, 'updateJobProgress'),

      setLoading: (isLoading) =>
        set(() => ({ isLoading }), false, 'setLoading'),

      setError: (error) =>
        set(() => ({ error }), false, 'setError'),

      clearJobs: () =>
        set(() => ({
          jobs: [],
          selectedJob: null,
          jobProgress: new Map(),
          error: null
        }), false, 'clearJobs'),
    }),
    {
      name: 'jobs-store',
    }
  )
);
```

### store/uploadsStore.ts
```tsx
import { create } from 'zustand';
import { devtools } from 'zustand/middleware';

export interface DestinationConfig {
  type: 'postgresql' | 'clickhouse' | 'hdfs';
  config: Record<string, any>;
  name: string;
}

export interface UploadState {
  selectedFile: File | null;
  destinationConfig: DestinationConfig | null;
  isUploading: boolean;
  uploadProgress: number;
  error: string | null;
}

interface UploadsState {
  currentUpload: UploadState;
  availableDestinations: DestinationConfig[];

  // Actions
  setSelectedFile: (file: File | null) => void;
  setDestinationConfig: (config: DestinationConfig | null) => void;
  setIsUploading: (uploading: boolean) => void;
  setUploadProgress: (progress: number) => void;
  setError: (error: string | null) => void;
  setAvailableDestinations: (destinations: DestinationConfig[]) => void;
  resetUpload: () => void;
}

const initialUploadState: UploadState = {
  selectedFile: null,
  destinationConfig: null,
  isUploading: false,
  uploadProgress: 0,
  error: null,
};

export const useUploadsStore = create<UploadsState>()(
  devtools(
    (set) => ({
      currentUpload: initialUploadState,
      availableDestinations: [],

      setSelectedFile: (selectedFile) =>
        set((state) => ({
          currentUpload: { ...state.currentUpload, selectedFile }
        }), false, 'setSelectedFile'),

      setDestinationConfig: (destinationConfig) =>
        set((state) => ({
          currentUpload: { ...state.currentUpload, destinationConfig }
        }), false, 'setDestinationConfig'),

      setIsUploading: (isUploading) =>
        set((state) => ({
          currentUpload: { ...state.currentUpload, isUploading }
        }), false, 'setIsUploading'),

      setUploadProgress: (uploadProgress) =>
        set((state) => ({
          currentUpload: { ...state.currentUpload, uploadProgress }
        }), false, 'setUploadProgress'),

      setError: (error) =>
        set((state) => ({
          currentUpload: { ...state.currentUpload, error }
        }), false, 'setError'),

      setAvailableDestinations: (availableDestinations) =>
        set(() => ({ availableDestinations }), false, 'setAvailableDestinations'),

      resetUpload: () =>
        set(() => ({ currentUpload: initialUploadState }), false, 'resetUpload'),
    }),
    {
      name: 'uploads-store',
    }
  )
);
```

## File Upload Component

### components/upload/FileUpload.tsx
```tsx
import React, { useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import {
  Box,
  Typography,
  Paper,
  LinearProgress,
  Chip,
  Alert
} from '@mui/material';
import {
  CloudUpload,
  InsertDriveFile,
  Error as ErrorIcon
} from '@mui/icons-material';
import { useUploadsStore } from '../../store/uploadsStore';
import { formatFileSize } from '../../utils/formatters';

const ACCEPTED_FILE_TYPES = {
  'text/csv': ['.csv'],
  'application/json': ['.json'],
  'text/xml': ['.xml'],
  'application/xml': ['.xml']
};

const MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024; // 10GB

interface FileUploadProps {
  onFileSelect?: (file: File) => void;
}

export const FileUpload: React.FC<FileUploadProps> = ({ onFileSelect }) => {
  const {
    currentUpload: { selectedFile, isUploading, uploadProgress, error },
    setSelectedFile,
    setError
  } = useUploadsStore();

  const onDrop = useCallback((acceptedFiles: File[], rejectedFiles: any[]) => {
    if (rejectedFiles.length > 0) {
      const rejection = rejectedFiles[0];
      if (rejection.errors[0]?.code === 'file-too-large') {
        setError('File size exceeds 10GB limit');
      } else if (rejection.errors[0]?.code === 'file-invalid-type') {
        setError('Invalid file type. Please upload CSV, JSON, or XML files');
      } else {
        setError('File upload error occurred');
      }
      return;
    }

    if (acceptedFiles.length > 0) {
      const file = acceptedFiles[0];
      setSelectedFile(file);
      setError(null);
      onFileSelect?.(file);
    }
  }, [setSelectedFile, setError, onFileSelect]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: ACCEPTED_FILE_TYPES,
    maxSize: MAX_FILE_SIZE,
    multiple: false,
    disabled: isUploading
  });

  return (
    <Box>
      <Paper
        {...getRootProps()}
        sx={{
          p: 4,
          textAlign: 'center',
          cursor: isUploading ? 'not-allowed' : 'pointer',
          border: '2px dashed',
          borderColor: isDragActive ? 'primary.main' : 'grey.300',
          backgroundColor: isDragActive ? 'action.hover' : 'background.paper',
          transition: 'all 0.3s ease',
          opacity: isUploading ? 0.6 : 1,
          '&:hover': {
            borderColor: 'primary.main',
            backgroundColor: 'action.hover'
          }
        }}
      >
        <input {...getInputProps()} />

        <CloudUpload sx={{ fontSize: 48, color: 'primary.main', mb: 2 }} />

        <Typography variant="h6" gutterBottom>
          {isDragActive
            ? 'Drop the file here'
            : 'Drag and drop a file here, or click to select'
          }
        </Typography>

        <Typography variant="body2" color="textSecondary" gutterBottom>
          Supported formats: CSV, JSON, XML (max 10GB)
        </Typography>

        <Box sx={{ mt: 2, display: 'flex', justifyContent: 'center', gap: 1 }}>
          <Chip label="CSV" size="small" variant="outlined" />
          <Chip label="JSON" size="small" variant="outlined" />
          <Chip label="XML" size="small" variant="outlined" />
        </Box>
      </Paper>

      {selectedFile && (
        <Box sx={{ mt: 2 }}>
          <Paper sx={{ p: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <InsertDriveFile color="primary" />
              <Box sx={{ flexGrow: 1 }}>
                <Typography variant="subtitle2">
                  {selectedFile.name}
                </Typography>
                <Typography variant="caption" color="textSecondary">
                  {formatFileSize(selectedFile.size)}
                </Typography>
              </Box>
            </Box>

            {isUploading && (
              <Box sx={{ mt: 2 }}>
                <LinearProgress
                  variant="determinate"
                  value={uploadProgress}
                  sx={{ mb: 1 }}
                />
                <Typography variant="caption" color="textSecondary">
                  Uploading... {Math.round(uploadProgress)}%
                </Typography>
              </Box>
            )}
          </Paper>
        </Box>
      )}

      {error && (
        <Alert severity="error" sx={{ mt: 2 }} icon={<ErrorIcon />}>
          {error}
        </Alert>
      )}
    </Box>
  );
};

export default FileUpload;
```

## Destination Selector Component

### components/upload/DestinationSelector.tsx
```tsx
import React, { useState } from 'react';
import {
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Typography,
  Box,
  TextField,
  Button,
  Alert
} from '@mui/material';
import { ExpandMore, Storage, Analytics, Folder } from '@mui/icons-material';
import { useForm, Controller } from 'react-hook-form';
import { yupResolver } from '@hookform/resolvers/yup';
import * as yup from 'yup';
import { useUploadsStore, DestinationConfig } from '../../store/uploadsStore';

const destinationSchemas = {
  postgresql: yup.object({
    host: yup.string().required('Host is required'),
    port: yup.number().min(1).max(65535).required('Port is required'),
    database: yup.string().required('Database is required'),
    schema: yup.string().default('public'),
    table_prefix: yup.string().default('imported_')
  }),
  clickhouse: yup.object({
    host: yup.string().required('Host is required'),
    port: yup.number().min(1).max(65535).required('Port is required'),
    database: yup.string().required('Database is required'),
    table_prefix: yup.string().default('analytics_')
  }),
  hdfs: yup.object({
    namenode_url: yup.string().required('NameNode URL is required'),
    base_path: yup.string().required('Base path is required'),
    file_format: yup.string().oneOf(['parquet', 'orc']).default('parquet'),
    compression: yup.string().oneOf(['none', 'snappy', 'gzip']).default('snappy')
  })
};

interface DestinationSelectorProps {
  onDestinationChange?: (config: DestinationConfig | null) => void;
}

export const DestinationSelector: React.FC<DestinationSelectorProps> = ({
  onDestinationChange
}) => {
  const {
    currentUpload: { destinationConfig },
    setDestinationConfig
  } = useUploadsStore();

  const [selectedType, setSelectedType] = useState<string>('postgresql');

  const { control, handleSubmit, reset, watch, formState: { errors } } = useForm({
    resolver: yupResolver(destinationSchemas[selectedType as keyof typeof destinationSchemas]),
    defaultValues: getDefaultValues(selectedType),
    mode: 'onChange'
  });

  function getDefaultValues(type: string) {
    const defaults = {
      postgresql: {
        host: 'postgres',
        port: 5432,
        database: 'user_data',
        schema: 'public',
        table_prefix: 'imported_'
      },
      clickhouse: {
        host: 'clickhouse',
        port: 8123,
        database: 'analytics',
        table_prefix: 'analytics_'
      },
      hdfs: {
        namenode_url: 'hdfs://namenode:9000',
        base_path: '/bigdata',
        file_format: 'parquet',
        compression: 'snappy'
      }
    };
    return defaults[type as keyof typeof defaults] || {};
  }

  const handleTypeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const newType = event.target.value;
    setSelectedType(newType);
    reset(getDefaultValues(newType));
  };

  const onSubmit = (data: any) => {
    const config: DestinationConfig = {
      type: selectedType as 'postgresql' | 'clickhouse' | 'hdfs',
      config: data,
      name: `${selectedType.charAt(0).toUpperCase() + selectedType.slice(1)} Configuration`
    };

    setDestinationConfig(config);
    onDestinationChange?.(config);
  };

  const destinationIcons = {
    postgresql: <Storage sx={{ mr: 1 }} />,
    clickhouse: <Analytics sx={{ mr: 1 }} />,
    hdfs: <Folder sx={{ mr: 1 }} />
  };

  const destinationDescriptions = {
    postgresql: 'Relational database for structured data',
    clickhouse: 'Column-oriented database for analytics',
    hdfs: 'Distributed file system for big data'
  };

  return (
    <Box>
      <FormControl component="fieldset">
        <FormLabel component="legend">
          <Typography variant="h6" gutterBottom>
            Select Destination
          </Typography>
        </FormLabel>

        <RadioGroup value={selectedType} onChange={handleTypeChange}>
          <FormControlLabel
            value="postgresql"
            control={<Radio />}
            label={
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                {destinationIcons.postgresql}
                <Box>
                  <Typography variant="subtitle2">PostgreSQL</Typography>
                  <Typography variant="caption" color="textSecondary">
                    {destinationDescriptions.postgresql}
                  </Typography>
                </Box>
              </Box>
            }
          />
          <FormControlLabel
            value="clickhouse"
            control={<Radio />}
            label={
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                {destinationIcons.clickhouse}
                <Box>
                  <Typography variant="subtitle2">ClickHouse</Typography>
                  <Typography variant="caption" color="textSecondary">
                    {destinationDescriptions.clickhouse}
                  </Typography>
                </Box>
              </Box>
            }
          />
          <FormControlLabel
            value="hdfs"
            control={<Radio />}
            label={
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                {destinationIcons.hdfs}
                <Box>
                  <Typography variant="subtitle2">HDFS</Typography>
                  <Typography variant="caption" color="textSecondary">
                    {destinationDescriptions.hdfs}
                  </Typography>
                </Box>
              </Box>
            }
          />
        </RadioGroup>
      </FormControl>

      <Accordion expanded sx={{ mt: 3 }}>
        <AccordionSummary>
          <Typography variant="subtitle1">
            Configuration for {selectedType.charAt(0).toUpperCase() + selectedType.slice(1)}
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box component="form" onSubmit={handleSubmit(onSubmit)}>
            {renderConfigurationFields(selectedType, control, errors)}

            <Button
              type="submit"
              variant="contained"
              sx={{ mt: 2 }}
              fullWidth
            >
              Apply Configuration
            </Button>
          </Box>
        </AccordionDetails>
      </Accordion>

      {destinationConfig && destinationConfig.type === selectedType && (
        <Alert severity="success" sx={{ mt: 2 }}>
          Configuration applied successfully for {destinationConfig.name}
        </Alert>
      )}
    </Box>
  );
};

function renderConfigurationFields(type: string, control: any, errors: any) {
  const commonFieldProps = {
    fullWidth: true,
    margin: 'normal' as const,
    variant: 'outlined' as const,
  };

  switch (type) {
    case 'postgresql':
      return (
        <>
          <Controller
            name="host"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Host"
                error={!!errors.host}
                helperText={errors.host?.message}
              />
            )}
          />
          <Controller
            name="port"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Port"
                type="number"
                error={!!errors.port}
                helperText={errors.port?.message}
              />
            )}
          />
          <Controller
            name="database"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Database"
                error={!!errors.database}
                helperText={errors.database?.message}
              />
            )}
          />
          <Controller
            name="schema"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Schema"
                error={!!errors.schema}
                helperText={errors.schema?.message}
              />
            )}
          />
          <Controller
            name="table_prefix"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Table Prefix"
                error={!!errors.table_prefix}
                helperText={errors.table_prefix?.message}
              />
            )}
          />
        </>
      );

    case 'clickhouse':
      return (
        <>
          <Controller
            name="host"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Host"
                error={!!errors.host}
                helperText={errors.host?.message}
              />
            )}
          />
          <Controller
            name="port"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Port"
                type="number"
                error={!!errors.port}
                helperText={errors.port?.message}
              />
            )}
          />
          <Controller
            name="database"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Database"
                error={!!errors.database}
                helperText={errors.database?.message}
              />
            )}
          />
          <Controller
            name="table_prefix"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Table Prefix"
                error={!!errors.table_prefix}
                helperText={errors.table_prefix?.message}
              />
            )}
          />
        </>
      );

    case 'hdfs':
      return (
        <>
          <Controller
            name="namenode_url"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="NameNode URL"
                error={!!errors.namenode_url}
                helperText={errors.namenode_url?.message}
              />
            )}
          />
          <Controller
            name="base_path"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Base Path"
                error={!!errors.base_path}
                helperText={errors.base_path?.message}
              />
            )}
          />
          <Controller
            name="file_format"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="File Format"
                select
                SelectProps={{ native: true }}
                error={!!errors.file_format}
                helperText={errors.file_format?.message}
              >
                <option value="parquet">Parquet</option>
                <option value="orc">ORC</option>
              </TextField>
            )}
          />
          <Controller
            name="compression"
            control={control}
            render={({ field }) => (
              <TextField
                {...field}
                {...commonFieldProps}
                label="Compression"
                select
                SelectProps={{ native: true }}
                error={!!errors.compression}
                helperText={errors.compression?.message}
              >
                <option value="none">None</option>
                <option value="snappy">Snappy</option>
                <option value="gzip">GZip</option>
              </TextField>
            )}
          />
        </>
      );

    default:
      return null;
  }
}

export default DestinationSelector;
```

## Задачи для выполнения

### 1. Базовая настройка React приложения
- [ ] Создать структуру проекта с TypeScript
- [ ] Настроить package.json с необходимыми зависимостями
- [ ] Создать Dockerfile для production build
- [ ] Настроить основные компоненты (App, Layout, ErrorBoundary)

### 2. State Management и Store
- [ ] Реализовать Zustand stores для jobs, uploads, destinations
- [ ] Добавить TypeScript типы для всех состояний
- [ ] Интеграция с React Query для API calls
- [ ] Настроить devtools для debugging

### 3. File Upload функциональность
- [ ] Компонент FileUpload с drag&drop поддержкой
- [ ] Валидация файлов (размер, тип)
- [ ] Progress indicator для загрузки
- [ ] Обработка ошибок при загрузке

### 4. Destination Configuration
- [ ] DestinationSelector с поддержкой всех типов
- [ ] Динамические формы конфигурации
- [ ] Валидация конфигураций с yup
- [ ] Предустановленные значения по умолчанию

### 5. Интеграция с API и WebSocket
- [ ] API client с axios
- [ ] WebSocket интеграция для real-time updates
- [ ] Error handling и retry логика
- [ ] Toast notifications для пользователя

### 6. UI/UX компоненты
- [ ] Material-UI тема и styling
- [ ] Responsive дизайн для мобильных устройств
- [ ] Loading states и skeleton screens
- [ ] Accessibility поддержка

### 7. Тестирование и отладка
- [ ] Unit тесты для компонентов
- [ ] Integration тесты для stores
- [ ] E2E тесты для критических путей
- [ ] Storybook для компонентов (опционально)

## Ожидаемый результат
После выполнения этой задачи должно быть готово React приложение с полнофункциональным интерфейсом для загрузки файлов и конфигурации destinations.

## Следующая задача
[08_file_upload_interface.md](./08_file_upload_interface.md) - Доработка интерфейса загрузки файлов