# Задача 8: File Upload Interface Enhancement

## Цель
Доработать интерфейс загрузки файлов с интеграцией всех компонентов: файл-загрузка, выбор destination, конфигурация, просмотр DDL/ETL скриптов и запуск обработки.

## Технические требования
- Пошаговый wizard для загрузки файлов
- Предварительный просмотр данных из файла
- Показ сгенерированных DDL и ETL скриптов
- Интеграция с backend API для загрузки
- Real-time обновления через WebSocket

## Главный Upload Wizard

### components/upload/UploadWizard.tsx
```tsx
import React, { useState } from 'react';
import {
  Stepper,
  Step,
  StepLabel,
  StepContent,
  Box,
  Button,
  Typography,
  Paper,
  Alert,
  Fade
} from '@mui/material';
import {
  CloudUpload,
  Settings,
  Visibility,
  PlayArrow
} from '@mui/icons-material';
import { useUploadsStore } from '../../store/uploadsStore';
import { useJobsStore } from '../../store/jobsStore';
import FileUpload from './FileUpload';
import DestinationSelector from './DestinationSelector';
import DataPreview from './DataPreview';
import ScriptPreview from './ScriptPreview';
import UploadSummary from './UploadSummary';
import { useFileUpload } from '../../hooks/useFileUpload';
import { useWebSocketContext } from '../../contexts/WebSocketContext';
import toast from 'react-hot-toast';

const steps = [
  {
    label: 'Select File',
    description: 'Choose a CSV, JSON, or XML file to upload',
    icon: <CloudUpload />
  },
  {
    label: 'Configure Destination',
    description: 'Select and configure the target destination',
    icon: <Settings />
  },
  {
    label: 'Preview & Validate',
    description: 'Review data preview and generated scripts',
    icon: <Visibility />
  },
  {
    label: 'Start Processing',
    description: 'Begin the ETL process',
    icon: <PlayArrow />
  }
];

export const UploadWizard: React.FC = () => {
  const [activeStep, setActiveStep] = useState(0);
  const [previewData, setPreviewData] = useState<any>(null);
  const [ddlScript, setDdlScript] = useState<string>('');
  const [etlScript, setEtlScript] = useState<string>('');

  const {
    currentUpload: { selectedFile, destinationConfig, isUploading, error },
    resetUpload
  } = useUploadsStore();

  const { addJob } = useJobsStore();
  const { subscribeToJob } = useWebSocketContext();
  const { uploadFile, analyzeFile } = useFileUpload();

  const handleNext = () => {
    setActiveStep((prevStep) => prevStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevStep) => prevStep - 1);
  };

  const handleReset = () => {
    setActiveStep(0);
    resetUpload();
    setPreviewData(null);
    setDdlScript('');
    setEtlScript('');
  };

  const handleFileSelect = async (file: File) => {
    try {
      // Анализ файла и получение превью
      const analysis = await analyzeFile(file);
      setPreviewData(analysis.preview);
      setDdlScript(analysis.ddlScript);
      setEtlScript(analysis.etlScript);
    } catch (error) {
      toast.error('Failed to analyze file');
      console.error('File analysis error:', error);
    }
  };

  const handleDestinationChange = (config: any) => {
    // При изменении destination перегенерируем DDL скрипт
    if (selectedFile && config) {
      generateScripts(selectedFile, config);
    }
  };

  const generateScripts = async (file: File, config: any) => {
    try {
      const analysis = await analyzeFile(file, config);
      setDdlScript(analysis.ddlScript);
      setEtlScript(analysis.etlScript);
    } catch (error) {
      toast.error('Failed to generate scripts');
    }
  };

  const handleStartProcessing = async () => {
    if (!selectedFile || !destinationConfig) {
      toast.error('Please complete all steps');
      return;
    }

    try {
      const job = await uploadFile(selectedFile, destinationConfig);

      // Добавляем задачу в store
      addJob(job);

      // Подписываемся на WebSocket обновления
      subscribeToJob(job.job_id);

      toast.success('Processing started!');

      // Переходим к следующему шагу или закрываем wizard
      handleNext();
    } catch (error) {
      toast.error('Failed to start processing');
    }
  };

  const canProceedToNext = (step: number): boolean => {
    switch (step) {
      case 0:
        return !!selectedFile;
      case 1:
        return !!destinationConfig;
      case 2:
        return !!ddlScript && !!etlScript;
      default:
        return true;
    }
  };

  const renderStepContent = (step: number) => {
    switch (step) {
      case 0:
        return (
          <Fade in timeout={500}>
            <Box>
              <FileUpload onFileSelect={handleFileSelect} />
              {previewData && (
                <Box sx={{ mt: 2 }}>
                  <Alert severity="info">
                    File analyzed successfully! {previewData.rowCount} rows detected.
                  </Alert>
                </Box>
              )}
            </Box>
          </Fade>
        );

      case 1:
        return (
          <Fade in timeout={500}>
            <Box>
              <DestinationSelector onDestinationChange={handleDestinationChange} />
            </Box>
          </Fade>
        );

      case 2:
        return (
          <Fade in timeout={500}>
            <Box>
              <DataPreview
                data={previewData}
                file={selectedFile}
              />
              <Box sx={{ mt: 3 }}>
                <ScriptPreview
                  ddlScript={ddlScript}
                  etlScript={etlScript}
                  destinationType={destinationConfig?.type}
                />
              </Box>
            </Box>
          </Fade>
        );

      case 3:
        return (
          <Fade in timeout={500}>
            <Box>
              <UploadSummary
                file={selectedFile}
                destination={destinationConfig}
                previewData={previewData}
                onStartProcessing={handleStartProcessing}
                isProcessing={isUploading}
              />
            </Box>
          </Fade>
        );

      case 4:
        return (
          <Fade in timeout={500}>
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <Alert severity="success" sx={{ mb: 2 }}>
                <Typography variant="h6">Processing Started!</Typography>
                <Typography variant="body2">
                  Your file is being processed. You can monitor the progress in the Jobs section.
                </Typography>
              </Alert>
              <Button
                variant="contained"
                onClick={handleReset}
                sx={{ mt: 2 }}
              >
                Upload Another File
              </Button>
            </Box>
          </Fade>
        );

      default:
        return null;
    }
  };

  if (error) {
    return (
      <Alert severity="error" sx={{ mb: 2 }}>
        <Typography variant="h6">Upload Error</Typography>
        <Typography variant="body2">{error}</Typography>
        <Button onClick={handleReset} sx={{ mt: 1 }}>
          Try Again
        </Button>
      </Alert>
    );
  }

  return (
    <Box sx={{ maxWidth: 800, mx: 'auto', p: 3 }}>
      <Typography variant="h4" gutterBottom align="center">
        Upload File for Processing
      </Typography>

      <Stepper activeStep={activeStep} orientation="vertical">
        {steps.map((step, index) => (
          <Step key={step.label}>
            <StepLabel
              optional={
                index === steps.length - 1 ? (
                  <Typography variant="caption">Last step</Typography>
                ) : null
              }
              StepIconComponent={() => step.icon}
            >
              {step.label}
            </StepLabel>
            <StepContent>
              <Typography variant="body2" color="textSecondary" sx={{ mb: 2 }}>
                {step.description}
              </Typography>

              {renderStepContent(index)}

              <Box sx={{ mb: 2, mt: 3 }}>
                <Button
                  variant="contained"
                  onClick={index === steps.length - 2 ? handleStartProcessing : handleNext}
                  disabled={!canProceedToNext(index) || isUploading}
                  sx={{ mt: 1, mr: 1 }}
                >
                  {index === steps.length - 2 ? 'Start Processing' : 'Continue'}
                </Button>
                <Button
                  disabled={index === 0 || isUploading}
                  onClick={handleBack}
                  sx={{ mt: 1, mr: 1 }}
                >
                  Back
                </Button>
              </Box>
            </StepContent>
          </Step>
        ))}
      </Stepper>
    </Box>
  );
};

export default UploadWizard;
```

## Data Preview Component

### components/upload/DataPreview.tsx
```tsx
import React from 'react';
import {
  Box,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import { ExpandMore, Info } from '@mui/icons-material';

interface DataPreviewProps {
  data: any;
  file: File | null;
}

export const DataPreview: React.FC<DataPreviewProps> = ({ data, file }) => {
  if (!data || !file) {
    return (
      <Alert severity="info">
        <Typography>No data to preview</Typography>
      </Alert>
    );
  }

  const { columns, rows, rowCount, fileInfo } = data;

  const formatDataType = (type: string) => {
    const typeColors: Record<string, 'default' | 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning'> = {
      'string': 'default',
      'integer': 'primary',
      'float': 'secondary',
      'boolean': 'success',
      'datetime': 'info',
      'date': 'warning'
    };

    return (
      <Chip
        label={type}
        size="small"
        color={typeColors[type] || 'default'}
        variant="outlined"
      />
    );
  };

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Data Preview
      </Typography>

      {/* File Information */}
      <Accordion>
        <AccordionSummary expandIcon={<ExpandMore />}>
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Info sx={{ mr: 1 }} />
            <Typography variant="subtitle1">File Information</Typography>
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 2 }}>
            <Box>
              <Typography variant="body2" color="textSecondary">Filename</Typography>
              <Typography variant="body1">{file.name}</Typography>
            </Box>
            <Box>
              <Typography variant="body2" color="textSecondary">File Size</Typography>
              <Typography variant="body1">{formatFileSize(file.size)}</Typography>
            </Box>
            <Box>
              <Typography variant="body2" color="textSecondary">Total Rows</Typography>
              <Typography variant="body1">{rowCount.toLocaleString()}</Typography>
            </Box>
            <Box>
              <Typography variant="body2" color="textSecondary">Columns</Typography>
              <Typography variant="body1">{columns.length}</Typography>
            </Box>
          </Box>
        </AccordionDetails>
      </Accordion>

      {/* Schema Information */}
      <Accordion sx={{ mt: 2 }}>
        <AccordionSummary expandIcon={<ExpandMore />}>
          <Typography variant="subtitle1">Schema Information</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <TableContainer component={Paper} variant="outlined">
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell><strong>Column Name</strong></TableCell>
                  <TableCell><strong>Data Type</strong></TableCell>
                  <TableCell><strong>Nullable</strong></TableCell>
                  <TableCell><strong>Sample Values</strong></TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {columns.map((column: any, index: number) => (
                  <TableRow key={index}>
                    <TableCell>
                      <Typography variant="body2" fontWeight="medium">
                        {column.name}
                      </Typography>
                    </TableCell>
                    <TableCell>{formatDataType(column.type)}</TableCell>
                    <TableCell>
                      <Chip
                        label={column.nullable ? 'Yes' : 'No'}
                        size="small"
                        color={column.nullable ? 'warning' : 'success'}
                        variant="outlined"
                      />
                    </TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                        {column.sampleValues?.slice(0, 3).map((value: any, i: number) => (
                          <Chip
                            key={i}
                            label={String(value).substring(0, 20) + (String(value).length > 20 ? '...' : '')}
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
        </AccordionDetails>
      </Accordion>

      {/* Data Sample */}
      <Box sx={{ mt: 3 }}>
        <Typography variant="subtitle1" gutterBottom>
          Sample Data (First 10 rows)
        </Typography>

        <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
          <Table stickyHeader size="small">
            <TableHead>
              <TableRow>
                {columns.map((column: any, index: number) => (
                  <TableCell key={index}>
                    <strong>{column.name}</strong>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {rows.slice(0, 10).map((row: any, rowIndex: number) => (
                <TableRow key={rowIndex}>
                  {columns.map((column: any, colIndex: number) => (
                    <TableCell key={colIndex}>
                      <Typography variant="body2">
                        {formatCellValue(row[column.name])}
                      </Typography>
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {rowCount > 10 && (
          <Typography variant="caption" color="textSecondary" sx={{ mt: 1, display: 'block' }}>
            Showing 10 of {rowCount.toLocaleString()} rows
          </Typography>
        )}
      </Box>
    </Box>
  );
};

function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatCellValue(value: any): string {
  if (value === null || value === undefined) {
    return '—';
  }
  if (typeof value === 'string' && value.length > 50) {
    return value.substring(0, 50) + '...';
  }
  return String(value);
}

export default DataPreview;
```

## Script Preview Component

### components/upload/ScriptPreview.tsx
```tsx
import React, { useState } from 'react';
import {
  Box,
  Typography,
  Tabs,
  Tab,
  Paper,
  IconButton,
  Tooltip,
  Alert
} from '@mui/material';
import {
  ContentCopy,
  Download,
  Code,
  Storage
} from '@mui/icons-material';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { tomorrow } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { saveAs } from 'file-saver';
import toast from 'react-hot-toast';

interface ScriptPreviewProps {
  ddlScript: string;
  etlScript: string;
  destinationType?: string;
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel({ children, value, index, ...other }: TabPanelProps) {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`script-tabpanel-${index}`}
      aria-labelledby={`script-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

export const ScriptPreview: React.FC<ScriptPreviewProps> = ({
  ddlScript,
  etlScript,
  destinationType = 'postgresql'
}) => {
  const [activeTab, setActiveTab] = useState(0);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  const copyToClipboard = (text: string, scriptType: string) => {
    navigator.clipboard.writeText(text).then(() => {
      toast.success(`${scriptType} script copied to clipboard!`);
    }).catch(() => {
      toast.error('Failed to copy to clipboard');
    });
  };

  const downloadScript = (text: string, filename: string) => {
    const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
    saveAs(blob, filename);
    toast.success('Script downloaded successfully!');
  };

  const getLanguageForHighlighting = () => {
    switch (destinationType) {
      case 'postgresql':
      case 'clickhouse':
        return 'sql';
      case 'hdfs':
        return 'python';
      default:
        return 'sql';
    }
  };

  const getFileExtension = () => {
    switch (destinationType) {
      case 'postgresql':
      case 'clickhouse':
        return 'sql';
      case 'hdfs':
        return 'py';
      default:
        return 'sql';
    }
  };

  if (!ddlScript && !etlScript) {
    return (
      <Alert severity="info">
        <Typography>Scripts will be generated after file analysis</Typography>
      </Alert>
    );
  }

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Generated Scripts
      </Typography>

      <Paper>
        <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Tabs value={activeTab} onChange={handleTabChange} aria-label="script tabs">
            <Tab
              icon={<Storage />}
              iconPosition="start"
              label="DDL Script"
              id="script-tab-0"
              aria-controls="script-tabpanel-0"
            />
            <Tab
              icon={<Code />}
              iconPosition="start"
              label="ETL Script"
              id="script-tab-1"
              aria-controls="script-tabpanel-1"
            />
          </Tabs>
        </Box>

        <TabPanel value={activeTab} index={0}>
          <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="subtitle1" color="textSecondary">
              Data Definition Language (DDL) Script
            </Typography>
            <Box>
              <Tooltip title="Copy to clipboard">
                <IconButton
                  size="small"
                  onClick={() => copyToClipboard(ddlScript, 'DDL')}
                >
                  <ContentCopy />
                </IconButton>
              </Tooltip>
              <Tooltip title="Download script">
                <IconButton
                  size="small"
                  onClick={() => downloadScript(ddlScript, `ddl_script.${getFileExtension()}`)}
                >
                  <Download />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>

          <Paper variant="outlined" sx={{ maxHeight: 400, overflow: 'auto' }}>
            <SyntaxHighlighter
              language={getLanguageForHighlighting()}
              style={tomorrow}
              customStyle={{
                margin: 0,
                padding: '16px',
                fontSize: '14px',
                lineHeight: '1.4'
              }}
              showLineNumbers
            >
              {ddlScript || '-- DDL script will appear here'}
            </SyntaxHighlighter>
          </Paper>

          <Alert severity="info" sx={{ mt: 2 }}>
            <Typography variant="body2">
              This DDL script will create the necessary tables and schemas in your {destinationType} destination.
            </Typography>
          </Alert>
        </TabPanel>

        <TabPanel value={activeTab} index={1}>
          <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="subtitle1" color="textSecondary">
              Extract, Transform, Load (ETL) Script
            </Typography>
            <Box>
              <Tooltip title="Copy to clipboard">
                <IconButton
                  size="small"
                  onClick={() => copyToClipboard(etlScript, 'ETL')}
                >
                  <ContentCopy />
                </IconButton>
              </Tooltip>
              <Tooltip title="Download script">
                <IconButton
                  size="small"
                  onClick={() => downloadScript(etlScript, `etl_script.${getFileExtension()}`)}
                >
                  <Download />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>

          <Paper variant="outlined" sx={{ maxHeight: 400, overflow: 'auto' }}>
            <SyntaxHighlighter
              language={destinationType === 'hdfs' ? 'python' : 'sql'}
              style={tomorrow}
              customStyle={{
                margin: 0,
                padding: '16px',
                fontSize: '14px',
                lineHeight: '1.4'
              }}
              showLineNumbers
            >
              {etlScript || '# ETL script will appear here'}
            </SyntaxHighlighter>
          </Paper>

          <Alert severity="info" sx={{ mt: 2 }}>
            <Typography variant="body2">
              This ETL script shows how your data will be processed and loaded into {destinationType}.
            </Typography>
          </Alert>
        </TabPanel>
      </Paper>
    </Box>
  );
};

export default ScriptPreview;
```

## Upload Summary Component

### components/upload/UploadSummary.tsx
```tsx
import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Divider,
  Button,
  CircularProgress,
  Alert,
  List,
  ListItem,
  ListItemIcon,
  ListItemText
} from '@mui/material';
import {
  InsertDriveFile,
  Storage,
  Analytics,
  Folder,
  CheckCircle,
  PlayArrow
} from '@mui/icons-material';

interface UploadSummaryProps {
  file: File | null;
  destination: any;
  previewData: any;
  onStartProcessing: () => void;
  isProcessing: boolean;
}

export const UploadSummary: React.FC<UploadSummaryProps> = ({
  file,
  destination,
  previewData,
  onStartProcessing,
  isProcessing
}) => {
  if (!file || !destination) {
    return (
      <Alert severity="error">
        <Typography>Missing file or destination configuration</Typography>
      </Alert>
    );
  }

  const getDestinationIcon = (type: string) => {
    switch (type) {
      case 'postgresql':
        return <Storage color="primary" />;
      case 'clickhouse':
        return <Analytics color="secondary" />;
      case 'hdfs':
        return <Folder color="success" />;
      default:
        return <Storage />;
    }
  };

  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const estimatedProcessingTime = () => {
    const sizeInMB = file.size / (1024 * 1024);
    const timePerMB = 2; // seconds per MB (rough estimate)
    const totalSeconds = Math.ceil(sizeInMB * timePerMB);

    if (totalSeconds < 60) {
      return `~${totalSeconds} seconds`;
    } else if (totalSeconds < 3600) {
      return `~${Math.ceil(totalSeconds / 60)} minutes`;
    } else {
      return `~${Math.ceil(totalSeconds / 3600)} hours`;
    }
  };

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Summary & Confirmation
      </Typography>

      <Paper sx={{ p: 3, mb: 3 }}>
        <Grid container spacing={3}>
          {/* File Information */}
          <Grid item xs={12} md={6}>
            <Box sx={{ mb: 2 }}>
              <Typography variant="subtitle1" gutterBottom color="primary">
                File Information
              </Typography>
              <List dense>
                <ListItem>
                  <ListItemIcon>
                    <InsertDriveFile />
                  </ListItemIcon>
                  <ListItemText
                    primary={file.name}
                    secondary={`${formatFileSize(file.size)} • ${file.type || 'Unknown type'}`}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon>
                    <CheckCircle color="success" />
                  </ListItemIcon>
                  <ListItemText
                    primary={`${previewData?.rowCount?.toLocaleString() || 'Unknown'} rows`}
                    secondary={`${previewData?.columns?.length || 'Unknown'} columns`}
                  />
                </ListItem>
              </List>
            </Box>
          </Grid>

          {/* Destination Information */}
          <Grid item xs={12} md={6}>
            <Box sx={{ mb: 2 }}>
              <Typography variant="subtitle1" gutterBottom color="primary">
                Destination Configuration
              </Typography>
              <List dense>
                <ListItem>
                  <ListItemIcon>
                    {getDestinationIcon(destination.type)}
                  </ListItemIcon>
                  <ListItemText
                    primary={destination.type.charAt(0).toUpperCase() + destination.type.slice(1)}
                    secondary={destination.name}
                  />
                </ListItem>
                <ListItem>
                  <ListItemIcon>
                    <CheckCircle color="success" />
                  </ListItemIcon>
                  <ListItemText
                    primary="Configuration verified"
                    secondary="All required parameters provided"
                  />
                </ListItem>
              </List>
            </Box>
          </Grid>
        </Grid>

        <Divider sx={{ my: 2 }} />

        {/* Processing Details */}
        <Box>
          <Typography variant="subtitle1" gutterBottom color="primary">
            Processing Details
          </Typography>
          <Grid container spacing={2}>
            <Grid item xs={12} md={4}>
              <Typography variant="body2" color="textSecondary">
                Estimated Processing Time
              </Typography>
              <Typography variant="h6">
                {estimatedProcessingTime()}
              </Typography>
            </Grid>
            <Grid item xs={12} md={4}>
              <Typography variant="body2" color="textSecondary">
                Processing Method
              </Typography>
              <Typography variant="h6">
                Streaming ETL
              </Typography>
            </Grid>
            <Grid item xs={12} md={4}>
              <Typography variant="body2" color="textSecondary">
                Chunk Size
              </Typography>
              <Typography variant="h6">
                10,000 rows
              </Typography>
            </Grid>
          </Grid>
        </Box>
      </Paper>

      {/* Process Steps */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="subtitle1" gutterBottom color="primary">
          What will happen next:
        </Typography>
        <List>
          <ListItem>
            <ListItemText
              primary="1. File Upload"
              secondary="Your file will be uploaded to the processing server"
            />
          </ListItem>
          <ListItem>
            <ListItemText
              primary="2. Schema Creation"
              secondary="Tables and schemas will be created in the destination"
            />
          </ListItem>
          <ListItem>
            <ListItemText
              primary="3. Data Processing"
              secondary="Data will be processed in chunks and loaded incrementally"
            />
          </ListItem>
          <ListItem>
            <ListItemText
              primary="4. Validation"
              secondary="Final validation and record count verification"
            />
          </ListItem>
        </List>
      </Paper>

      {/* Action Buttons */}
      <Box sx={{ display: 'flex', justifyContent: 'center', gap: 2 }}>
        <Button
          variant="contained"
          size="large"
          startIcon={isProcessing ? <CircularProgress size={20} color="inherit" /> : <PlayArrow />}
          onClick={onStartProcessing}
          disabled={isProcessing}
          sx={{ minWidth: 200 }}
        >
          {isProcessing ? 'Starting...' : 'Start Processing'}
        </Button>
      </Box>

      {isProcessing && (
        <Alert severity="info" sx={{ mt: 2 }}>
          <Typography variant="body2">
            Processing has started. You'll receive real-time updates on the progress.
          </Typography>
        </Alert>
      )}
    </Box>
  );
};

export default UploadSummary;
```

## useFileUpload Hook

### hooks/useFileUpload.ts
```tsx
import { useState } from 'react';
import { useUploadsStore } from '../store/uploadsStore';
import { api } from '../services/api';
import toast from 'react-hot-toast';

export const useFileUpload = () => {
  const [isAnalyzing, setIsAnalyzing] = useState(false);

  const {
    setIsUploading,
    setUploadProgress,
    setError
  } = useUploadsStore();

  const analyzeFile = async (file: File, destinationConfig?: any) => {
    setIsAnalyzing(true);
    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await api.post('/api/v1/files/analyze', formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      });

      return response.data;
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || 'File analysis failed';
      setError(errorMessage);
      throw error;
    } finally {
      setIsAnalyzing(false);
    }
  };

  const uploadFile = async (file: File, destinationConfig: any) => {
    setIsUploading(true);
    setUploadProgress(0);
    setError(null);

    try {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('destination_type', destinationConfig.type);
      formData.append('destination_config', JSON.stringify(destinationConfig.config));

      const response = await api.post('/api/v1/jobs/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        },
        onUploadProgress: (progressEvent) => {
          if (progressEvent.total) {
            const percentCompleted = Math.round(
              (progressEvent.loaded * 100) / progressEvent.total
            );
            setUploadProgress(percentCompleted);
          }
        }
      });

      toast.success('File uploaded successfully!');
      return response.data;

    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || 'Upload failed';
      setError(errorMessage);
      toast.error(errorMessage);
      throw error;
    } finally {
      setIsUploading(false);
    }
  };

  return {
    analyzeFile,
    uploadFile,
    isAnalyzing
  };
};
```

## Задачи для выполнения

### 1. Создать Upload Wizard компонент
- [ ] Пошаговый интерфейс с Material-UI Stepper
- [ ] Интеграция всех компонентов (FileUpload, DestinationSelector, Preview)
- [ ] Валидация каждого шага перед переходом
- [ ] Состояние и navigation между шагами

### 2. Реализовать Data Preview
- [ ] Компонент для отображения структуры файла
- [ ] Таблица с образцами данных
- [ ] Информация о схеме и типах данных
- [ ] Аккордеоны для разных типов информации

### 3. Создать Script Preview компонент
- [ ] Syntax highlighting для DDL/ETL скриптов
- [ ] Табы для переключения между DDL и ETL
- [ ] Функции копирования и скачивания скриптов
- [ ] Адаптация под разные типы destinations

### 4. Разработать Upload Summary
- [ ] Финальный обзор перед запуском обработки
- [ ] Отображение всей конфигурации
- [ ] Оценка времени обработки
- [ ] Кнопка запуска процесса

### 5. File Analysis Integration
- [ ] Hook useFileUpload для работы с API
- [ ] Анализ структуры файлов на backend
- [ ] Генерация DDL/ETL скриптов
- [ ] Обработка ошибок анализа

### 6. UX улучшения
- [ ] Loading states для всех операций
- [ ] Smooth transitions между шагами
- [ ] Toast notifications для пользователя
- [ ] Responsive design для мобильных устройств

### 7. Интеграция с WebSocket
- [ ] Подписка на обновления после начала обработки
- [ ] Real-time отображение прогресса
- [ ] Обработка ошибок и переподключений

## Ожидаемый результат
После выполнения этой задачи должен быть готов полнофункциональный wizard для загрузки файлов с предварительным просмотром данных, генерацией скриптов и запуском обработки.

## Следующая задача
[09_react_flow_visualization.md](./09_react_flow_visualization.md) - Создание визуализации процесса с React Flow