# Задача 9: React Flow Process Visualization

## Цель
Создать интерактивную визуализацию ETL процесса с использованием React Flow, показывающую все этапы обработки данных в real-time с метриками и статусами.

## Технические требования
- React Flow для создания диаграмм процессов
- Real-time обновления статуса узлов через WebSocket
- Интерактивные узлы с детальной информацией
- Кастомные типы узлов для разных этапов процесса
- Анимации и визуальные индикаторы прогресса

## Установка зависимостей

### package.json (дополнения)
```json
{
  "dependencies": {
    "reactflow": "^11.10.1",
    "@reactflow/core": "^11.10.1",
    "@reactflow/controls": "^11.2.3",
    "@reactflow/background": "^11.3.7",
    "@reactflow/minimap": "^11.7.7",
    "d3-ease": "^3.0.1",
    "react-spring": "^9.7.2"
  }
}
```

## Главный компонент Process Flow

### components/visualization/ProcessFlow.tsx
```tsx
import React, { useCallback, useEffect, useState, useMemo } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
  Connection,
  Edge,
  Node,
  ReactFlowProvider,
  Panel,
  ConnectionMode
} from 'reactflow';
import { Box, Paper, Typography, Chip } from '@mui/material';

import 'reactflow/dist/style.css';

// Кастомные узлы
import FileSourceNode from './nodes/FileSourceNode';
import ProcessingNode from './nodes/ProcessingNode';
import DestinationNode from './nodes/DestinationNode';
import ValidatorNode from './nodes/ValidatorNode';
import MetricsNode from './nodes/MetricsNode';

// Типы узлов
const nodeTypes = {
  fileSource: FileSourceNode,
  processing: ProcessingNode,
  destination: DestinationNode,
  validator: ValidatorNode,
  metrics: MetricsNode
};

// Стили для разных типов edges
const edgeTypes = {
  default: 'smoothstep'
};

interface ProcessFlowProps {
  jobId: string;
  jobData: any;
  progressData: any;
}

export const ProcessFlow: React.FC<ProcessFlowProps> = ({
  jobId,
  jobData,
  progressData
}) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [isInitialized, setIsInitialized] = useState(false);

  // Создание начальных узлов на основе данных задачи
  const createInitialNodes = useCallback(() => {
    if (!jobData) return [];

    const destinationType = jobData.destination_type;
    const fileName = jobData.filename;

    const initialNodes: Node[] = [
      // Источник файла
      {
        id: 'source',
        type: 'fileSource',
        position: { x: 50, y: 200 },
        data: {
          label: 'File Source',
          fileName: fileName,
          fileSize: jobData.file_size,
          fileFormat: jobData.file_format,
          status: 'completed'
        },
        draggable: false
      },

      // Валидация файла
      {
        id: 'validation',
        type: 'validator',
        position: { x: 300, y: 100 },
        data: {
          label: 'File Validation',
          status: progressData?.validation?.status || 'pending',
          details: progressData?.validation?.details || {}
        },
        draggable: false
      },

      // Анализ схемы
      {
        id: 'analysis',
        type: 'processing',
        position: { x: 300, y: 200 },
        data: {
          label: 'Schema Analysis',
          status: progressData?.analysis?.status || 'pending',
          recordsProcessed: 0,
          totalRecords: 0,
          details: progressData?.analysis?.details || {}
        },
        draggable: false
      },

      // ETL процесс
      {
        id: 'etl',
        type: 'processing',
        position: { x: 550, y: 200 },
        data: {
          label: 'ETL Processing',
          status: progressData?.etl?.status || 'pending',
          recordsProcessed: progressData?.records_processed || 0,
          totalRecords: jobData.records_total || 0,
          details: {
            chunksProcessed: progressData?.chunks_processed || 0,
            currentChunk: progressData?.current_chunk || 0,
            processingSpeed: progressData?.processing_speed || 0
          }
        },
        draggable: false
      },

      // Destination
      {
        id: 'destination',
        type: 'destination',
        position: { x: 800, y: 200 },
        data: {
          label: `${destinationType.charAt(0).toUpperCase() + destinationType.slice(1)} Destination`,
          destinationType: destinationType,
          config: jobData.destination_config,
          status: progressData?.destination?.status || 'pending',
          recordsLoaded: progressData?.records_loaded || 0
        },
        draggable: false
      },

      // Финальная валидация
      {
        id: 'final-validation',
        type: 'validator',
        position: { x: 800, y: 350 },
        data: {
          label: 'Final Validation',
          status: progressData?.final_validation?.status || 'pending',
          details: progressData?.final_validation?.details || {}
        },
        draggable: false
      },

      // Метрики
      {
        id: 'metrics',
        type: 'metrics',
        position: { x: 300, y: 350 },
        data: {
          label: 'Processing Metrics',
          metrics: {
            duration: progressData?.duration || 0,
            throughput: progressData?.throughput || 0,
            errorRate: progressData?.error_rate || 0,
            memoryUsage: progressData?.memory_usage || 0
          },
          status: 'active'
        },
        draggable: false
      }
    ];

    return initialNodes;
  }, [jobData, progressData]);

  // Создание connections между узлами
  const createInitialEdges = useCallback(() => {
    const initialEdges: Edge[] = [
      {
        id: 'source-validation',
        source: 'source',
        target: 'validation',
        type: 'smoothstep',
        animated: false
      },
      {
        id: 'validation-analysis',
        source: 'validation',
        target: 'analysis',
        type: 'smoothstep',
        animated: false
      },
      {
        id: 'analysis-etl',
        source: 'analysis',
        target: 'etl',
        type: 'smoothstep',
        animated: false
      },
      {
        id: 'etl-destination',
        source: 'etl',
        target: 'destination',
        type: 'smoothstep',
        animated: false
      },
      {
        id: 'destination-final-validation',
        source: 'destination',
        target: 'final-validation',
        type: 'smoothstep',
        animated: false
      },
      {
        id: 'analysis-metrics',
        source: 'analysis',
        target: 'metrics',
        type: 'smoothstep',
        animated: false,
        style: { strokeDasharray: '5,5', opacity: 0.6 }
      },
      {
        id: 'etl-metrics',
        source: 'etl',
        target: 'metrics',
        type: 'smoothstep',
        animated: false,
        style: { strokeDasharray: '5,5', opacity: 0.6 }
      }
    ];

    return initialEdges;
  }, []);

  // Инициализация узлов и edges
  useEffect(() => {
    if (jobData && !isInitialized) {
      const initialNodes = createInitialNodes();
      const initialEdges = createInitialEdges();

      setNodes(initialNodes);
      setEdges(initialEdges);
      setIsInitialized(true);
    }
  }, [jobData, isInitialized, createInitialNodes, createInitialEdges, setNodes, setEdges]);

  // Обновление узлов при изменении прогресса
  useEffect(() => {
    if (progressData && isInitialized) {
      setNodes((nodes) =>
        nodes.map((node) => {
          const newData = { ...node.data };

          switch (node.id) {
            case 'validation':
              newData.status = progressData.validation?.status || 'pending';
              newData.details = progressData.validation?.details || {};
              break;
            case 'analysis':
              newData.status = progressData.analysis?.status || 'pending';
              newData.details = progressData.analysis?.details || {};
              break;
            case 'etl':
              newData.status = progressData.etl?.status || 'pending';
              newData.recordsProcessed = progressData.records_processed || 0;
              newData.totalRecords = jobData?.records_total || 0;
              newData.details = {
                chunksProcessed: progressData.chunks_processed || 0,
                currentChunk: progressData.current_chunk || 0,
                processingSpeed: progressData.processing_speed || 0
              };
              break;
            case 'destination':
              newData.status = progressData.destination?.status || 'pending';
              newData.recordsLoaded = progressData.records_loaded || 0;
              break;
            case 'final-validation':
              newData.status = progressData.final_validation?.status || 'pending';
              newData.details = progressData.final_validation?.details || {};
              break;
            case 'metrics':
              newData.metrics = {
                duration: progressData.duration || 0,
                throughput: progressData.throughput || 0,
                errorRate: progressData.error_rate || 0,
                memoryUsage: progressData.memory_usage || 0
              };
              break;
          }

          return { ...node, data: newData };
        })
      );

      // Обновление анимации edges на основе активных процессов
      setEdges((edges) =>
        edges.map((edge) => {
          let animated = false;

          if (edge.id === 'analysis-etl' && progressData.etl?.status === 'processing') {
            animated = true;
          } else if (edge.id === 'etl-destination' && progressData.destination?.status === 'loading') {
            animated = true;
          }

          return { ...edge, animated };
        })
      );
    }
  }, [progressData, isInitialized, jobData, setNodes, setEdges]);

  const onConnect = useCallback(
    (params: Edge | Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  const proOptions = {
    hideAttribution: true
  };

  // Цветовая схема для minimap
  const nodeColor = (node: Node) => {
    switch (node.data.status) {
      case 'completed':
        return '#4caf50';
      case 'processing':
        return '#2196f3';
      case 'failed':
        return '#f44336';
      case 'pending':
        return '#9e9e9e';
      default:
        return '#e0e0e0';
    }
  };

  if (!jobData) {
    return (
      <Box sx={{ p: 3, textAlign: 'center' }}>
        <Typography variant="h6" color="textSecondary">
          No job data available for visualization
        </Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ height: '600px', width: '100%', border: '1px solid #e0e0e0', borderRadius: 1 }}>
      <ReactFlowProvider>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          connectionMode={ConnectionMode.Loose}
          fitView
          proOptions={proOptions}
          defaultViewport={{ x: 0, y: 0, zoom: 0.8 }}
        >
          <Background color="#f5f5f5" gap={20} />
          <Controls showInteractive={false} />
          <MiniMap
            nodeColor={nodeColor}
            nodeStrokeWidth={3}
            pannable
            zoomable
            style={{
              backgroundColor: '#ffffff',
              border: '1px solid #e0e0e0'
            }}
          />

          <Panel position="top-left">
            <Paper sx={{ p: 2, minWidth: 200 }}>
              <Typography variant="subtitle1" gutterBottom>
                Job Status: {jobData.status}
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                <Chip
                  label={`${jobData.file_format.toUpperCase()}`}
                  size="small"
                  color="primary"
                  variant="outlined"
                />
                <Chip
                  label={jobData.destination_type}
                  size="small"
                  color="secondary"
                  variant="outlined"
                />
                {progressData?.records_processed && (
                  <Chip
                    label={`${progressData.records_processed.toLocaleString()} records`}
                    size="small"
                    color="success"
                    variant="outlined"
                  />
                )}
              </Box>
            </Paper>
          </Panel>
        </ReactFlow>
      </ReactFlowProvider>
    </Box>
  );
};

export default ProcessFlow;
```

## Кастомные узлы

### components/visualization/nodes/FileSourceNode.tsx
```tsx
import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import {
  Box,
  Paper,
  Typography,
  LinearProgress,
  Chip
} from '@mui/material';
import { InsertDriveFile, CheckCircle } from '@mui/icons-material';

interface FileSourceNodeData {
  label: string;
  fileName: string;
  fileSize: number;
  fileFormat: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
}

export const FileSourceNode: React.FC<NodeProps<FileSourceNodeData>> = ({ data }) => {
  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'success';
      case 'processing': return 'primary';
      case 'failed': return 'error';
      default: return 'default';
    }
  };

  return (
    <Paper
      elevation={3}
      sx={{
        p: 2,
        minWidth: 200,
        border: '2px solid',
        borderColor: data.status === 'completed' ? 'success.main' : 'grey.300',
        backgroundColor: 'background.paper'
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <InsertDriveFile color={data.status === 'completed' ? 'success' : 'primary'} />
        {data.status === 'completed' && (
          <CheckCircle color="success" sx={{ fontSize: 16 }} />
        )}
        <Typography variant="subtitle2" fontWeight="bold">
          {data.label}
        </Typography>
      </Box>

      <Typography variant="body2" sx={{ mb: 1 }}>
        {data.fileName}
      </Typography>

      <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
        <Chip
          label={data.fileFormat.toUpperCase()}
          size="small"
          color="primary"
          variant="outlined"
        />
        <Chip
          label={formatFileSize(data.fileSize)}
          size="small"
          variant="outlined"
        />
      </Box>

      <Chip
        label={data.status.charAt(0).toUpperCase() + data.status.slice(1)}
        size="small"
        color={getStatusColor(data.status) as any}
      />

      <Handle
        type="source"
        position={Position.Right}
        style={{
          background: data.status === 'completed' ? '#4caf50' : '#9e9e9e'
        }}
      />
    </Paper>
  );
};

export default FileSourceNode;
```

### components/visualization/nodes/ProcessingNode.tsx
```tsx
import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import {
  Box,
  Paper,
  Typography,
  LinearProgress,
  Chip,
  CircularProgress
} from '@mui/material';
import {
  Transform,
  Speed,
  Memory
} from '@mui/icons-material';

interface ProcessingNodeData {
  label: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  recordsProcessed: number;
  totalRecords: number;
  details: {
    chunksProcessed?: number;
    currentChunk?: number;
    processingSpeed?: number;
  };
}

export const ProcessingNode: React.FC<NodeProps<ProcessingNodeData>> = ({ data }) => {
  const progress = data.totalRecords > 0
    ? Math.round((data.recordsProcessed / data.totalRecords) * 100)
    : 0;

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'success';
      case 'processing': return 'primary';
      case 'failed': return 'error';
      default: return 'default';
    }
  };

  const getBorderColor = (status: string) => {
    switch (status) {
      case 'completed': return 'success.main';
      case 'processing': return 'primary.main';
      case 'failed': return 'error.main';
      default: return 'grey.300';
    }
  };

  return (
    <Paper
      elevation={3}
      sx={{
        p: 2,
        minWidth: 220,
        border: '2px solid',
        borderColor: getBorderColor(data.status),
        backgroundColor: 'background.paper',
        position: 'relative',
        overflow: 'visible'
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        style={{
          background: data.status === 'processing' ? '#2196f3' : '#9e9e9e'
        }}
      />

      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <Transform color={data.status === 'processing' ? 'primary' : 'disabled'} />
        {data.status === 'processing' && (
          <CircularProgress size={16} />
        )}
        <Typography variant="subtitle2" fontWeight="bold">
          {data.label}
        </Typography>
      </Box>

      <Chip
        label={data.status.charAt(0).toUpperCase() + data.status.slice(1)}
        size="small"
        color={getStatusColor(data.status) as any}
        sx={{ mb: 2 }}
      />

      {data.status === 'processing' && data.totalRecords > 0 && (
        <Box sx={{ mb: 2 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
            <Typography variant="caption">Progress</Typography>
            <Typography variant="caption">{progress}%</Typography>
          </Box>
          <LinearProgress
            variant="determinate"
            value={progress}
            sx={{ height: 8, borderRadius: 4 }}
          />
          <Typography variant="caption" color="textSecondary" sx={{ mt: 0.5, display: 'block' }}>
            {data.recordsProcessed.toLocaleString()} / {data.totalRecords.toLocaleString()} records
          </Typography>
        </Box>
      )}

      {data.details && (
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mt: 1 }}>
          {data.details.processingSpeed && data.details.processingSpeed > 0 && (
            <Chip
              icon={<Speed />}
              label={`${data.details.processingSpeed}/s`}
              size="small"
              variant="outlined"
            />
          )}
          {data.details.chunksProcessed && (
            <Chip
              label={`Chunk ${data.details.currentChunk}/${data.details.chunksProcessed}`}
              size="small"
              variant="outlined"
            />
          )}
        </Box>
      )}

      <Handle
        type="source"
        position={Position.Right}
        style={{
          background: data.status === 'completed' ? '#4caf50' :
                     data.status === 'processing' ? '#2196f3' : '#9e9e9e'
        }}
      />
    </Paper>
  );
};

export default ProcessingNode;
```

### components/visualization/nodes/DestinationNode.tsx
```tsx
import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import {
  Box,
  Paper,
  Typography,
  Chip,
  LinearProgress
} from '@mui/material';
import {
  Storage,
  Analytics,
  Folder,
  CloudDone
} from '@mui/icons-material';

interface DestinationNodeData {
  label: string;
  destinationType: 'postgresql' | 'clickhouse' | 'hdfs';
  config: any;
  status: 'pending' | 'loading' | 'completed' | 'failed';
  recordsLoaded: number;
}

export const DestinationNode: React.FC<NodeProps<DestinationNodeData>> = ({ data }) => {
  const getDestinationIcon = () => {
    switch (data.destinationType) {
      case 'postgresql':
        return <Storage color={data.status === 'completed' ? 'success' : 'primary'} />;
      case 'clickhouse':
        return <Analytics color={data.status === 'completed' ? 'success' : 'secondary'} />;
      case 'hdfs':
        return <Folder color={data.status === 'completed' ? 'success' : 'info'} />;
      default:
        return <Storage />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'success';
      case 'loading': return 'primary';
      case 'failed': return 'error';
      default: return 'default';
    }
  };

  const getBorderColor = (status: string) => {
    switch (status) {
      case 'completed': return 'success.main';
      case 'loading': return 'primary.main';
      case 'failed': return 'error.main';
      default: return 'grey.300';
    }
  };

  const getDestinationDetails = () => {
    switch (data.destinationType) {
      case 'postgresql':
        return `${data.config?.database || 'database'}.${data.config?.schema || 'schema'}`;
      case 'clickhouse':
        return `${data.config?.database || 'analytics'}`;
      case 'hdfs':
        return `${data.config?.base_path || '/bigdata'}`;
      default:
        return 'Unknown';
    }
  };

  return (
    <Paper
      elevation={3}
      sx={{
        p: 2,
        minWidth: 200,
        border: '2px solid',
        borderColor: getBorderColor(data.status),
        backgroundColor: 'background.paper'
      }}
    >
      <Handle
        type="target"
        position={Position.Left}
        style={{
          background: data.status === 'loading' ? '#2196f3' : '#9e9e9e'
        }}
      />

      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        {getDestinationIcon()}
        {data.status === 'completed' && (
          <CloudDone color="success" sx={{ fontSize: 16 }} />
        )}
        <Typography variant="subtitle2" fontWeight="bold">
          {data.label}
        </Typography>
      </Box>

      <Typography variant="body2" color="textSecondary" sx={{ mb: 1 }}>
        {getDestinationDetails()}
      </Typography>

      <Chip
        label={data.status.charAt(0).toUpperCase() + data.status.slice(1)}
        size="small"
        color={getStatusColor(data.status) as any}
        sx={{ mb: 1 }}
      />

      {data.status === 'loading' && (
        <Box sx={{ mt: 1 }}>
          <LinearProgress color="primary" />
          <Typography variant="caption" color="textSecondary" sx={{ mt: 0.5, display: 'block' }}>
            Loading data...
          </Typography>
        </Box>
      )}

      {data.recordsLoaded > 0 && (
        <Typography variant="caption" sx={{ display: 'block', mt: 1 }}>
          {data.recordsLoaded.toLocaleString()} records loaded
        </Typography>
      )}

      <Handle
        type="source"
        position={Position.Bottom}
        style={{
          background: data.status === 'completed' ? '#4caf50' : '#9e9e9e'
        }}
      />
    </Paper>
  );
};

export default DestinationNode;
```

### components/visualization/nodes/MetricsNode.tsx
```tsx
import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import {
  Box,
  Paper,
  Typography,
  Grid
} from '@mui/material';
import {
  Speed,
  Timer,
  Memory,
  Error
} from '@mui/icons-material';

interface MetricsNodeData {
  label: string;
  metrics: {
    duration: number;
    throughput: number;
    errorRate: number;
    memoryUsage: number;
  };
  status: 'active' | 'inactive';
}

export const MetricsNode: React.FC<NodeProps<MetricsNodeData>> = ({ data }) => {
  const formatDuration = (seconds: number): string => {
    if (seconds < 60) {
      return `${Math.round(seconds)}s`;
    } else if (seconds < 3600) {
      return `${Math.round(seconds / 60)}m ${Math.round(seconds % 60)}s`;
    } else {
      return `${Math.round(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
    }
  };

  const formatThroughput = (recordsPerSecond: number): string => {
    if (recordsPerSecond < 1000) {
      return `${Math.round(recordsPerSecond)}/s`;
    } else if (recordsPerSecond < 1000000) {
      return `${Math.round(recordsPerSecond / 1000)}K/s`;
    } else {
      return `${Math.round(recordsPerSecond / 1000000)}M/s`;
    }
  };

  const formatMemory = (mb: number): string => {
    if (mb < 1024) {
      return `${Math.round(mb)}MB`;
    } else {
      return `${Math.round(mb / 1024 * 10) / 10}GB`;
    }
  };

  const MetricItem: React.FC<{
    icon: React.ReactNode;
    label: string;
    value: string;
    color?: string;
  }> = ({ icon, label, value, color = 'textPrimary' }) => (
    <Box sx={{ textAlign: 'center' }}>
      <Box sx={{ color, mb: 0.5 }}>
        {icon}
      </Box>
      <Typography variant="caption" color="textSecondary" sx={{ display: 'block' }}>
        {label}
      </Typography>
      <Typography variant="body2" fontWeight="bold">
        {value}
      </Typography>
    </Box>
  );

  return (
    <Paper
      elevation={2}
      sx={{
        p: 2,
        minWidth: 240,
        border: '1px solid',
        borderColor: data.status === 'active' ? 'info.main' : 'grey.300',
        backgroundColor: 'background.paper',
        opacity: data.status === 'active' ? 1 : 0.7
      }}
    >
      <Handle
        type="target"
        position={Position.Top}
        style={{
          background: data.status === 'active' ? '#2196f3' : '#9e9e9e'
        }}
      />

      <Typography variant="subtitle2" fontWeight="bold" sx={{ mb: 2, textAlign: 'center' }}>
        {data.label}
      </Typography>

      <Grid container spacing={2}>
        <Grid item xs={6}>
          <MetricItem
            icon={<Timer />}
            label="Duration"
            value={formatDuration(data.metrics.duration)}
            color="primary.main"
          />
        </Grid>
        <Grid item xs={6}>
          <MetricItem
            icon={<Speed />}
            label="Throughput"
            value={formatThroughput(data.metrics.throughput)}
            color="success.main"
          />
        </Grid>
        <Grid item xs={6}>
          <MetricItem
            icon={<Memory />}
            label="Memory"
            value={formatMemory(data.metrics.memoryUsage)}
            color="info.main"
          />
        </Grid>
        <Grid item xs={6}>
          <MetricItem
            icon={<Error />}
            label="Error Rate"
            value={`${data.metrics.errorRate.toFixed(2)}%`}
            color={data.metrics.errorRate > 5 ? 'error.main' : 'success.main'}
          />
        </Grid>
      </Grid>
    </Paper>
  );
};

export default MetricsNode;
```

## Process Flow Container

### components/jobs/JobProcessFlow.tsx
```tsx
import React from 'react';
import { Box, Paper, Typography, Alert } from '@mui/material';
import { useWebSocketContext } from '../../contexts/WebSocketContext';
import ProcessFlow from '../visualization/ProcessFlow';

interface JobProcessFlowProps {
  job: any;
}

export const JobProcessFlow: React.FC<JobProcessFlowProps> = ({ job }) => {
  const { getJobProgress } = useWebSocketContext();

  const progressData = getJobProgress(job.job_id);

  if (!job) {
    return (
      <Alert severity="info">
        <Typography>No job selected for visualization</Typography>
      </Alert>
    );
  }

  return (
    <Paper sx={{ p: 2 }}>
      <Typography variant="h6" gutterBottom>
        Process Flow - {job.filename}
      </Typography>

      <ProcessFlow
        jobId={job.job_id}
        jobData={job}
        progressData={progressData}
      />

      {progressData && (
        <Box sx={{ mt: 2 }}>
          <Typography variant="caption" color="textSecondary">
            Last updated: {new Date(progressData.timestamp).toLocaleTimeString()}
          </Typography>
        </Box>
      )}
    </Paper>
  );
};

export default JobProcessFlow;
```

## Задачи для выполнения

### 1. Установка и настройка React Flow
- [ ] Добавить React Flow зависимости в package.json
- [ ] Настроить базовую структуру компонента ProcessFlow
- [ ] Добавить стили и конфигурацию React Flow

### 2. Создание кастомных узлов
- [ ] FileSourceNode для отображения исходного файла
- [ ] ProcessingNode для ETL процессов с прогресс-баром
- [ ] DestinationNode для различных типов хранилищ
- [ ] ValidatorNode для валидации данных
- [ ] MetricsNode для отображения метрик производительности

### 3. Логика создания диаграмм
- [ ] Генерация узлов на основе конфигурации задачи
- [ ] Создание связей между узлами (edges)
- [ ] Позиционирование узлов для оптимального отображения

### 4. Real-time обновления
- [ ] Интеграция с WebSocket для получения обновлений
- [ ] Обновление статусов узлов в реальном времени
- [ ] Анимация edges для активных процессов
- [ ] Обновление метрик и прогресса

### 5. Интерактивность и UX
- [ ] MiniMap для навигации по большим диаграммам
- [ ] Controls для масштабирования и панорамирования
- [ ] Tooltip'ы с детальной информацией о узлах
- [ ] Панель с общей информацией о задаче

### 6. Визуальные улучшения
- [ ] Цветовая кодировка статусов узлов
- [ ] Анимации для переходов состояний
- [ ] Responsive дизайн для мобильных устройств
- [ ] Темная/светлая тема поддержка

### 7. Интеграция с основным приложением
- [ ] Интеграция в JobDetails компонент
- [ ] Связь с jobs store для получения данных
- [ ] Автоматические обновления при изменении данных

## Ожидаемый результат
После выполнения этой задачи должна быть готова полнофункциональная визуализация ETL процесса с real-time обновлениями, интерактивными узлами и красивым интерфейсом.

## Следующая задача
[10_airflow_dlt_integration.md](./10_airflow_dlt_integration.md) - Полная интеграция Airflow с dlt