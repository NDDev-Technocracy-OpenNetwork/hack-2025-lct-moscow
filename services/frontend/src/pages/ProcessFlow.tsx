import React, { useEffect, useState, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  Alert,
  LinearProgress,
} from '@mui/material'
import {
  ArrowBack as ArrowBackIcon,
  Info as InfoIcon,
} from '@mui/icons-material'
import ReactFlow, {
  Node,
  Edge,
  addEdge,
  Connection,
  useNodesState,
  useEdgesState,
  ConnectionMode,
  Controls,
  MiniMap,
  Background,
  BackgroundVariant,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { useJobStore } from '../store/jobStore'

// Custom node component for process steps
const ProcessNode = ({ data }: { data: any }) => {
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return '#4caf50'
      case 'running':
        return '#2196f3'
      case 'failed':
        return '#f44336'
      case 'pending':
      default:
        return '#9e9e9e'
    }
  }

  return (
    <Card
      sx={{
        minWidth: 200,
        border: `2px solid ${getStatusColor(data.status)}`,
        backgroundColor: 'white',
      }}
    >
      <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
        <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
          {data.label}
        </Typography>
        <Typography variant="caption" color="text.secondary">
          Status: {data.status}
        </Typography>
        {data.progress !== undefined && (
          <Box mt={1}>
            <LinearProgress
              variant="determinate"
              value={data.progress}
              sx={{ height: 4, borderRadius: 2 }}
            />
            <Typography variant="caption" color="text.secondary">
              {Math.round(data.progress)}%
            </Typography>
          </Box>
        )}
        {data.message && (
          <Typography variant="caption" display="block" mt={1}>
            {data.message}
          </Typography>
        )}
      </CardContent>
    </Card>
  )
}

const nodeTypes = {
  process: ProcessNode,
}

export const ProcessFlow: React.FC = () => {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()
  const {
    currentJob,
    jobStatuses,
    isLoading,
    error,
    fetchJob,
    subscribeToJob,
    unsubscribeFromJob,
  } = useJobStore()

  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  // Initialize flow
  const initializeFlow = useCallback(() => {
    if (!currentJob) return

    const status = jobStatuses[jobId!] || {
      job_id: jobId!,
      status: currentJob.status,
      records_processed: currentJob.records_processed,
      records_total: currentJob.records_total || 0,
      progress_percentage: 0,
    }

    const flowNodes: Node[] = [
      {
        id: '1',
        type: 'process',
        position: { x: 100, y: 50 },
        data: {
          label: 'File Upload',
          status: 'completed',
          message: `${currentJob.filename} (${(currentJob.file_size / 1024 / 1024).toFixed(1)} MB)`,
        },
      },
      {
        id: '2',
        type: 'process',
        position: { x: 100, y: 200 },
        data: {
          label: 'File Validation',
          status: ['pending', 'validating'].includes(status.status) ? 'running' :
                 ['validated', 'analyzing', 'processing', 'completed'].includes(status.status) ? 'completed' :
                 status.status === 'failed' ? 'failed' : 'pending',
          message: 'Checking file format and integrity',
        },
      },
      {
        id: '3',
        type: 'process',
        position: { x: 100, y: 350 },
        data: {
          label: 'File Analysis',
          status: status.status === 'analyzing' ? 'running' :
                 ['processing', 'completed'].includes(status.status) ? 'completed' :
                 status.status === 'failed' ? 'failed' : 'pending',
          message: 'Detecting schema and generating DDL',
        },
      },
      {
        id: '4',
        type: 'process',
        position: { x: 100, y: 500 },
        data: {
          label: 'Data Processing',
          status: status.status === 'processing' ? 'running' :
                 status.status === 'completed' ? 'completed' :
                 status.status === 'failed' ? 'failed' : 'pending',
          progress: status.progress_percentage,
          message: `${status.records_processed.toLocaleString()} / ${status.records_total.toLocaleString()} records`,
        },
      },
      {
        id: '5',
        type: 'process',
        position: { x: 400, y: 350 },
        data: {
          label: `${currentJob.destination_type.toUpperCase()} Load`,
          status: status.status === 'completed' ? 'completed' :
                 status.status === 'processing' ? 'running' :
                 status.status === 'failed' ? 'failed' : 'pending',
          message: `Loading to ${currentJob.destination_type}`,
        },
      },
      {
        id: '6',
        type: 'process',
        position: { x: 400, y: 500 },
        data: {
          label: 'Finalization',
          status: status.status === 'completed' ? 'completed' :
                 status.status === 'failed' ? 'failed' : 'pending',
          message: 'Updating metadata and cleanup',
        },
      },
    ]

    const flowEdges: Edge[] = [
      { id: 'e1-2', source: '1', target: '2', animated: true },
      { id: 'e2-3', source: '2', target: '3', animated: true },
      { id: 'e3-4', source: '3', target: '4', animated: true },
      { id: 'e4-5', source: '4', target: '5', animated: true },
      { id: 'e5-6', source: '5', target: '6', animated: true },
    ]

    setNodes(flowNodes)
    setEdges(flowEdges)
  }, [currentJob, jobStatuses, jobId, setNodes, setEdges])

  useEffect(() => {
    if (jobId) {
      fetchJob(jobId)
      subscribeToJob(jobId)

      return () => {
        unsubscribeFromJob(jobId)
      }
    }
  }, [jobId, fetchJob, subscribeToJob, unsubscribeFromJob])

  useEffect(() => {
    initializeFlow()
  }, [initializeFlow])

  const onConnect = useCallback(
    (params: Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  )

  if (!jobId) {
    return (
      <Alert severity="error">
        Invalid job ID
      </Alert>
    )
  }

  if (isLoading && !currentJob) {
    return (
      <Box display="flex" justifyContent="center" mt={4}>
        <LinearProgress sx={{ width: 200 }} />
      </Box>
    )
  }

  if (error) {
    return (
      <Alert severity="error">
        Error: {error}
      </Alert>
    )
  }

  if (!currentJob) {
    return (
      <Alert severity="warning">
        Job not found
      </Alert>
    )
  }

  const status = jobStatuses[jobId] || {
    job_id: jobId,
    status: currentJob.status,
    records_processed: currentJob.records_processed,
    records_total: currentJob.records_total || 0,
    progress_percentage: 0,
  }

  return (
    <Box sx={{ height: '100vh', width: '100%' }}>
      <Box
        display="flex"
        alignItems="center"
        justifyContent="space-between"
        p={2}
        bgcolor="white"
        borderBottom={1}
        borderColor="divider"
      >
        <Box display="flex" alignItems="center">
          <Button
            startIcon={<ArrowBackIcon />}
            onClick={() => navigate(`/job/${jobId}`)}
            sx={{ mr: 2 }}
          >
            Back to Details
          </Button>
          <Typography variant="h6">
            Process Flow - {currentJob.filename}
          </Typography>
        </Box>

        <Box display="flex" alignItems="center" gap={2}>
          <Typography variant="body2" color="text.secondary">
            Status: {status.status}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Progress: {Math.round(status.progress_percentage)}%
          </Typography>
        </Box>
      </Box>

      <Box sx={{ height: 'calc(100vh - 80px)' }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          nodeTypes={nodeTypes}
          connectionMode={ConnectionMode.Loose}
          fitView
          fitViewOptions={{
            padding: 0.2,
          }}
        >
          <Controls />
          <MiniMap
            nodeColor={(node) => {
              switch (node.data.status) {
                case 'completed':
                  return '#4caf50'
                case 'running':
                  return '#2196f3'
                case 'failed':
                  return '#f44336'
                default:
                  return '#9e9e9e'
              }
            }}
            maskColor="rgb(240, 240, 240, 0.6)"
            style={{
              backgroundColor: 'white',
            }}
          />
          <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
        </ReactFlow>
      </Box>

      {/* Legend */}
      <Box
        position="absolute"
        top={100}
        right={20}
        bgcolor="white"
        border={1}
        borderColor="divider"
        borderRadius={1}
        p={2}
        sx={{ zIndex: 1000 }}
      >
        <Typography variant="subtitle2" gutterBottom>
          <InfoIcon fontSize="small" sx={{ mr: 1 }} />
          Status Legend
        </Typography>
        <Box display="flex" flexDirection="column" gap={0.5}>
          <Box display="flex" alignItems="center" gap={1}>
            <Box width={12} height={12} bgcolor="#9e9e9e" borderRadius="50%" />
            <Typography variant="caption">Pending</Typography>
          </Box>
          <Box display="flex" alignItems="center" gap={1}>
            <Box width={12} height={12} bgcolor="#2196f3" borderRadius="50%" />
            <Typography variant="caption">Running</Typography>
          </Box>
          <Box display="flex" alignItems="center" gap={1}>
            <Box width={12} height={12} bgcolor="#4caf50" borderRadius="50%" />
            <Typography variant="caption">Completed</Typography>
          </Box>
          <Box display="flex" alignItems="center" gap={1}>
            <Box width={12} height={12} bgcolor="#f44336" borderRadius="50%" />
            <Typography variant="caption">Failed</Typography>
          </Box>
        </Box>
      </Box>
    </Box>
  )
}