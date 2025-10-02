import React, { useEffect } from 'react'
import ReactFlow, {
  Node,
  Edge,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
  Position,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { Box, Typography, Paper, Chip } from '@mui/material'
import { useParams } from 'react-router-dom'
import { useJobStore } from '../store/jobStore'

const StageNode = ({ data }: { data: any }) => {
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return '#4caf50'
      case 'processing':
        return '#2196f3'
      case 'pending':
        return '#ff9800'
      case 'failed':
        return '#f44336'
      default:
        return '#757575'
    }
  }

  return (
    <Box
      sx={{
        minWidth: 200,
        backgroundColor: '#1e1e1e',
        border: `3px solid ${getStatusColor(data.status)}`,
        borderRadius: 2,
        padding: 2,
      }}
    >
      <Typography variant="h6" sx={{ color: '#fff', marginBottom: 1 }}>
        {data.label}
      </Typography>
      <Chip
        label={data.status}
        size="small"
        sx={{
          backgroundColor: getStatusColor(data.status),
          color: '#fff',
          marginBottom: 1,
        }}
      />
      {data.progress !== undefined && (
        <Box sx={{ marginTop: 1 }}>
          <Typography variant="caption" sx={{ color: '#999' }}>
            Progress: {data.progress}%
          </Typography>
          <Box
            sx={{
              width: '100%',
              height: 6,
              backgroundColor: '#333',
              borderRadius: 1,
              overflow: 'hidden',
              marginTop: 0.5,
            }}
          >
            <Box
              sx={{
                width: `${data.progress}%`,
                height: '100%',
                backgroundColor: getStatusColor(data.status),
                transition: 'width 0.3s ease',
              }}
            />
          </Box>
        </Box>
      )}
      {data.details && (
        <Typography variant="caption" sx={{ color: '#999', display: 'block', marginTop: 1 }}>
          {data.details}
        </Typography>
      )}
    </Box>
  )
}

const nodeTypes = {
  stageNode: StageNode,
}

export const PipelineVisualization: React.FC = () => {
  const { jobId } = useParams<{ jobId: string }>()
  const { jobs, jobStatuses, fetchJob, fetchJobs, subscribeToJob, unsubscribeFromJob } = useJobStore()
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  const job = jobs.find((j) => j.job_id === jobId)
  const jobStatus = jobStatuses[jobId || '']

  // Fetch job if not in store
  useEffect(() => {
    if (jobId && !job) {
      console.log('[Pipeline] Job not found in store, fetching:', jobId)
      fetchJob(jobId).catch(() => {
        console.log('[Pipeline] Failed to fetch job, trying fetchJobs')
        fetchJobs()
      })
    }
  }, [jobId, job, fetchJob, fetchJobs])

  useEffect(() => {
    if (jobId) {
      subscribeToJob(jobId)
      return () => unsubscribeFromJob(jobId)
    }
  }, [jobId, subscribeToJob, unsubscribeFromJob])

  useEffect(() => {
    if (!job) return

    const stages = [
      {
        id: 'upload',
        label: 'File Upload',
        status: 'completed',
        progress: 100,
        details: `${job.filename}${job.file_size ? ` (${(job.file_size / 1024 / 1024).toFixed(2)} MB)` : ''}`,
      },
      {
        id: 'analysis',
        label: 'File Analysis',
        status: job.status === 'pending' ? 'pending' : 'completed',
        progress: job.status === 'pending' ? 0 : 100,
        details: `Format: ${job.file_format}`,
      },
      {
        id: 'schema',
        label: 'Schema Generation',
        status: job.status === 'processing' && jobStatus?.current_stage === 'Schema Generation' ? 'processing' :
                job.status === 'completed' ? 'completed' : 'pending',
        progress: job.status === 'processing' && jobStatus?.current_stage === 'Schema Generation'
          ? jobStatus.progress_percentage
          : job.status === 'completed' ? 100 : 0,
        details: 'DDL & ETL scripts',
      },
      {
        id: 'orchestration',
        label: 'Orchestration',
        status: job.status === 'processing' && jobStatus?.current_stage === 'Orchestration' ? 'processing' :
                job.status === 'completed' ? 'completed' : 'pending',
        progress: job.status === 'processing' && jobStatus?.current_stage === 'Orchestration'
          ? jobStatus.progress_percentage
          : job.status === 'completed' ? 100 : 0,
        details: 'Airflow DAG',
      },
      {
        id: 'loading',
        label: 'Data Loading',
        status: job.status === 'processing' && jobStatus?.current_stage === 'Data Loading' ? 'processing' :
                job.status === 'completed' ? 'completed' : 'pending',
        progress: job.status === 'processing' && jobStatus?.current_stage === 'Data Loading'
          ? jobStatus.progress_percentage
          : job.status === 'completed' ? 100 : 0,
        details: jobStatus?.records_processed
          ? `${jobStatus.records_processed || 0} / ${jobStatus.records_total || 0} records`
          : job.records_processed
          ? `${job.records_processed || 0} / ${job.records_total || 0} records`
          : '',
      },
      {
        id: 'destination',
        label: 'Destination',
        status: job.status === 'completed' ? 'completed' : 'pending',
        progress: job.status === 'completed' ? 100 : 0,
        details: `${job.destination_type}: ${job.destination_config?.table_name || 'N/A'}`,
      },
    ]

    const newNodes: Node[] = stages.map((stage, index) => ({
      id: stage.id,
      type: 'stageNode',
      position: { x: index * 300, y: 200 },
      data: stage,
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    }))

    const newEdges: Edge[] = stages.slice(0, -1).map((stage, index) => ({
      id: `edge-${index}`,
      source: stage.id,
      target: stages[index + 1].id,
      type: 'smoothstep',
      animated: stage.status === 'completed' || stage.status === 'processing',
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: stage.status === 'completed' ? '#4caf50' : '#2196f3',
      },
      style: {
        stroke: stage.status === 'completed' ? '#4caf50' : '#2196f3',
        strokeWidth: 3,
      },
    }))

    setNodes(newNodes)
    setEdges(newEdges)
  }, [job, jobStatus, setNodes, setEdges])

  if (!job) {
    return (
      <Box p={3}>
        <Typography>Loading job {jobId}...</Typography>
      </Box>
    )
  }

  return (
    <Box sx={{ width: '100%', height: 'calc(100vh - 100px)' }}>
      <Paper sx={{ height: '100%', position: 'relative' }}>
        <Box sx={{ padding: 2, backgroundColor: '#1e1e1e' }}>
          <Typography variant="h5" gutterBottom>
            Pipeline Visualization: {job.filename}
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Job ID: {job.job_id} | Status: {job.status}
          </Typography>
          {jobStatus && (
            <Typography variant="body2" color="text.secondary">
              Current Stage: {jobStatus.current_stage} | Progress: {jobStatus.progress_percentage}%
            </Typography>
          )}
        </Box>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeTypes={nodeTypes}
          fitView
          attributionPosition="bottom-left"
        >
          <Background />
          <Controls />
        </ReactFlow>
      </Paper>
    </Box>
  )
}
