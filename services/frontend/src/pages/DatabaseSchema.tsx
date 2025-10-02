import React, { useEffect, useState, useCallback } from 'react'
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
import { Box, Typography, Paper, CircularProgress } from '@mui/material'
import { api } from '../utils/api'

interface TableColumn {
  name: string
  type: string
  nullable: boolean
  default: string | null
}

interface DatabaseTable {
  name: string
  columns: TableColumn[]
  size: string
  column_count: number
}

interface Relationship {
  from_table: string
  to_table: string
  from_column: string
  to_column: string
}

interface SchemaData {
  tables: DatabaseTable[]
  relationships: Relationship[]
}

const TableNode = ({ data }: { data: DatabaseTable }) => {
  return (
    <Box
      sx={{
        minWidth: 250,
        backgroundColor: '#1e1e1e',
        border: '2px solid #00bcd4',
        borderRadius: 1,
        overflow: 'hidden',
      }}
    >
      <Box
        sx={{
          backgroundColor: '#00bcd4',
          color: '#000',
          padding: 1,
          fontWeight: 'bold',
        }}
      >
        <Typography variant="subtitle1">{data.name}</Typography>
        <Typography variant="caption">
          {data.column_count} columns | {data.size}
        </Typography>
      </Box>
      <Box sx={{ padding: 1, maxHeight: 300, overflow: 'auto' }}>
        {data.columns.slice(0, 10).map((col, idx) => (
          <Box
            key={idx}
            sx={{
              padding: 0.5,
              fontSize: '0.75rem',
              borderBottom: '1px solid #333',
              color: '#fff',
            }}
          >
            <Typography variant="caption" component="div">
              <strong>{col.name}</strong>: {col.type}
              {!col.nullable && ' NOT NULL'}
            </Typography>
          </Box>
        ))}
        {data.columns.length > 10 && (
          <Typography variant="caption" sx={{ color: '#999', padding: 0.5 }}>
            +{data.columns.length - 10} more columns
          </Typography>
        )}
      </Box>
    </Box>
  )
}

const nodeTypes = {
  tableNode: TableNode,
}

export const DatabaseSchema: React.FC = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchSchema = useCallback(async () => {
    try {
      setLoading(true)
      const response = await api.get<SchemaData>('/schema/full')
      const { tables, relationships } = response.data

      // Position tables in a grid layout
      const gridSize = Math.ceil(Math.sqrt(tables.length))
      const spacing = 400

      const newNodes: Node[] = tables.map((table, index) => {
        const row = Math.floor(index / gridSize)
        const col = index % gridSize

        return {
          id: table.name,
          type: 'tableNode',
          position: { x: col * spacing, y: row * spacing },
          data: table,
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
        }
      })

      const newEdges: Edge[] = relationships.map((rel, index) => ({
        id: `edge-${index}`,
        source: rel.from_table,
        target: rel.to_table,
        label: `${rel.from_column} → ${rel.to_column}`,
        type: 'smoothstep',
        animated: true,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: '#00bcd4',
        },
        style: { stroke: '#00bcd4', strokeWidth: 2 },
        labelStyle: { fill: '#fff', fontSize: 10 },
        labelBgStyle: { fill: '#1e1e1e' },
      }))

      setNodes(newNodes)
      setEdges(newEdges)
      setError(null)
    } catch (err) {
      console.error('Error fetching schema:', err)
      setError('Failed to load database schema')
    } finally {
      setLoading(false)
    }
  }, [setNodes, setEdges])

  useEffect(() => {
    fetchSchema()
  }, [fetchSchema])

  if (loading) {
    return (
      <Box
        display="flex"
        justifyContent="center"
        alignItems="center"
        minHeight="80vh"
      >
        <CircularProgress />
      </Box>
    )
  }

  if (error) {
    return (
      <Box p={3}>
        <Typography color="error">{error}</Typography>
      </Box>
    )
  }

  return (
    <Box sx={{ width: '100%', height: 'calc(100vh - 100px)' }}>
      <Paper sx={{ height: '100%', position: 'relative' }}>
        <Box sx={{ padding: 2, backgroundColor: '#1e1e1e' }}>
          <Typography variant="h5" gutterBottom>
            Database Schema Visualization
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {nodes.length} tables | {edges.length} relationships
          </Typography>
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
