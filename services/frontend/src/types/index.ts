export interface Job {
  id: number
  job_id: string
  filename: string
  file_size: number
  file_format: string
  destination_type: string
  destination_config: Record<string, any>
  status: string
  created_at: string
  updated_at: string
  started_at?: string
  completed_at?: string
  error_message?: string
  records_processed: number
  records_total?: number
  ddl_script?: string
  etl_script?: string
  airflow_dag_id?: string
  airflow_run_id?: string
}

export interface JobStatus {
  job_id: string
  status: string
  records_processed: number
  records_total: number
  progress_percentage: number
  current_stage?: string
  error_message?: string
}

export interface FileUploadResponse {
  job_id: string
  filename: string
  file_size: number
  file_format: string
  message: string
}

export interface DestinationConfig {
  postgresql: {
    host: string
    port: number
    database: string
    schema_name: string
    table_name: string
    username: string
    password: string
  }
  clickhouse: {
    host: string
    port: number
    database: string
    table_name: string
    username: string
    password: string
    engine?: string
    order_by?: string
  }
  hdfs: {
    namenode_url: string
    path: string
    file_format: string
    compression?: string
    partition_by?: string
  }
}

export interface WebSocketMessage {
  type: string
  job_id: string
  data: Record<string, any>
  timestamp: string
}

export interface ProcessFlowNode {
  id: string
  type: string
  data: {
    label: string
    status: 'pending' | 'running' | 'completed' | 'failed'
    progress?: number
    message?: string
  }
  position: { x: number; y: number }
}

export interface ProcessFlowEdge {
  id: string
  source: string
  target: string
  animated?: boolean
  style?: Record<string, any>
}