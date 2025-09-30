-- ================================================================================================
-- BIGDATA PROCESSING SYSTEM - POSTGRESQL INITIALIZATION SCRIPT
-- ================================================================================================
-- This script initializes the PostgreSQL database for the BigData Processing System
-- It creates necessary databases, schemas, tables, and grants appropriate permissions
-- ================================================================================================

-- Note: Airflow database (airflow_db) is created by init-airflow-db.sh script
-- This is executed before this SQL script, so airflow_db should already exist

-- ================================================================================================
-- SCHEMA CREATION
-- ================================================================================================

-- Create schemas in the main database
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS data_warehouse;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS metadata;

-- ================================================================================================
-- TABLE CREATION - JOB MANAGEMENT
-- ================================================================================================

-- Main table for tracking processing jobs
CREATE TABLE IF NOT EXISTS processing_jobs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(36) UNIQUE NOT NULL,
    filename VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL,
    file_type VARCHAR(20) NOT NULL CHECK (file_type IN ('csv', 'json', 'xml', 'parquet', 'avro')),
    destination VARCHAR(50) NOT NULL CHECK (destination IN ('postgres', 'clickhouse', 'hdfs', 's3')),
    destination_config JSONB NOT NULL DEFAULT '{}',
    source_path VARCHAR(500),
    target_schema VARCHAR(100),
    target_table VARCHAR(100),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')),
    priority INTEGER DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,

    -- Constraints
    CONSTRAINT valid_completion_time CHECK (completed_at IS NULL OR completed_at >= started_at),
    CONSTRAINT valid_start_time CHECK (started_at IS NULL OR started_at >= created_at)
);

-- Table for tracking processing progress in real-time
CREATE TABLE IF NOT EXISTS processing_progress (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    stage VARCHAR(50) NOT NULL,
    stage_order INTEGER NOT NULL DEFAULT 0,
    progress_percent DECIMAL(5,2) DEFAULT 0 CHECK (progress_percent BETWEEN 0 AND 100),
    records_processed BIGINT DEFAULT 0,
    total_records BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,
    processing_rate DECIMAL(10,2), -- records per second
    estimated_completion TIMESTAMP WITH TIME ZONE,
    message TEXT,
    details JSONB DEFAULT '{}',
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key constraint
    CONSTRAINT fk_progress_job_id FOREIGN KEY (job_id) REFERENCES processing_jobs(job_id) ON DELETE CASCADE
);

-- Table for storing file metadata and analysis
CREATE TABLE IF NOT EXISTS file_metadata (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    file_size BIGINT NOT NULL,
    mime_type VARCHAR(100),
    encoding VARCHAR(50),
    delimiter VARCHAR(10), -- for CSV files
    quote_char VARCHAR(1), -- for CSV files
    escape_char VARCHAR(1), -- for CSV files
    header_row BOOLEAN DEFAULT FALSE,
    columns_detected JSONB DEFAULT '[]',
    sample_data JSONB DEFAULT '[]',
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score BETWEEN 0 AND 1),
    schema_validation_errors JSONB DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key constraint
    CONSTRAINT fk_metadata_job_id FOREIGN KEY (job_id) REFERENCES processing_jobs(job_id) ON DELETE CASCADE
);

-- Table for storing data lineage and transformation information
CREATE TABLE IF NOT EXISTS data_lineage (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    source_location VARCHAR(500) NOT NULL,
    target_type VARCHAR(50) NOT NULL,
    target_location VARCHAR(500) NOT NULL,
    transformation_rules JSONB DEFAULT '[]',
    data_mapping JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key constraint
    CONSTRAINT fk_lineage_job_id FOREIGN KEY (job_id) REFERENCES processing_jobs(job_id) ON DELETE CASCADE
);

-- Table for storing system configuration and settings
CREATE TABLE IF NOT EXISTS system_config (
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(100) UNIQUE NOT NULL,
    config_value JSONB NOT NULL,
    config_type VARCHAR(50) NOT NULL DEFAULT 'general',
    description TEXT,
    is_encrypted BOOLEAN DEFAULT FALSE,
    updated_by VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ================================================================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- ================================================================================================

-- Indexes for processing_jobs table
CREATE INDEX IF NOT EXISTS idx_processing_jobs_job_id ON processing_jobs(job_id);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_status ON processing_jobs(status);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_created_at ON processing_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_destination ON processing_jobs(destination);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_file_type ON processing_jobs(file_type);
CREATE INDEX IF NOT EXISTS idx_processing_jobs_priority ON processing_jobs(priority DESC);

-- Indexes for processing_progress table
CREATE INDEX IF NOT EXISTS idx_processing_progress_job_id ON processing_progress(job_id);
CREATE INDEX IF NOT EXISTS idx_processing_progress_timestamp ON processing_progress(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_processing_progress_stage ON processing_progress(stage);

-- Indexes for file_metadata table
CREATE INDEX IF NOT EXISTS idx_file_metadata_job_id ON file_metadata(job_id);
CREATE INDEX IF NOT EXISTS idx_file_metadata_filename ON file_metadata(original_filename);
CREATE INDEX IF NOT EXISTS idx_file_metadata_size ON file_metadata(file_size);

-- Indexes for data_lineage table
CREATE INDEX IF NOT EXISTS idx_data_lineage_job_id ON data_lineage(job_id);
CREATE INDEX IF NOT EXISTS idx_data_lineage_source ON data_lineage(source_type, source_location);
CREATE INDEX IF NOT EXISTS idx_data_lineage_target ON data_lineage(target_type, target_location);

-- Indexes for system_config table
CREATE INDEX IF NOT EXISTS idx_system_config_key ON system_config(config_key);
CREATE INDEX IF NOT EXISTS idx_system_config_type ON system_config(config_type);

-- ================================================================================================
-- FUNCTIONS AND TRIGGERS
-- ================================================================================================

-- Function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Function to calculate processing rate
CREATE OR REPLACE FUNCTION calculate_processing_rate()
RETURNS TRIGGER AS $$
DECLARE
    time_diff INTERVAL;
    rate DECIMAL(10,2);
BEGIN
    IF OLD.records_processed IS NOT NULL AND NEW.records_processed > OLD.records_processed THEN
        time_diff := NEW.timestamp - OLD.timestamp;
        IF EXTRACT(EPOCH FROM time_diff) > 0 THEN
            rate := (NEW.records_processed - OLD.records_processed) / EXTRACT(EPOCH FROM time_diff);
            NEW.processing_rate := rate;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Function to estimate completion time
CREATE OR REPLACE FUNCTION estimate_completion_time()
RETURNS TRIGGER AS $$
DECLARE
    remaining_records BIGINT;
    estimated_seconds DECIMAL;
BEGIN
    IF NEW.processing_rate > 0 AND NEW.total_records > 0 AND NEW.records_processed < NEW.total_records THEN
        remaining_records := NEW.total_records - NEW.records_processed;
        estimated_seconds := remaining_records / NEW.processing_rate;
        NEW.estimated_completion := NEW.timestamp + (estimated_seconds * INTERVAL '1 second');
    END IF;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- ================================================================================================
-- TRIGGERS
-- ================================================================================================

-- Trigger to update updated_at on processing_jobs
CREATE TRIGGER update_processing_jobs_updated_at
    BEFORE UPDATE ON processing_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger to update updated_at on system_config
CREATE TRIGGER update_system_config_updated_at
    BEFORE UPDATE ON system_config
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger to calculate processing rate on progress updates
CREATE TRIGGER calculate_progress_rate
    BEFORE UPDATE ON processing_progress
    FOR EACH ROW
    EXECUTE FUNCTION calculate_processing_rate();

-- Trigger to estimate completion time on progress updates
CREATE TRIGGER estimate_progress_completion
    BEFORE INSERT OR UPDATE ON processing_progress
    FOR EACH ROW
    EXECUTE FUNCTION estimate_completion_time();

-- ================================================================================================
-- VIEWS FOR COMMON QUERIES
-- ================================================================================================

-- View for active jobs with latest progress
CREATE OR REPLACE VIEW active_jobs_with_progress AS
SELECT
    pj.job_id,
    pj.filename,
    pj.file_size,
    pj.file_type,
    pj.destination,
    pj.status,
    pj.priority,
    pj.created_at,
    pj.started_at,
    pp.stage,
    pp.progress_percent,
    pp.records_processed,
    pp.total_records,
    pp.processing_rate,
    pp.estimated_completion,
    pp.message as latest_message
FROM processing_jobs pj
LEFT JOIN LATERAL (
    SELECT DISTINCT ON (job_id) *
    FROM processing_progress
    WHERE job_id = pj.job_id
    ORDER BY job_id, timestamp DESC
) pp ON TRUE
WHERE pj.status IN ('pending', 'processing');

-- View for job statistics
CREATE OR REPLACE VIEW job_statistics AS
SELECT
    destination,
    file_type,
    COUNT(*) as total_jobs,
    COUNT(*) FILTER (WHERE status = 'completed') as completed_jobs,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_jobs,
    COUNT(*) FILTER (WHERE status = 'processing') as processing_jobs,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) FILTER (WHERE status = 'completed') as avg_processing_time_seconds,
    SUM(file_size) FILTER (WHERE status = 'completed') as total_processed_bytes
FROM processing_jobs
GROUP BY destination, file_type;

-- ================================================================================================
-- INITIAL SYSTEM CONFIGURATION
-- ================================================================================================

-- Insert default system configuration
INSERT INTO system_config (config_key, config_value, config_type, description) VALUES
    ('max_file_size_bytes', '10737418240', 'limits', 'Maximum file size allowed for processing (10GB)'),
    ('max_concurrent_jobs', '5', 'performance', 'Maximum number of jobs that can be processed concurrently'),
    ('default_retry_count', '3', 'reliability', 'Default number of retries for failed jobs'),
    ('progress_report_interval_seconds', '10', 'monitoring', 'How often to report progress updates'),
    ('cleanup_completed_jobs_days', '30', 'maintenance', 'Days to keep completed job records'),
    ('supported_file_types', '["csv", "json", "xml", "parquet", "avro"]', 'general', 'List of supported input file types'),
    ('supported_destinations', '["postgres", "clickhouse", "hdfs", "s3"]', 'general', 'List of supported output destinations')
ON CONFLICT (config_key) DO NOTHING;

-- ================================================================================================
-- PERMISSIONS AND SECURITY
-- ================================================================================================

-- Grant permissions to the database user (uses POSTGRES_USER environment variable)
-- Note: The actual username will be determined by the POSTGRES_USER environment variable
-- during container initialization

-- Grant permissions on all existing tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO CURRENT_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO CURRENT_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_warehouse TO CURRENT_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_warehouse TO CURRENT_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO CURRENT_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO CURRENT_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO CURRENT_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA metadata TO CURRENT_USER;

-- Grant permissions on future tables and sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse GRANT ALL ON TABLES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_warehouse GRANT ALL ON SEQUENCES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON SEQUENCES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata GRANT ALL ON TABLES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata GRANT ALL ON SEQUENCES TO CURRENT_USER;

-- ================================================================================================
-- COMPLETION MESSAGE
-- ================================================================================================

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'BigData Processing System database initialization completed successfully';
    RAISE NOTICE 'Database: %', current_database();
    RAISE NOTICE 'User: %', current_user;
    RAISE NOTICE 'Timestamp: %', CURRENT_TIMESTAMP;
END $$;