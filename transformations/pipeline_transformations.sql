
-- ============================================================================
-- SDP PIPELINE: E-Commerce Orders Processing (Continuous + CDC)
-- Bronze → Silver → Gold with Change Data Capture
-- ============================================================================

-- ----------------------------------------------------------------------------
-- BRONZE LAYER: Raw Data Ingestion with Auto Loader + CDC Metadata
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT "Raw e-commerce orders with CDC tracking"
AS SELECT 
  *,
  current_timestamp() as ingestion_time,
  _metadata.file_path as source_file,
  _metadata.file_modification_time as file_modified_time,
  'INSERT' as operation_type
FROM STREAM read_files(
  '/Volumes/main/default/demo_orders',
  format => 'json',
  inferColumnTypes => true
);

-- ----------------------------------------------------------------------------
-- QUARANTINE LAYER: Capture Failed Quality Checks
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE quarantine_orders
COMMENT "Orders that failed quality checks"
AS SELECT 
  order_id,
  user_id,
  product_id,
  amount,
  status,
  order_timestamp,
  ingestion_time,
  source_file,
  CASE 
    WHEN user_id IS NULL THEN 'NULL_USER_ID'
    WHEN amount <= 0 THEN 'INVALID_AMOUNT'
    WHEN order_id IS NULL THEN 'NULL_ORDER_ID'
    ELSE 'UNKNOWN'
  END as quarantine_reason,
  current_timestamp() as quarantine_time
FROM STREAM(bronze_orders)
WHERE user_id IS NULL OR amount <= 0 OR order_id IS NULL;

-- ----------------------------------------------------------------------------
-- SILVER LAYER: Data Quality & Cleansing with CDC Tracking
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_orders(
  CONSTRAINT valid_user_id 
    EXPECT (user_id IS NOT NULL) 
    ON VIOLATION DROP ROW,
  
  CONSTRAINT valid_amount 
    EXPECT (amount > 0) 
    ON VIOLATION DROP ROW,
  
  CONSTRAINT valid_order_id 
    EXPECT (order_id IS NOT NULL) 
    ON VIOLATION DROP ROW
)
COMMENT "Quality-validated orders with CDC metadata"
AS SELECT 
  order_id,
  user_id,
  product_id,
  amount,
  status,
  CAST(order_timestamp AS TIMESTAMP) as order_timestamp,
  created_at,
  ingestion_time,
  source_file,
  file_modified_time,
  operation_type,
  current_timestamp() as processed_time
FROM STREAM(bronze_orders);

-- ----------------------------------------------------------------------------
-- SILVER LAYER: Deduplicated Orders with Late Data Tracking
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_orders_deduped
AS SELECT 
  order_id,
  user_id,
  product_id,
  amount,
  status,
  order_timestamp,
  created_at,
  ingestion_time,
  source_file,
  file_modified_time,
  operation_type,
  processed_time,
  CASE 
    WHEN ingestion_time > file_modified_time + INTERVAL 1 HOUR 
    THEN true 
    ELSE false 
  END as is_late_data
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id 
      ORDER BY processed_time DESC
    ) as rn
  FROM silver_orders
)
WHERE rn = 1;

-- ----------------------------------------------------------------------------
-- CDC AUDIT TABLE: Track All Changes for Observability
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE cdc_audit_log
COMMENT "CDC audit log for observability and compliance"
AS SELECT 
  order_id,
  user_id,
  product_id,
  amount,
  status,
  operation_type,
  ingestion_time,
  processed_time,
  source_file,
  CAST(DATE(processed_time) AS DATE) as processing_date,
  HOUR(processed_time) as processing_hour
FROM STREAM(silver_orders);

-- ----------------------------------------------------------------------------
-- GOLD LAYER: Real-time Daily Sales Aggregation
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_user_sales
COMMENT "Real-time daily sales by user"
AS SELECT 
  DATE(order_timestamp) as order_date,
  user_id,
  COUNT(*) as total_orders,
  SUM(amount) as total_sales,
  AVG(amount) as avg_order_value,
  MIN(amount) as min_order_value,
  MAX(amount) as max_order_value,
  COUNT(DISTINCT product_id) as unique_products,
  COLLECT_LIST(status) as order_statuses,
  MAX(processed_time) as last_updated
FROM silver_orders
GROUP BY ALL;

-- ----------------------------------------------------------------------------
-- GOLD LAYER: Real-time Product Performance
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_product_performance
COMMENT "Real-time product performance metrics"
AS SELECT 
  product_id,
  COUNT(*) as total_orders,
  SUM(amount) as total_revenue,
  AVG(amount) as avg_price,
  COUNT(DISTINCT user_id) as unique_customers,
  MAX(processed_time) as last_updated
FROM silver_orders
GROUP BY product_id;

-- ----------------------------------------------------------------------------
-- GOLD LAYER: Hourly Aggregated Metrics
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_hourly_metrics
COMMENT "Hourly aggregated metrics"
AS SELECT 
  DATE(processed_time) as metric_date,
  HOUR(processed_time) as metric_hour,
  COUNT(*) as orders_count,
  SUM(amount) as total_revenue,
  AVG(amount) as avg_order_value,
  MIN(amount) as min_order_value,
  MAX(amount) as max_order_value,
  COUNT(DISTINCT user_id) as active_users,
  COUNT(DISTINCT product_id) as products_sold,
  SUM(CAST(is_late_data AS INT)) as late_data_count,
  ROUND(SUM(CAST(is_late_data AS INT)) * 100.0 / COUNT(*), 2) as late_data_pct
FROM silver_orders_deduped
GROUP BY ALL;

-- ----------------------------------------------------------------------------
-- GOLD LAYER: User Dimension with Segmentation
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_user_dimension
COMMENT "User dimension with lifetime metrics and segmentation"
AS SELECT 
  user_id,
  MIN(order_timestamp) as first_order_date,
  MAX(order_timestamp) as last_order_date,
  COUNT(*) as lifetime_orders,
  SUM(amount) as lifetime_value,
  AVG(amount) as avg_order_value,
  CASE 
    WHEN SUM(amount) >= 1000 THEN 'PLATINUM'
    WHEN SUM(amount) >= 500 THEN 'GOLD'
    WHEN SUM(amount) >= 250 THEN 'SILVER'
    ELSE 'BRONZE'
  END as customer_tier,
  CASE 
    WHEN MAX(order_timestamp) >= current_timestamp() - INTERVAL 30 DAYS THEN 'Active'
    WHEN MAX(order_timestamp) >= current_timestamp() - INTERVAL 90 DAYS THEN 'At Risk'
    ELSE 'Inactive'
  END as customer_status,
  MAX(processed_time) as last_updated
FROM silver_orders
GROUP BY user_id;

-- ----------------------------------------------------------------------------
-- GOLD LAYER: Real-time 5-Minute Windowed Metrics (10-minute late data tolerance)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE gold_realtime_metrics
COMMENT "Real-time 5-minute aggregated metrics with 10-minute late data tolerance"
AS SELECT 
  window.start as window_start,
  window.end as window_end,
  COUNT(*) as orders_in_window,
  SUM(amount) as revenue_in_window,
  AVG(amount) as avg_order_value,
  approx_count_distinct(user_id) as active_users,
  approx_count_distinct(product_id) as products_sold,
  SUM(CAST(
    CASE 
      WHEN ingestion_time > file_modified_time + INTERVAL 10 MINUTES 
      THEN true 
      ELSE false 
    END AS INT)) as late_data_count
FROM STREAM(silver_orders)
WATERMARK order_timestamp DELAY OF INTERVAL 10 MINUTES
GROUP BY window(order_timestamp, '5 minutes');

-- ----------------------------------------------------------------------------
-- OBSERVABILITY: Quarantine Summary
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW quarantine_summary
COMMENT "Quarantined data summary"
AS SELECT 
  DATE(quarantine_time) as quarantine_date,
  quarantine_reason,
  COUNT(*) as quarantine_count,
  COUNT(DISTINCT source_file) as affected_files,
  MAX(quarantine_time) as last_quarantine_time
FROM quarantine_orders
GROUP BY ALL;

-- ----------------------------------------------------------------------------
-- OBSERVABILITY: Data Quality Metrics
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW data_quality_metrics
COMMENT "Data quality metrics for monitoring"
AS SELECT 
  DATE(ingestion_time) as metric_date,
  HOUR(ingestion_time) as metric_hour,
  COUNT(*) as total_records_ingested,
  COUNT(DISTINCT source_file) as files_processed,
  MIN(ingestion_time) as first_record_time,
  MAX(ingestion_time) as last_record_time
FROM bronze_orders
GROUP BY ALL;

-- ----------------------------------------------------------------------------
-- OBSERVABILITY: CDC Metrics Summary
-- ----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW cdc_metrics_summary
COMMENT "Aggregated CDC metrics for dashboard"
AS SELECT 
  processing_date,
  operation_type,
  COUNT(*) as operation_count,
  COUNT(DISTINCT user_id) as affected_users,
  SUM(amount) as total_amount
FROM cdc_audit_log
GROUP BY ALL;
