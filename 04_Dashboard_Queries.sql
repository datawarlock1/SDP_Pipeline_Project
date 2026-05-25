-- ============================================================================
-- SDP Pipeline Observability Dashboard - All Queries
-- Dashboard ID: 01f1497789da16dcb1f180988df882b
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Throughput Counter (Last 5 Minutes)
-- ----------------------------------------------------------------------------
SELECT 
  'Bronze' as layer, 
  COUNT(*) as rows_processed 
FROM main.demo_sdp.bronze_orders 
WHERE ingestion_time >= current_timestamp() - INTERVAL 5 MINUTES
UNION ALL 
SELECT 
  'Silver' as layer, 
  COUNT(*) 
FROM main.demo_sdp.silver_orders 
WHERE processed_time >= current_timestamp() - INTERVAL 5 MINUTES;

-- ----------------------------------------------------------------------------
-- 2. Data Flow by Layer
-- ----------------------------------------------------------------------------
SELECT 'Bronze' as layer, COUNT(*) as record_count 
FROM main.demo_sdp.bronze_orders
UNION ALL 
SELECT 'Silver', COUNT(*) 
FROM main.demo_sdp.silver_orders
UNION ALL 
SELECT 'Gold Users', COUNT(*) 
FROM main.demo_sdp.gold_daily_user_sales
UNION ALL 
SELECT 'Gold Products', COUNT(*) 
FROM main.demo_sdp.gold_product_performance
ORDER BY record_count DESC;

-- ----------------------------------------------------------------------------
-- 3. Processing Latency (Average Seconds)
-- ----------------------------------------------------------------------------
SELECT 
  AVG(TIMESTAMPDIFF(SECOND, b.ingestion_time, s.processed_time)) as avg_latency_seconds
FROM main.demo_sdp.silver_orders s
JOIN main.demo_sdp.bronze_orders b ON s.order_id = b.order_id
WHERE s.processed_time >= current_timestamp() - INTERVAL 10 MINUTES;

-- ----------------------------------------------------------------------------
-- 4. Quality Rejection Rate (Percentage)
-- ----------------------------------------------------------------------------
SELECT 
  ROUND((bronze_cnt - silver_cnt) * 100.0 / NULLIF(bronze_cnt, 0), 2) as rejection_rate_pct
FROM (
  SELECT 
    (SELECT COUNT(*) FROM main.demo_sdp.bronze_orders) as bronze_cnt,
    (SELECT COUNT(*) FROM main.demo_sdp.silver_orders) as silver_cnt
);

-- ----------------------------------------------------------------------------
-- 5. Ingestion Rate Over Time (Last Hour)
-- ----------------------------------------------------------------------------
SELECT 
  DATE_TRUNC('minute', ingestion_time) as time_bucket,
  COUNT(*) as records_per_minute,
  COUNT(DISTINCT source_file) as files_processed
FROM main.demo_sdp.bronze_orders
WHERE ingestion_time >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY DATE_TRUNC('minute', ingestion_time)
ORDER BY time_bucket DESC;

-- ----------------------------------------------------------------------------
-- 6. Pipeline State & Freshness
-- ----------------------------------------------------------------------------
SELECT 
  'Bronze Orders' as dataset,
  COUNT(*) as total_rows,
  MAX(ingestion_time) as last_updated,
  TIMESTAMPDIFF(MINUTE, MAX(ingestion_time), current_timestamp()) as minutes_ago
FROM main.demo_sdp.bronze_orders
UNION ALL
SELECT 
  'Silver Orders',
  COUNT(*),
  MAX(processed_time),
  TIMESTAMPDIFF(MINUTE, MAX(processed_time), current_timestamp())
FROM main.demo_sdp.silver_orders
UNION ALL
SELECT 
  'Gold User Sales',
  COUNT(*),
  MAX(last_updated),
  TIMESTAMPDIFF(MINUTE, MAX(last_updated), current_timestamp())
FROM main.demo_sdp.gold_daily_user_sales;
